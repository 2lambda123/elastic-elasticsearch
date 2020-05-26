/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.results.Forecast;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.core.ml.utils.time.TimeUtils;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Removes up to {@link #MAX_FORECASTS} forecasts (stats + forecasts docs) that have expired.
 * A forecast is deleted if its expiration timestamp is earlier
 * than the start of the current day (local time-zone).
 *
 * This is expected to be used by actions requiring admin rights. Thus,
 * it is also expected that the provided client will be a client with the
 * ML origin so that permissions to manage ML indices are met.
 */
public class ExpiredForecastsRemover implements MlDataRemover {

    private static final Logger LOGGER = LogManager.getLogger(ExpiredForecastsRemover.class);
    private static final int MAX_FORECASTS = 10000;
    private static final String RESULTS_INDEX_PATTERN =  AnomalyDetectorsIndex.jobResultsIndexPrefix() + "*";

    private final Client client;
    private final ThreadPool threadPool;
    private final long cutoffEpochMs;

    public ExpiredForecastsRemover(Client client, ThreadPool threadPool) {
        this.client = Objects.requireNonNull(client);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.cutoffEpochMs = DateTime.now(ISOChronology.getInstance()).getMillis();
    }

    @Override
    public void remove(ActionListener<Boolean> listener, Supplier<Boolean> isTimedOutSupplier) {
        LOGGER.debug("Removing forecasts that expire before [{}]", cutoffEpochMs);
        ActionListener<SearchResponse> forecastStatsHandler = ActionListener.wrap(
                searchResponse -> deleteForecasts(searchResponse, listener, isTimedOutSupplier),
                e -> listener.onFailure(new ElasticsearchException("An error occurred while searching forecasts to delete", e)));

        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(Result.RESULT_TYPE.getPreferredName(), ForecastRequestStats.RESULT_TYPE_VALUE))
                .filter(QueryBuilders.existsQuery(ForecastRequestStats.EXPIRY_TIME.getPreferredName())));
        source.size(MAX_FORECASTS);
        source.fetchSource(false);
        source.docValueField(Job.ID.getPreferredName(), null);
        source.docValueField(ForecastRequestStats.FORECAST_ID.getPreferredName(), null);
        source.docValueField(ForecastRequestStats.EXPIRY_TIME.getPreferredName(), "epoch_millis");

        // _doc is the most efficient sort order and will also disable scoring
        source.sort(ElasticsearchMappings.ES_DOC);

        SearchRequest searchRequest = new SearchRequest(RESULTS_INDEX_PATTERN);
        searchRequest.source(source);
        client.execute(SearchAction.INSTANCE, searchRequest, new ThreadedActionListener<>(LOGGER, threadPool,
                MachineLearning.UTILITY_THREAD_POOL_NAME, forecastStatsHandler, false));
    }

    private void deleteForecasts(SearchResponse searchResponse, ActionListener<Boolean> listener, Supplier<Boolean> isTimedOutSupplier) {
        List<JobForecastId> forecastsToDelete = findForecastsToDelete(searchResponse);
        if (forecastsToDelete.isEmpty()) {
            listener.onResponse(true);
            return;
        }

        if (isTimedOutSupplier.get()) {
            listener.onResponse(false);
            return;
        }

        DeleteByQueryRequest request = buildDeleteByQuery(forecastsToDelete);
        client.execute(DeleteByQueryAction.INSTANCE, request, new ActionListener<BulkByScrollResponse>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                try {
                    if (bulkByScrollResponse.getDeleted() > 0) {
                        LOGGER.info("Deleted [{}] documents corresponding to [{}] expired forecasts",
                                bulkByScrollResponse.getDeleted(), forecastsToDelete.size());
                    }
                    listener.onResponse(true);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(new ElasticsearchException("Failed to remove expired forecasts", e));
            }
        });
    }

    private List<JobForecastId> findForecastsToDelete(SearchResponse searchResponse) {
        List<JobForecastId> forecastsToDelete = new ArrayList<>();

        SearchHits hits = searchResponse.getHits();
        if (hits.getTotalHits() > MAX_FORECASTS) {
            LOGGER.info("More than [{}] forecasts were found. This run will only delete [{}] of them", MAX_FORECASTS, MAX_FORECASTS);
        }

        for (SearchHit hit : hits.getHits()) {
            String expiryTime = stringFieldValueOrNull(hit, ForecastRequestStats.EXPIRY_TIME.getPreferredName());
            if (expiryTime == null) {
                LOGGER.warn("Forecast request stats document [{}] has a null [{}] field", hit.getId(),
                    ForecastRequestStats.EXPIRY_TIME.getPreferredName());
                continue;
            }
            long expiryMs = TimeUtils.parseToEpochMs(expiryTime);
            if (expiryMs < cutoffEpochMs) {
                JobForecastId idPair = new JobForecastId(
                    stringFieldValueOrNull(hit, Job.ID.getPreferredName()),
                    stringFieldValueOrNull(hit, Forecast.FORECAST_ID.getPreferredName()));

                if (idPair.hasNullValue() == false) {
                    forecastsToDelete.add(idPair);
                }
            }
        }
        return forecastsToDelete;
    }

    private DeleteByQueryRequest buildDeleteByQuery(List<JobForecastId> ids) {
        DeleteByQueryRequest request = new DeleteByQueryRequest();
        request.setSlices(5);

        request.indices(RESULTS_INDEX_PATTERN);
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
        boolQuery.must(QueryBuilders.termsQuery(Result.RESULT_TYPE.getPreferredName(),
                ForecastRequestStats.RESULT_TYPE_VALUE, Forecast.RESULT_TYPE_VALUE));
        for (JobForecastId jobForecastId : ids) {
            if (jobForecastId.hasNullValue() == false) {
                boolQuery.should(QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery(Job.ID.getPreferredName(), jobForecastId.jobId))
                    .must(QueryBuilders.termQuery(Forecast.FORECAST_ID.getPreferredName(), jobForecastId.forecastId)));
            }
        }
        QueryBuilder query = QueryBuilders.boolQuery().filter(boolQuery);
        request.setQuery(query);

        // _doc is the most efficient sort order and will also disable scoring
        request.getSearchRequest().source().sort(ElasticsearchMappings.ES_DOC);

        return request;
    }

    private static class JobForecastId {
        private final String jobId;
        private final String forecastId;

        private JobForecastId(String jobId, String forecastId) {
            this.jobId = jobId;
            this.forecastId = forecastId;
        }

        boolean hasNullValue() {
            return jobId == null || forecastId == null;
        }
    }
}
