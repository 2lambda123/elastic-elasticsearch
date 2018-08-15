/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.persistence;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.elasticsearch.xpack.ml.job.persistence.ExpandedIdsMatcher;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class DatafeedConfigProvider extends AbstractComponent {

    private final Client client;
    private final NamedXContentRegistry xContentRegistry;

    private static final Map<String, String> TO_XCONTENT_PARAMS = new HashMap<>();
    static {
        TO_XCONTENT_PARAMS.put(ToXContentParams.FOR_INTERNAL_STORAGE, "true");
        TO_XCONTENT_PARAMS.put(ToXContentParams.INCLUDE_TYPE, "true");
    }

    public DatafeedConfigProvider(Client client, Settings settings, NamedXContentRegistry xContentRegistry) {
        super(settings);
        this.client = client;
        this.xContentRegistry = xContentRegistry;
    }

    /**
     * Persist the datafeed configuration to the config index.
     * It is an error if a datafeed with the same Id already exists -
     * the config will not be overwritten.
     *
     * @param config The datafeed configuration
     * @param listener Index response listener
     */
    public void putDatafeedConfig(DatafeedConfig config, ActionListener<IndexResponse> listener) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = config.toXContent(builder, new ToXContent.MapParams(TO_XCONTENT_PARAMS));

            IndexRequest indexRequest =  client.prepareIndex(AnomalyDetectorsIndex.configIndexName(),
                    ElasticsearchMappings.DOC_TYPE, DatafeedConfig.documentId(config.getId()))
                    .setSource(source)
                    .setOpType(DocWriteRequest.OpType.CREATE)
                    .request();

            executeAsyncWithOrigin(client, ML_ORIGIN, IndexAction.INSTANCE, indexRequest, listener);

        } catch (IOException e) {
            listener.onFailure(new ElasticsearchParseException("Failed to serialise datafeed config with id [" + config.getId() + "]", e));
        }
    }

    /**
     * Get the datafeed config specified by {@code datafeedId}.
     * If the datafeed document is missing a {@code ResourceNotFoundException}
     * is returned via the listener.
     *
     * @param datafeedId The datafeed ID
     * @param datafeedConfigListener The config listener
     */
    public void getDatafeedConfig(String datafeedId, ActionListener<DatafeedConfig.Builder> datafeedConfigListener) {
        GetRequest getRequest = new GetRequest(AnomalyDetectorsIndex.configIndexName(),
                ElasticsearchMappings.DOC_TYPE, DatafeedConfig.documentId(datafeedId));
        executeAsyncWithOrigin(client, ML_ORIGIN, GetAction.INSTANCE, getRequest, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists() == false) {
                    datafeedConfigListener.onFailure(ExceptionsHelper.missingDatafeedException(datafeedId));
                    return;
                }
                BytesReference source = getResponse.getSourceAsBytesRef();
                parseLenientlyFromSource(source, datafeedConfigListener);
            }
            @Override
            public void onFailure(Exception e) {
                datafeedConfigListener.onFailure(e);
            }
        });
    }

    /**
     * Delete the datafeed config document
     *
     * @param datafeedId The datafeed id
     * @param actionListener Deleted datafeed listener
     */
    public void deleteDatafeedConfig(String datafeedId,  ActionListener<DeleteResponse> actionListener) {
        DeleteRequest request = new DeleteRequest(AnomalyDetectorsIndex.configIndexName(),
                ElasticsearchMappings.DOC_TYPE, DatafeedConfig.documentId(datafeedId));
        executeAsyncWithOrigin(client, ML_ORIGIN, DeleteAction.INSTANCE, request, new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                    actionListener.onFailure(ExceptionsHelper.missingDatafeedException(datafeedId));
                    return;
                }
                assert deleteResponse.getResult() == DocWriteResponse.Result.DELETED;
                actionListener.onResponse(deleteResponse);
            }
            @Override
            public void onFailure(Exception e) {
                actionListener.onFailure(e);
            }
        });
    }

    /**
     * Get the datafeed config and apply the {@code update}
     * then index the modified config setting the version in the request.
     *
     * @param datafeedId The Id of the datafeed to update
     * @param update The update
     * @param headers Datafeed headers applied with the update
     * @param updatedConfigListener Updated datafeed config listener
     */
    public void updateDatefeedConfig(String datafeedId, DatafeedUpdate update, Map<String, String> headers,
                          ActionListener<DatafeedConfig> updatedConfigListener) {
        GetRequest getRequest = new GetRequest(AnomalyDetectorsIndex.configIndexName(),
                ElasticsearchMappings.DOC_TYPE, DatafeedConfig.documentId(datafeedId));

        executeAsyncWithOrigin(client, ML_ORIGIN, GetAction.INSTANCE, getRequest, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists() == false) {
                    updatedConfigListener.onFailure(ExceptionsHelper.missingDatafeedException(datafeedId));
                    return;
                }
                long version = getResponse.getVersion();
                BytesReference source = getResponse.getSourceAsBytesRef();
                DatafeedConfig.Builder configBuilder;
                try {
                    configBuilder = parseLenientlyFromSource(source);
                } catch (IOException e) {
                    updatedConfigListener.onFailure(
                            new ElasticsearchParseException("Failed to parse datafeed config [" + datafeedId + "]", e));
                    return;
                }

                DatafeedConfig updatedConfig;
                try {
                    updatedConfig = update.apply(configBuilder.build(), headers);
                } catch (Exception e) {
                    updatedConfigListener.onFailure(e);
                    return;
                }

                try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                    XContentBuilder updatedSource = updatedConfig.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    IndexRequest indexRequest = client.prepareIndex(AnomalyDetectorsIndex.configIndexName(),
                            ElasticsearchMappings.DOC_TYPE, DatafeedConfig.documentId(updatedConfig.getId()))
                            .setSource(updatedSource)
                            .setVersion(version)
                            .request();

                    executeAsyncWithOrigin(client, ML_ORIGIN, IndexAction.INSTANCE, indexRequest, ActionListener.wrap(
                            indexResponse -> {
                                assert indexResponse.getResult() == DocWriteResponse.Result.UPDATED;
                                updatedConfigListener.onResponse(updatedConfig);
                            },
                            updatedConfigListener::onFailure
                    ));

                } catch (IOException e) {
                    updatedConfigListener.onFailure(
                            new ElasticsearchParseException("Failed to serialise datafeed config with id [" + datafeedId + "]", e));
                }
            }

            @Override
            public void onFailure(Exception e) {
                updatedConfigListener.onFailure(e);
            }
        });
    }

    /**
     * Expands an expression into the set of matching names. {@code expresssion}
     * may be a wildcard, a datafeed ID or a list of those.
     * If {@code expression} == 'ALL', '*' or the empty string then all
     * datafeed IDs are returned.
     *
     * For example, given a set of names ["foo-1", "foo-2", "bar-1", bar-2"],
     * expressions resolve follows:
     * <ul>
     *     <li>"foo-1" : ["foo-1"]</li>
     *     <li>"bar-1" : ["bar-1"]</li>
     *     <li>"foo-1,foo-2" : ["foo-1", "foo-2"]</li>
     *     <li>"foo-*" : ["foo-1", "foo-2"]</li>
     *     <li>"*-1" : ["bar-1", "foo-1"]</li>
     *     <li>"*" : ["bar-1", "bar-2", "foo-1", "foo-2"]</li>
     *     <li>"_all" : ["bar-1", "bar-2", "foo-1", "foo-2"]</li>
     * </ul>
     *
     * @param expression the expression to resolve
     * @param allowNoDatafeeds if {@code false}, an error is thrown when no name matches the {@code expression}.
     *                     This only applies to wild card expressions, if {@code expression} is not a
     *                     wildcard then setting this true will not suppress the exception
     * @param listener The expanded datafeed IDs listener
     */
    public void expandDatafeedIds(String expression, boolean allowNoDatafeeds, ActionListener<Set<String>> listener) {
        String [] tokens = ExpandedIdsMatcher.tokenizeExpression(expression);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(buildQuery(tokens));
        sourceBuilder.sort(DatafeedConfig.ID.getPreferredName());
        String [] includes = new String[] {DatafeedConfig.ID.getPreferredName()};
        sourceBuilder.fetchSource(includes, null);

        SearchRequest searchRequest = client.prepareSearch(AnomalyDetectorsIndex.configIndexName())
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setSource(sourceBuilder).request();

        ExpandedIdsMatcher requiredMatches = new ExpandedIdsMatcher(tokens, allowNoDatafeeds);

        executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, searchRequest,
                ActionListener.<SearchResponse>wrap(
                        response -> {
                            Set<String> datafeedIds = new HashSet<>();
                            SearchHit[] hits = response.getHits().getHits();
                            for (SearchHit hit : hits) {
                                datafeedIds.add((String)hit.getSourceAsMap().get(DatafeedConfig.ID.getPreferredName()));
                            }

                            requiredMatches.filterMatchedIds(datafeedIds);
                            if (requiredMatches.hasUnmatchedIds()) {
                                // some required datafeeds were not found
                                listener.onFailure(ExceptionsHelper.missingDatafeedException(requiredMatches.unmatchedIdsString()));
                                return;
                            }

                            listener.onResponse(datafeedIds);
                        },
                        listener::onFailure)
                , client::search);

    }

    /**
     * The same logic as {@link #expandDatafeedIds(String, boolean, ActionListener)} but
     * the full datafeed configuration is returned.
     *
     * See {@link #expandDatafeedIds(String, boolean, ActionListener)}
     *
     * @param expression the expression to resolve
     * @param allowNoDatafeeds if {@code false}, an error is thrown when no name matches the {@code expression}.
     *                     This only applies to wild card expressions, if {@code expression} is not a
     *                     wildcard then setting this true will not suppress the exception
     * @param listener The expanded datafeed config listener
     */
    // NORELEASE datafeed configs should be paged or have a mechanism to return all jobs if there are many of them
    public void expandDatafeedConfigs(String expression, boolean allowNoDatafeeds, ActionListener<List<DatafeedConfig.Builder>> listener) {
        String [] tokens = ExpandedIdsMatcher.tokenizeExpression(expression);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(buildQuery(tokens));
        sourceBuilder.sort(DatafeedConfig.ID.getPreferredName());

        SearchRequest searchRequest = client.prepareSearch(AnomalyDetectorsIndex.configIndexName())
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setSource(sourceBuilder).request();

        ExpandedIdsMatcher requiredMatches = new ExpandedIdsMatcher(tokens, allowNoDatafeeds);

        executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, searchRequest,
                ActionListener.<SearchResponse>wrap(
                        response -> {
                            List<DatafeedConfig.Builder> datafeeds = new ArrayList<>();
                            Set<String> datafeedIds = new HashSet<>();
                            SearchHit[] hits = response.getHits().getHits();
                            for (SearchHit hit : hits) {
                                try {
                                    BytesReference source = hit.getSourceRef();
                                    DatafeedConfig.Builder datafeed = parseLenientlyFromSource(source);
                                    datafeeds.add(datafeed);
                                    datafeedIds.add(datafeed.getId());
                                } catch (IOException e) {
                                    // TODO A better way to handle this rather than just ignoring the error?
                                    logger.error("Error parsing datafeed configuration [" + hit.getId() + "]", e);
                                }
                            }

                            requiredMatches.filterMatchedIds(datafeedIds);
                            if (requiredMatches.hasUnmatchedIds()) {
                                // some required datafeeds were not found
                                listener.onFailure(ExceptionsHelper.missingDatafeedException(requiredMatches.unmatchedIdsString()));
                                return;
                            }

                            listener.onResponse(datafeeds);
                        },
                        listener::onFailure)
                , client::search);

    }

    private QueryBuilder buildQuery(String [] tokens) {
        QueryBuilder jobQuery = new TermQueryBuilder(DatafeedConfig.CONFIG_TYPE.getPreferredName(), DatafeedConfig.TYPE);
        if (Strings.isAllOrWildcard(tokens)) {
            // match all
            return jobQuery;
        }

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(jobQuery);
        BoolQueryBuilder shouldQueries = new BoolQueryBuilder();

        List<String> terms = new ArrayList<>();
        for (String token : tokens) {
            if (Regex.isSimpleMatchPattern(token)) {
                shouldQueries.should(new WildcardQueryBuilder(DatafeedConfig.ID.getPreferredName(), token));
            } else {
                terms.add(token);
            }
        }

        if (terms.isEmpty() == false) {
            shouldQueries.should(new TermsQueryBuilder(DatafeedConfig.ID.getPreferredName(), terms));
        }

        if (shouldQueries.should().isEmpty() == false) {
            boolQueryBuilder.filter(shouldQueries);
        }

        return boolQueryBuilder;
    }

    private void parseLenientlyFromSource(BytesReference source, ActionListener<DatafeedConfig.Builder> datafeedConfigListener)  {
        try (InputStream stream = source.streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                     .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
            datafeedConfigListener.onResponse(DatafeedConfig.LENIENT_PARSER.apply(parser, null));
        } catch (Exception e) {
            datafeedConfigListener.onFailure(e);
        }
    }

    private DatafeedConfig.Builder parseLenientlyFromSource(BytesReference source) throws IOException {
        try (InputStream stream = source.streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                     .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
            return DatafeedConfig.LENIENT_PARSER.apply(parser, null);
        }
    }
}
