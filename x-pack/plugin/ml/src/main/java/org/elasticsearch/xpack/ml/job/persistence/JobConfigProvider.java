/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

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
import org.elasticsearch.common.unit.ByteSizeValue;
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
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * This class implements CRUD operation for the
 * anomaly detector job configuration document
 */
public class JobConfigProvider extends AbstractComponent {

    private final Client client;

    public JobConfigProvider(Client client, Settings settings) {
        super(settings);
        this.client = client;
    }

    /**
     * Persist the anomaly detector job configuration to the configuration index.
     * It is an error if an job with the same Id already exists - the config will
     * not be overwritten.
     *
     * @param job The anomaly detector job configuration
     * @param listener Index response listener
     */
    public void putJob(Job job, ActionListener<IndexResponse> listener) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = job.toXContent(builder, ToXContent.EMPTY_PARAMS);
            IndexRequest indexRequest =  client.prepareIndex(AnomalyDetectorsIndex.configIndexName(),
                    ElasticsearchMappings.DOC_TYPE, Job.documentId(job.getId()))
                    .setSource(source)
                    .setOpType(DocWriteRequest.OpType.CREATE)
                    .request();

            executeAsyncWithOrigin(client, ML_ORIGIN, IndexAction.INSTANCE, indexRequest, listener);

        } catch (IOException e) {
            listener.onFailure(new ElasticsearchParseException("Failed to serialise job with id [" + job.getId() + "]", e));
        }
    }

    /**
     * Get the anomaly detector job specified by {@code jobId}.
     * If the job is missing a {@code ResourceNotFoundException} is returned
     * via the listener.
     *
     * @param jobId The job ID
     * @param jobListener Job listener
     */
    public void getJob(String jobId, ActionListener<Job.Builder> jobListener) {
        GetRequest getRequest = new GetRequest(AnomalyDetectorsIndex.configIndexName(),
                ElasticsearchMappings.DOC_TYPE, Job.documentId(jobId));

        executeAsyncWithOrigin(client, ML_ORIGIN, GetAction.INSTANCE, getRequest, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists() == false) {
                    jobListener.onFailure(ExceptionsHelper.missingJobException(jobId));
                    return;
                }

                BytesReference source = getResponse.getSourceAsBytesRef();
                parseJobLenientlyFromSource(source, jobListener);
            }

            @Override
            public void onFailure(Exception e) {
                jobListener.onFailure(e);
            }
        });
    }

    /**
     * Delete the anomaly detector job config document
     *
     * @param jobId The job id
     * @param actionListener Deleted job listener
     */
    public void deleteJob(String jobId,  ActionListener<DeleteResponse> actionListener) {
        DeleteRequest request = new DeleteRequest(AnomalyDetectorsIndex.configIndexName(),
                ElasticsearchMappings.DOC_TYPE, Job.documentId(jobId));

        executeAsyncWithOrigin(client, ML_ORIGIN, DeleteAction.INSTANCE, request, new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                    actionListener.onFailure(ExceptionsHelper.missingJobException(jobId));
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
     * Get the job and update it by applying {@code jobUpdater} then index the changed job
     * setting the version in the request. Applying the update may cause a validation error
     * which is returned via {@code updatedJobListener}
     *
     * @param jobId The Id of the job to update
     * @param update The job update
     * @param maxModelMemoryLimit The maximum model memory allowed
     * @param updatedJobListener Updated job listener
     */
    public void updateJob(String jobId, JobUpdate update, ByteSizeValue maxModelMemoryLimit, ActionListener<Job> updatedJobListener) {
        GetRequest getRequest = new GetRequest(AnomalyDetectorsIndex.configIndexName(),
                ElasticsearchMappings.DOC_TYPE, Job.documentId(jobId));

        executeAsyncWithOrigin(client, ML_ORIGIN, GetAction.INSTANCE, getRequest, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists() == false) {
                    updatedJobListener.onFailure(ExceptionsHelper.missingJobException(jobId));
                    return;
                }

                long version = getResponse.getVersion();
                BytesReference source = getResponse.getSourceAsBytesRef();
                Job.Builder jobBuilder;
                try {
                     jobBuilder = parseJobLenientlyFromSource(source);
                } catch (IOException e) {
                    updatedJobListener.onFailure(
                            new ElasticsearchParseException("Failed to parse job configuration [" + jobId + "]", e));
                    return;
                }

                Job updatedJob;
                try {
                    // Applying the update may result in a validation error
                    updatedJob = update.mergeWithJob(jobBuilder.build(), maxModelMemoryLimit);
                } catch (Exception e) {
                    updatedJobListener.onFailure(e);
                    return;
                }

                try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                    XContentBuilder updatedSource = updatedJob.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    IndexRequest indexRequest = client.prepareIndex(AnomalyDetectorsIndex.configIndexName(),
                            ElasticsearchMappings.DOC_TYPE, Job.documentId(updatedJob.getId()))
                            .setSource(updatedSource)
                            .setVersion(version)
                            .request();

                    executeAsyncWithOrigin(client, ML_ORIGIN, IndexAction.INSTANCE, indexRequest, ActionListener.wrap(
                            indexResponse -> {
                                assert indexResponse.getResult() == DocWriteResponse.Result.UPDATED;
                                updatedJobListener.onResponse(updatedJob);
                            },
                            updatedJobListener::onFailure
                    ));

                } catch (IOException e) {
                    updatedJobListener.onFailure(
                            new ElasticsearchParseException("Failed to serialise job with id [" + jobId + "]", e));
                }
            }

            @Override
            public void onFailure(Exception e) {
                updatedJobListener.onFailure(e);
            }
        });
    }

    /**
     * Expands an expression into the set of matching names. {@code expresssion}
     * may be a wildcard, a job group, a job ID or a list of those.
     * If {@code expression} == 'ALL', '*' or the empty string then all
     * job IDs are returned.
     * Job groups are expanded to all the jobs IDs in that group.
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
     * @param allowNoJobs if {@code false}, an error is thrown when no name matches the {@code expression}.
     *                     This only applies to wild card expressions, if {@code expression} is not a
     *                     wildcard then setting this true will not suppress the exception
     * @param listener The expanded job IDs listener
     */
    public void expandJobsIds(String expression, boolean allowNoJobs, ActionListener<Set<String>> listener) {
        String [] tokens = ExpandedIdsMatcher.tokenizeExpression(expression);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(buildQuery(tokens));
        sourceBuilder.sort(Job.ID.getPreferredName());
        String [] includes = new String[] {Job.ID.getPreferredName(), Job.GROUPS.getPreferredName()};
        sourceBuilder.fetchSource(includes, null);

        SearchRequest searchRequest = client.prepareSearch(AnomalyDetectorsIndex.configIndexName())
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setSource(sourceBuilder).request();

        ExpandedIdsMatcher requiredMatches = new ExpandedIdsMatcher(tokens, allowNoJobs);

        executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, searchRequest,
                ActionListener.<SearchResponse>wrap(
                        response -> {
                            Set<String> jobIds = new HashSet<>();
                            Set<String> groupsIds = new HashSet<>();
                            SearchHit[] hits = response.getHits().getHits();
                            for (SearchHit hit : hits) {
                                jobIds.add((String)hit.getSourceAsMap().get(Job.ID.getPreferredName()));
                                List<String> groups = (List<String>)hit.getSourceAsMap().get(Job.GROUPS.getPreferredName());
                                if (groups != null) {
                                    groupsIds.addAll(groups);
                                }
                            }

                            groupsIds.addAll(jobIds);
                            requiredMatches.filterMatchedIds(groupsIds);
                            if (requiredMatches.hasUnmatchedIds()) {
                                // some required jobs were not found
                                listener.onFailure(ExceptionsHelper.missingJobException(requiredMatches.unmatchedIdsString()));
                                return;
                            }

                            listener.onResponse(jobIds);
                        },
                        listener::onFailure)
                , client::search);

    }

    /**
     * The same logic as {@link #expandJobsIds(String, boolean, ActionListener)} but
     * the full anomaly detector job configuration is returned.
     *
     * See {@link #expandJobsIds(String, boolean, ActionListener)}
     *
     * @param expression the expression to resolve
     * @param allowNoJobs if {@code false}, an error is thrown when no name matches the {@code expression}.
     *                     This only applies to wild card expressions, if {@code expression} is not a
     *                     wildcard then setting this true will not suppress the exception
     * @param listener The expanded jobs listener
     */
    // NORELEASE jobs should be paged or have a mechanism to return all jobs if there are many of them
    public void expandJobs(String expression, boolean allowNoJobs, ActionListener<List<Job.Builder>> listener) {
        String [] tokens = ExpandedIdsMatcher.tokenizeExpression(expression);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(buildQuery(tokens));
        sourceBuilder.sort(Job.ID.getPreferredName());

        SearchRequest searchRequest = client.prepareSearch(AnomalyDetectorsIndex.configIndexName())
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setSource(sourceBuilder).request();

        ExpandedIdsMatcher requiredMatches = new ExpandedIdsMatcher(tokens, allowNoJobs);

        executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, searchRequest,
                ActionListener.<SearchResponse>wrap(
                        response -> {
                            List<Job.Builder> jobs = new ArrayList<>();
                            Set<String> jobAndGroupIds = new HashSet<>();

                            SearchHit[] hits = response.getHits().getHits();
                            for (SearchHit hit : hits) {
                                try {
                                    BytesReference source = hit.getSourceRef();
                                    Job.Builder job = parseJobLenientlyFromSource(source);
                                    jobs.add(job);
                                    jobAndGroupIds.add(job.getId());
                                    jobAndGroupIds.addAll(job.getGroups());
                                } catch (IOException e) {
                                    // TODO A better way to handle this rather than just ignoring the error?
                                    logger.error("Error parsing anomaly detector job configuration [" + hit.getId() + "]", e);
                                }
                            }

                            requiredMatches.filterMatchedIds(jobAndGroupIds);
                            if (requiredMatches.hasUnmatchedIds()) {
                                // some required jobs were not found
                                listener.onFailure(ExceptionsHelper.missingJobException(requiredMatches.unmatchedIdsString()));
                                return;
                            }

                            listener.onResponse(jobs);
                        },
                        listener::onFailure)
                , client::search);

    }

    private void parseJobLenientlyFromSource(BytesReference source, ActionListener<Job.Builder> jobListener)  {
        try (InputStream stream = source.streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                     .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
            jobListener.onResponse(Job.LENIENT_PARSER.apply(parser, null));
        } catch (Exception e) {
            jobListener.onFailure(e);
        }
    }

    private Job.Builder parseJobLenientlyFromSource(BytesReference source) throws IOException {
        try (InputStream stream = source.streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                     .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
            return Job.LENIENT_PARSER.apply(parser, null);
        }
    }

    private QueryBuilder buildQuery(String [] tokens) {
        QueryBuilder jobQuery = new TermQueryBuilder(Job.JOB_TYPE.getPreferredName(), Job.ANOMALY_DETECTOR_JOB_TYPE);
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
                shouldQueries.should(new WildcardQueryBuilder(Job.ID.getPreferredName(), token));
                shouldQueries.should(new WildcardQueryBuilder(Job.GROUPS.getPreferredName(), token));
            } else {
                terms.add(token);
            }
        }

        if (terms.isEmpty() == false) {
            shouldQueries.should(new TermsQueryBuilder(Job.ID.getPreferredName(), terms));
            shouldQueries.should(new TermsQueryBuilder(Job.GROUPS.getPreferredName(), terms));
        }

        if (shouldQueries.should().isEmpty() == false) {
            boolQueryBuilder.filter(shouldQueries);
        }

        return boolQueryBuilder;
    }
}
