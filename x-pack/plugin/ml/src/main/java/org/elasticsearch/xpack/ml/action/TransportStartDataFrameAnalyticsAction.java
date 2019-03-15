/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsManager;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;

import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Starts the persistent task for running data frame analytics.
 *
 * TODO Add to the upgrade mode action
 */
public class TransportStartDataFrameAnalyticsAction
    extends TransportMasterNodeAction<StartDataFrameAnalyticsAction.Request, AcknowledgedResponse> {

    private static final Logger LOGGER = LogManager.getLogger(TransportStartDataFrameAnalyticsAction.class);

    private final XPackLicenseState licenseState;
    private final Client client;
    private final PersistentTasksService persistentTasksService;
    private final DataFrameAnalyticsConfigProvider configProvider;

    @Inject
    public TransportStartDataFrameAnalyticsAction(TransportService transportService, Client client, ClusterService clusterService,
                                                  ThreadPool threadPool, ActionFilters actionFilters, XPackLicenseState licenseState,
                                                  IndexNameExpressionResolver indexNameExpressionResolver,
                                                  PersistentTasksService persistentTasksService,
                                                  DataFrameAnalyticsConfigProvider configProvider) {
        super(StartDataFrameAnalyticsAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                StartDataFrameAnalyticsAction.Request::new);
        this.licenseState = licenseState;
        this.client = client;
        this.persistentTasksService = persistentTasksService;
        this.configProvider = configProvider;
    }

    @Override
    protected String executor() {
        // This api doesn't do heavy or blocking operations (just delegates PersistentTasksService),
        // so we can do this on the network thread
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(StartDataFrameAnalyticsAction.Request request, ClusterState state) {
        // We only delegate here to PersistentTasksService, but if there is a metadata writeblock,
        // then delegating to PersistentTasksService doesn't make a whole lot of sense,
        // because PersistentTasksService will then fail.
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(StartDataFrameAnalyticsAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) {
        if (licenseState.isMachineLearningAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
            return;
        }

        StartDataFrameAnalyticsAction.TaskParams taskParams = new StartDataFrameAnalyticsAction.TaskParams(request.getId());

        // Wait for analytics to be started
        ActionListener<PersistentTasksCustomMetaData.PersistentTask<StartDataFrameAnalyticsAction.TaskParams>> waitForAnalyticsToStart =
            new ActionListener<PersistentTasksCustomMetaData.PersistentTask<StartDataFrameAnalyticsAction.TaskParams>>() {
                @Override
                public void onResponse(PersistentTasksCustomMetaData.PersistentTask<StartDataFrameAnalyticsAction.TaskParams> task) {
                    waitForAnalyticsStarted(task, request.getTimeout(), listener);
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof ResourceAlreadyExistsException) {
                        e = new ElasticsearchStatusException("Cannot open data frame analytics [" + request.getId() +
                            "] because it has already been opened", RestStatus.CONFLICT, e);
                    }
                    listener.onFailure(e);
                }
            };

        // Start persistent task
        ActionListener<Boolean> validateListener = ActionListener.wrap(
            validated -> persistentTasksService.sendStartRequest(MlTasks.dataFrameAnalyticsTaskId(request.getId()),
                MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME, taskParams, waitForAnalyticsToStart),
            listener::onFailure
        );

        // Validate config
        ActionListener<DataFrameAnalyticsConfig> configListener = ActionListener.wrap(
            config -> DataFrameDataExtractorFactory.validateConfigAndSourceIndex(client, config, validateListener),
            listener::onFailure
        );

        // Get config
        configProvider.get(request.getId(), configListener);
    }

    private void waitForAnalyticsStarted(PersistentTasksCustomMetaData.PersistentTask<StartDataFrameAnalyticsAction.TaskParams> task,
                                         TimeValue timeout, ActionListener<AcknowledgedResponse> listener) {
        AnalyticsPredicate predicate = new AnalyticsPredicate();
        persistentTasksService.waitForPersistentTaskCondition(task.getId(), predicate, timeout,

            new PersistentTasksService.WaitForPersistentTaskListener<PersistentTaskParams>() {

                @Override
                public void onResponse(PersistentTasksCustomMetaData.PersistentTask<PersistentTaskParams> persistentTask) {
                    if (predicate.exception != null) {
                        // We want to return to the caller without leaving an unassigned persistent task, to match
                        // what would have happened if the error had been detected in the "fast fail" validation
                        cancelAnalyticsStart(task, predicate.exception, listener);
                    } else {
                        listener.onResponse(new AcknowledgedResponse(true));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onFailure(new ElasticsearchException("Starting data frame analytics [" + task.getParams().getId()
                        + "] timed out after [" + timeout + "]"));
                }
        });
    }

    /**
     * Important: the methods of this class must NOT throw exceptions.  If they did then the callers
     * of endpoints waiting for a condition tested by this predicate would never get a response.
     */
    private class AnalyticsPredicate implements Predicate<PersistentTasksCustomMetaData.PersistentTask<?>> {

        private volatile Exception exception;

        @Override
        public boolean test(PersistentTasksCustomMetaData.PersistentTask<?> persistentTask) {
            if (persistentTask == null) {
                return false;
            }

            PersistentTasksCustomMetaData.Assignment assignment = persistentTask.getAssignment();
            if (assignment != null && assignment.equals(PersistentTasksCustomMetaData.INITIAL_ASSIGNMENT) == false &&
                assignment.isAssigned() == false) {
                // Assignment has failed despite passing our "fast fail" validation
                exception = new ElasticsearchStatusException("Could not start data frame analytics task, allocation explanation [" +
                    assignment.getExplanation() + "]", RestStatus.TOO_MANY_REQUESTS);
                return true;
            }
            DataFrameAnalyticsTaskState taskState = (DataFrameAnalyticsTaskState) persistentTask.getState();
            DataFrameAnalyticsState analyticsState = taskState == null ? DataFrameAnalyticsState.STOPPED : taskState.getState();
            return analyticsState == DataFrameAnalyticsState.STARTED;
        }
    }

    private void cancelAnalyticsStart(
        PersistentTasksCustomMetaData.PersistentTask<StartDataFrameAnalyticsAction.TaskParams> persistentTask, Exception exception,
        ActionListener<AcknowledgedResponse> listener) {
        persistentTasksService.sendRemoveRequest(persistentTask.getId(),
            new ActionListener<PersistentTasksCustomMetaData.PersistentTask<?>>() {
                @Override
                public void onResponse(PersistentTasksCustomMetaData.PersistentTask<?> task) {
                    // We succeeded in cancelling the persistent task, but the
                    // problem that caused us to cancel it is the overall result
                    listener.onFailure(exception);
                }

                @Override
                public void onFailure(Exception e) {
                    LOGGER.error("[" + persistentTask.getParams().getId() + "] Failed to cancel persistent task that could " +
                        "not be assigned due to [" + exception.getMessage() + "]", e);
                    listener.onFailure(exception);
                }
            }
        );
    }

    public static class DataFrameAnalyticsTask extends AllocatedPersistentTask implements StartDataFrameAnalyticsAction.TaskMatcher {

        private final StartDataFrameAnalyticsAction.TaskParams taskParams;
        @Nullable
        private volatile Long reindexingTaskId;

        public DataFrameAnalyticsTask(long id, String type, String action, TaskId parentTask, Map<String, String> headers,
                                      StartDataFrameAnalyticsAction.TaskParams taskParams) {
            super(id, type, action, MlTasks.DATA_FRAME_ANALYTICS_TASK_ID_PREFIX + taskParams.getId(), parentTask, headers);
            this.taskParams = Objects.requireNonNull(taskParams);
        }

        public StartDataFrameAnalyticsAction.TaskParams getParams() {
            return taskParams;
        }

        public void setReindexingTaskId(long reindexingTaskId) {
            this.reindexingTaskId = reindexingTaskId;
        }

        @Nullable
        public Long getReindexingTaskId() {
            return reindexingTaskId;
        }
    }

    public static class TaskExecutor extends PersistentTasksExecutor<StartDataFrameAnalyticsAction.TaskParams> {

        private final DataFrameAnalyticsManager manager;

        public TaskExecutor(DataFrameAnalyticsManager manager) {
            super(MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME, MachineLearning.UTILITY_THREAD_POOL_NAME);
            this.manager = Objects.requireNonNull(manager);
        }

        @Override
        protected AllocatedPersistentTask createTask(
            long id, String type, String action, TaskId parentTaskId,
            PersistentTasksCustomMetaData.PersistentTask<StartDataFrameAnalyticsAction.TaskParams> persistentTask,
            Map<String, String> headers) {
            return new DataFrameAnalyticsTask(id, type, action, parentTaskId, headers, persistentTask.getParams());
        }

        @Override
        protected DiscoveryNode selectLeastLoadedNode(ClusterState clusterState, Predicate<DiscoveryNode> selector) {
            // For starters, let's just select the least loaded ML node
            // TODO implement memory-based load balancing
            return super.selectLeastLoadedNode(clusterState, MachineLearning::isMlNode);
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, StartDataFrameAnalyticsAction.TaskParams params,
                                     PersistentTaskState state) {
            LOGGER.info("[{}] Starting data frame analytics", params.getId());
            DataFrameAnalyticsTaskState analyticsTaskState = (DataFrameAnalyticsTaskState) state;

            // If we are "stopping" there is nothing to do
            if (analyticsTaskState != null && analyticsTaskState.getState() == DataFrameAnalyticsState.STOPPING) {
                return;
            }

            if (analyticsTaskState == null) {
                DataFrameAnalyticsTaskState startedState = new DataFrameAnalyticsTaskState(DataFrameAnalyticsState.STARTED,
                    task.getAllocationId());
                task.updatePersistentTaskState(startedState, ActionListener.wrap(
                    response -> manager.execute((DataFrameAnalyticsTask) task, DataFrameAnalyticsState.STARTED),
                    task::markAsFailed));
            } else {
                manager.execute((DataFrameAnalyticsTask)task, analyticsTaskState.getState());
            }

        }
    }
}
