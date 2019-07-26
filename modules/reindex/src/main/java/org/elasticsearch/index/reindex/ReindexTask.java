/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class ReindexTask extends AllocatedPersistentTask {

    private static final Logger logger = LogManager.getLogger(ReindexTask.class);

    // TODO: Name
    public static final String NAME = "reindex/job";

    private final NodeClient client;
    private final TaskId taskId;
    private final BulkByScrollTask newTask;
    private final Reindexer reindexer;

    public static class ReindexPersistentTasksExecutor extends PersistentTasksExecutor<ReindexJob> {

        private final ClusterService clusterService;
        private final Client client;
        private final ThreadPool threadPool;
        private final ScriptService scriptService;
        private final ReindexSslConfig reindexSslConfig;

        ReindexPersistentTasksExecutor(ClusterService clusterService, final Client client, ThreadPool threadPool,
                                       ScriptService scriptService, ReindexSslConfig reindexSslConfig) {
            super(NAME, ThreadPool.Names.GENERIC);
            this.clusterService = clusterService;
            this.client = client;
            this.threadPool = threadPool;
            this.scriptService = scriptService;
            this.reindexSslConfig = reindexSslConfig;
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, ReindexJob reindexJob, PersistentTaskState state) {
            ReindexTask reindexTask = (ReindexTask) task;
            reindexTask.startTaskAndNotify(reindexJob);
        }

        @Override
        protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
                                                     PersistentTasksCustomMetaData.PersistentTask<ReindexJob> taskInProgress,
                                                     Map<String, String> headers) {
            ReindexRequest reindexRequest = taskInProgress.getParams().getReindexRequest();
            headers.putAll(taskInProgress.getParams().getHeaders());
            Reindexer reindexer = new Reindexer(clusterService, client, threadPool, scriptService, reindexSslConfig);
            return new ReindexTask(id, type, action, parentTaskId, headers, clusterService, client, reindexer, reindexRequest);
        }
    }

    private ReindexTask(long id, String type, String action, TaskId parentTask, Map<String, String> headers,
                        ClusterService clusterService, Client client, Reindexer reindexer, ReindexRequest reindexRequest) {
        super(id, type, action, "persistent " + reindexRequest.toString(), parentTask, headers);
        taskId = new TaskId(clusterService.localNode().getId(), id);
        this.reindexer = reindexer;
        this.client = (NodeClient) client;
        this.newTask = new BulkByScrollTask(id, type, action, getDescription(), parentTask, headers);
    }

    @Override
    public Status getStatus() {
        return newTask.getStatus();
    }

    public BulkByScrollTask getChildTask() {
        return newTask;
    }

    private void startTaskAndNotify(ReindexJob reindexJob) {
        updatePersistentTaskState(new ReindexJobState(taskId, null, null), new ActionListener<>() {
            @Override
            public void onResponse(PersistentTasksCustomMetaData.PersistentTask<?> persistentTask) {
                doReindex(reindexJob);
            }

            @Override
            public void onFailure(Exception e) {
                logger.info("Failed to update reindex persistent task with ephemeral id", e);
            }
        });
    }

    private void doReindex(ReindexJob reindexJob) {
        TaskManager taskManager = getTaskManager();
        assert taskManager != null : "TaskManager should have been set before reindex started";

        ThreadContext threadContext = client.threadPool().getThreadContext();
        // TODO: Eventually we only want to retain security context
        final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(false);
        try (ThreadContext.StoredContext ignore = stashWithHeaders(threadContext, reindexJob.getHeaders())) {
            ReindexRequest reindexRequest = reindexJob.getReindexRequest();
            reindexRequest.setParentTask(taskId);
            boolean shouldStoreResult = reindexJob.shouldStoreResult();

            reindexer.execute(newTask, reindexRequest, new ContextPreservingActionListener<>(supplier, new ActionListener<>() {
                @Override
                public void onResponse(BulkByScrollResponse response) {
                    updatePersistentTaskState(new ReindexJobState(taskId, response, null), new ActionListener<>() {
                        @Override
                        public void onResponse(PersistentTasksCustomMetaData.PersistentTask<?> persistentTask) {
                            if (shouldStoreResult) {
                                taskManager.storeResult(ReindexTask.this, response, new ActionListener<>() {
                                    @Override
                                    public void onResponse(BulkByScrollResponse response) {
                                        markAsCompleted();
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        logger.info("Failed to store task result", e);
                                        markAsFailed(e);
                                    }
                                });
                            } else {
                                markAsCompleted();
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.info("Failed to update task state to success", e);
                            if (shouldStoreResult) {
                                taskManager.storeResult(ReindexTask.this, e, ActionListener.wrap(() -> markAsFailed(e)));
                            } else {
                                markAsFailed(e);
                            }
                        }
                    });
                }

                @Override
                public void onFailure(Exception ex) {
                    updatePersistentTaskState(new ReindexJobState(taskId, null, wrapException(ex)), new ActionListener<>() {
                        @Override
                        public void onResponse(PersistentTasksCustomMetaData.PersistentTask<?> persistentTask) {
                            if (shouldStoreResult) {
                                taskManager.storeResult(ReindexTask.this, ex, ActionListener.wrap(() -> markAsFailed(ex)));
                            } else {
                                markAsFailed(ex);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.info("Failed to update task state to failed", e);
                            ex.addSuppressed(e);
                            if (shouldStoreResult) {
                                taskManager.storeResult(ReindexTask.this, ex, ActionListener.wrap(() -> markAsFailed(ex)));
                            } else {
                                markAsFailed(ex);
                            }
                        }
                    });
                }
            }));
        }
    }

    private static ElasticsearchException wrapException(Exception ex) {
        if (ex instanceof ElasticsearchException) {
            return (ElasticsearchException) ex;
        } else {
            return new ElasticsearchException(ex);
        }
    }

    // TODO: Copied from ClientHelper in x-pack
    private static ThreadContext.StoredContext stashWithHeaders(ThreadContext threadContext, Map<String, String> headers) {
        final ThreadContext.StoredContext storedContext = threadContext.stashContext();
        if (headers.containsKey(Task.X_OPAQUE_ID)) {
            headers = new HashMap<>(headers);
            // If the X_OPAQUE_ID is present, we should not set it again.
            headers.remove(Task.X_OPAQUE_ID);
        }
        threadContext.copyHeaders(headers.entrySet());
        return storedContext;
    }
}
