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
import org.elasticsearch.client.Client;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

public class ReindexTask extends AllocatedPersistentTask {

    private static final Logger logger = LogManager.getLogger(ReindexTask.class);

    // TODO: Name
    public static final String NAME = "reindex/job";

    private final Client client;

    public static class ReindexPersistentTasksExecutor extends PersistentTasksExecutor<ReindexJob> {

        private final Client client;

        public ReindexPersistentTasksExecutor(final Client client) {
            super(NAME, ThreadPool.Names.GENERIC);
            this.client = client;
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, ReindexJob reindexJob, PersistentTaskState state) {
            ReindexTask reindexTask = (ReindexTask) task;
            reindexTask.doReindex(reindexJob);

        }

        @Override
        protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
                                                     PersistentTasksCustomMetaData.PersistentTask<ReindexJob> taskInProgress,
                                                     Map<String, String> headers) {
            return new ReindexTask(id, type, action, parentTaskId, headers, client);
        }
    }

    private ReindexTask(long id, String type, String action, TaskId parentTask, Map<String, String> headers,
                        Client client) {
        super(id, type, action, "reindex_" + id, parentTask, headers);
        this.client = client;
    }

    private void doReindex(ReindexJob reindexJob) {
        client.execute(ReindexAction.INSTANCE, reindexJob.getReindexRequest(), new ActionListener<>() {
            @Override
            public void onResponse(BulkByScrollResponse response) {
                updatePersistentTaskState(new ReindexJobState(response, null), new ActionListener<>() {
                    @Override
                    public void onResponse(PersistentTasksCustomMetaData.PersistentTask<?> persistentTask) {
                        markAsCompleted();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("Failed to update task state to success.", e);
                        markAsCompleted();
                    }
                });
            }

            @Override
            public void onFailure(Exception ex) {
                updatePersistentTaskState(new ReindexJobState(null,  new ElasticsearchException(ex)), new ActionListener<>() {
                    @Override
                    public void onResponse(PersistentTasksCustomMetaData.PersistentTask<?> persistentTask) {
                        markAsCompleted();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("Failed to update task state to failed.", e);
                        markAsCompleted();
                    }
                });
            }
        });
    }
}
