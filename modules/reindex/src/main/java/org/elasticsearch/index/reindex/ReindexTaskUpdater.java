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
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.rest.RestStatus;

import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

public class ReindexTaskUpdater implements Reindexer.CheckpointListener {

    private static final int MAX_ASSIGNMENT_ATTEMPTS = 10;

    private static final Logger logger = LogManager.getLogger(ReindexTask.class);

    private final ReindexIndexClient reindexIndexClient;
    private final String persistentTaskId;
    private final long allocationId;
    private final Consumer<BulkByScrollTask.Status> committedCallback;
    private final Semaphore semaphore = new Semaphore(1);

    private int assignmentAttempts = 0;
    private ReindexTaskState lastState;
    private boolean isDone = false;

    public ReindexTaskUpdater(ReindexIndexClient reindexIndexClient, String persistentTaskId, long allocationId,
                              Consumer<BulkByScrollTask.Status> committedCallback) {
        this.reindexIndexClient = reindexIndexClient;
        this.persistentTaskId = persistentTaskId;
        this.allocationId = allocationId;
        this.committedCallback = committedCallback;
    }

    public void assign(AssignmentListener listener) {
        ++assignmentAttempts;
        reindexIndexClient.getReindexTaskDoc(persistentTaskId, new ActionListener<>() {
            @Override
            public void onResponse(ReindexTaskState taskState) {
                long term = taskState.getPrimaryTerm();
                long seqNo = taskState.getSeqNo();
                ReindexTaskStateDoc oldDoc = taskState.getStateDoc();
                ReindexRequest request = oldDoc.getReindexRequest();
                BulkByScrollResponse response = oldDoc.getReindexResponse();
                ElasticsearchException exception = oldDoc.getException();
                RestStatus failureStatusCode = oldDoc.getFailureStatusCode();
                ScrollableHitSource.Checkpoint checkpoint = oldDoc.getCheckpoint();

                if (oldDoc.getAllocationId() == null || allocationId > oldDoc.getAllocationId()) {
                    ReindexTaskStateDoc newDoc = new ReindexTaskStateDoc(request, allocationId, response, exception, failureStatusCode,
                        checkpoint);
                    reindexIndexClient.updateReindexTaskDoc(persistentTaskId, newDoc, term, seqNo, new ActionListener<>() {
                        @Override
                        public void onResponse(ReindexTaskState newTaskState) {
                            lastState = newTaskState;
                            listener.onAssignment(newTaskState);
                        }

                        @Override
                        public void onFailure(Exception ex) {
                            if (ex instanceof VersionConflictEngineException) {
                                // There has been an indexing operation since the GET operation. Try
                                // again if there are assignment attempts left.
                                if (assignmentAttempts < MAX_ASSIGNMENT_ATTEMPTS) {
                                    assign(listener);
                                } else {
                                    logger.info("Failed to write allocation id to reindex task doc after maximum retry attempts", ex);
                                    listener.onFailure(ReindexJobState.Status.ASSIGNMENT_FAILED, ex);
                                }
                            } else {
                                logger.info("Failed to write allocation id to reindex task doc", ex);
                                listener.onFailure(ReindexJobState.Status.FAILED_TO_WRITE_TO_REINDEX_INDEX, ex);
                            }
                        }
                    });
                } else {
                    ElasticsearchException ex = new ElasticsearchException("A newer task has already been allocated");
                    listener.onFailure(ReindexJobState.Status.ASSIGNMENT_FAILED, ex);
                }
            }

            @Override
            public void onFailure(Exception ex) {
                logger.info("Failed to fetch reindex task doc", ex);
                listener.onFailure(ReindexJobState.Status.FAILED_TO_READ_FROM_REINDEX_INDEX, ex);
            }
        });
    }

    @Override
    public void onCheckpoint(ScrollableHitSource.Checkpoint checkpoint, BulkByScrollTask.Status status) {
        // todo: need some kind of throttling here, no need to do this all the time.
        // only do one checkpoint at a time, in case checkpointing is too slow.
        if (semaphore.tryAcquire() && isDone == false) {
            ReindexTaskStateDoc nextState = lastState.getStateDoc().withCheckpoint(checkpoint, status);
            // todo: clarify whether updateReindexTaskDoc can fail with exception and use conditional update
            long term = lastState.getPrimaryTerm();
            long seqNo = lastState.getSeqNo();
            reindexIndexClient.updateReindexTaskDoc(persistentTaskId, nextState, term, seqNo, new ActionListener<>() {
                @Override
                public void onResponse(ReindexTaskState taskState) {
                    lastState = taskState;
                    committedCallback.accept(status);
                    semaphore.release();
                }

                @Override
                public void onFailure(Exception e) {
                    semaphore.release();
                }
            });
        }
    }

    public void finish(ReindexTaskStateDoc state, ActionListener<ReindexTaskState> listener) {
        // TODO: Maybe just normal acquire
        semaphore.acquireUninterruptibly();
        isDone = true;
        long term = lastState.getPrimaryTerm();
        long seqNo = lastState.getSeqNo();
        reindexIndexClient.updateReindexTaskDoc(persistentTaskId, state, term, seqNo, new ActionListener<>() {
            @Override
            public void onResponse(ReindexTaskState taskState) {
                lastState = null;
                semaphore.release();
                listener.onResponse(taskState);

            }

            @Override
            public void onFailure(Exception e) {
                semaphore.release();
                listener.onFailure(e);
            }
        });
    }

    interface AssignmentListener {

        void onAssignment(ReindexTaskState reindexTaskState);

        void onFailure(ReindexJobState.Status status, Exception exception);
    }
}
