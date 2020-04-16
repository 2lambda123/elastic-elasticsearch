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

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportFuture;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class RemoteRecoveryTargetHandler implements RecoveryTargetHandler {

    private static final Logger logger = LogManager.getLogger(RemoteRecoveryTargetHandler.class);

    private final TransportService transportService;
    private final ThreadPool threadPool;
    private final long recoveryId;
    private final ShardId shardId;
    private final DiscoveryNode targetNode;
    private final RecoverySettings recoverySettings;
    private final Map<Object, RetryableAction<?>> onGoingRetryableActions = ConcurrentCollections.newConcurrentMap();

    private final TransportRequestOptions translogOpsRequestOptions;
    private final TransportRequestOptions fileChunkRequestOptions;

    private final AtomicLong bytesSinceLastPause = new AtomicLong();

    private final Consumer<Long> onSourceThrottle;
    private volatile boolean isCancelled = false;

    public RemoteRecoveryTargetHandler(long recoveryId, ShardId shardId, TransportService transportService,
                                       DiscoveryNode targetNode, RecoverySettings recoverySettings, Consumer<Long> onSourceThrottle) {
        this.transportService = transportService;
        this.threadPool = transportService.getThreadPool();
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.targetNode = targetNode;
        this.recoverySettings = recoverySettings;
        this.onSourceThrottle = onSourceThrottle;
        this.translogOpsRequestOptions = TransportRequestOptions.builder()
                .withType(TransportRequestOptions.Type.RECOVERY)
                .withTimeout(recoverySettings.internalActionLongTimeout())
                .build();
        this.fileChunkRequestOptions = TransportRequestOptions.builder()
                .withType(TransportRequestOptions.Type.RECOVERY)
                .withTimeout(recoverySettings.internalActionTimeout())
                .build();
    }

    @Override
    public void prepareForTranslogOperations(int totalTranslogOps, ActionListener<Void> listener) {
        final TimeValue timeout = recoverySettings.internalActionTimeout();
        final RecoveryPrepareForTranslogOperationsRequest request =
            new RecoveryPrepareForTranslogOperationsRequest(recoveryId, shardId, totalTranslogOps);
        final Consumer<ActionListener<Void>> action = actionListener ->
            transportService.sendRequest(targetNode, PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG, request,
                TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build(),
                new ActionListenerResponseHandler<>(ActionListener.map(actionListener, r -> null),
                    in -> TransportResponse.Empty.INSTANCE, ThreadPool.Names.GENERIC));
        executeRetryableAction(action, listener, timeout);
    }

    @Override
    public void finalizeRecovery(final long globalCheckpoint, final long trimAboveSeqNo, final ActionListener<Void> listener) {
        final TimeValue timeout = recoverySettings.internalActionTimeout();
        final RecoveryFinalizeRecoveryRequest request =
            new RecoveryFinalizeRecoveryRequest(recoveryId, shardId, globalCheckpoint, trimAboveSeqNo);
        final Consumer<ActionListener<Void>> action = actionListener ->
            transportService.sendRequest(targetNode, PeerRecoveryTargetService.Actions.FINALIZE, request,
                TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionLongTimeout()).build(),
                new ActionListenerResponseHandler<>(ActionListener.map(actionListener, r -> null),
                    in -> TransportResponse.Empty.INSTANCE, ThreadPool.Names.GENERIC));
        executeRetryableAction(action, listener, timeout);
    }

    @Override
    public void handoffPrimaryContext(final ReplicationTracker.PrimaryContext primaryContext) {
        TransportFuture<TransportResponse.Empty> handler = new TransportFuture<>(EmptyTransportResponseHandler.INSTANCE_SAME);
        transportService.sendRequest(
            targetNode, PeerRecoveryTargetService.Actions.HANDOFF_PRIMARY_CONTEXT,
            new RecoveryHandoffPrimaryContextRequest(recoveryId, shardId, primaryContext),
            TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build(), handler);
        handler.txGet();
    }

    @Override
    public void indexTranslogOperations(
            final List<Translog.Operation> operations,
            final int totalTranslogOps,
            final long maxSeenAutoIdTimestampOnPrimary,
            final long maxSeqNoOfDeletesOrUpdatesOnPrimary,
            final RetentionLeases retentionLeases,
            final long mappingVersionOnPrimary,
            final ActionListener<Long> listener) {
        final TimeValue timeout = translogOpsRequestOptions.timeout();
        final RecoveryTranslogOperationsRequest request = new RecoveryTranslogOperationsRequest(
            recoveryId,
            shardId,
            operations,
            totalTranslogOps,
            maxSeenAutoIdTimestampOnPrimary,
            maxSeqNoOfDeletesOrUpdatesOnPrimary,
            retentionLeases,
            mappingVersionOnPrimary);

        final Consumer<ActionListener<Long>> action = actionListener ->
            transportService.sendRequest(targetNode, PeerRecoveryTargetService.Actions.TRANSLOG_OPS, request, translogOpsRequestOptions,
                new ActionListenerResponseHandler<>(ActionListener.map(actionListener, r -> r.localCheckpoint),
                    RecoveryTranslogOperationsResponse::new, ThreadPool.Names.GENERIC));

        executeRetryableAction(action, listener, timeout);
    }

    @Override
    public void receiveFileInfo(List<String> phase1FileNames, List<Long> phase1FileSizes, List<String> phase1ExistingFileNames,
                                List<Long> phase1ExistingFileSizes, int totalTranslogOps, ActionListener<Void> listener) {
        final TimeValue timeout = recoverySettings.internalActionTimeout();
        RecoveryFilesInfoRequest recoveryInfoFilesRequest = new RecoveryFilesInfoRequest(recoveryId, shardId,
            phase1FileNames, phase1FileSizes, phase1ExistingFileNames, phase1ExistingFileSizes, totalTranslogOps);
        final Consumer<ActionListener<Void>> action = actionListener ->
            transportService.sendRequest(targetNode, PeerRecoveryTargetService.Actions.FILES_INFO, recoveryInfoFilesRequest,
                TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build(),
                new ActionListenerResponseHandler<>(ActionListener.map(actionListener, r -> null),
                    in -> TransportResponse.Empty.INSTANCE, ThreadPool.Names.GENERIC));
        executeRetryableAction(action, listener, timeout);
    }

    @Override
    public void cleanFiles(int totalTranslogOps, long globalCheckpoint, Store.MetadataSnapshot sourceMetadata,
                           ActionListener<Void> listener) {
        final TimeValue timeout = recoverySettings.internalActionTimeout();
        final RecoveryCleanFilesRequest request =
            new RecoveryCleanFilesRequest(recoveryId, shardId, sourceMetadata, totalTranslogOps, globalCheckpoint);
        final Consumer<ActionListener<Void>> action = actionListener ->
            transportService.sendRequest(targetNode, PeerRecoveryTargetService.Actions.CLEAN_FILES, request,
                TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build(),
                new ActionListenerResponseHandler<>(ActionListener.map(actionListener, r -> null),
                    in -> TransportResponse.Empty.INSTANCE, ThreadPool.Names.GENERIC));
        executeRetryableAction(action, listener, timeout);
    }

    @Override
    public void writeFileChunk(StoreFileMetadata fileMetadata, long position, BytesReference content,
                               boolean lastChunk, int totalTranslogOps, ActionListener<Void> listener) {
        // Pause using the rate limiter, if desired, to throttle the recovery
        final long throttleTimeInNanos;
        // always fetch the ratelimiter - it might be updated in real-time on the recovery settings
        final RateLimiter rl = recoverySettings.rateLimiter();
        if (rl != null) {
            long bytes = bytesSinceLastPause.addAndGet(content.length());
            if (bytes > rl.getMinPauseCheckBytes()) {
                // Time to pause
                bytesSinceLastPause.addAndGet(-bytes);
                try {
                    throttleTimeInNanos = rl.pause(bytes);
                    onSourceThrottle.accept(throttleTimeInNanos);
                } catch (IOException e) {
                    throw new ElasticsearchException("failed to pause recovery", e);
                }
            } else {
                throttleTimeInNanos = 0;
            }
        } else {
            throttleTimeInNanos = 0;
        }

        final TimeValue timeout = fileChunkRequestOptions.timeout();
        /* we send estimateTotalOperations with every request since we collect stats on the target and that way we can
         * see how many translog ops we accumulate while copying files across the network. A future optimization
         * would be in to restart file copy again (new deltas) if we have too many translog ops are piling up.
         */
        final RecoveryFileChunkRequest request = new RecoveryFileChunkRequest(recoveryId, shardId, fileMetadata,
            position, content, lastChunk, totalTranslogOps, throttleTimeInNanos);

        Consumer<ActionListener<Void>> action = actionListener ->
            transportService.sendRequest(targetNode, PeerRecoveryTargetService.Actions.FILE_CHUNK, request, fileChunkRequestOptions,
                new ActionListenerResponseHandler<>(ActionListener.map(actionListener, r -> null),
                    in -> TransportResponse.Empty.INSTANCE, ThreadPool.Names.GENERIC));

        executeRetryableAction(action, listener, timeout);
    }

    @Override
    public void cancel() {
        isCancelled = true;
        final RuntimeException exception = new CancellableThreads.ExecutionCancelledException("recovery was cancelled");
        // Dispatch to generic as cancellation calls can come on the cluster state applier thread
        threadPool.generic().execute(() -> {
            for (Map.Entry<Object, RetryableAction<?>> action : onGoingRetryableActions.entrySet()) {
                action.getValue().cancel(exception);
            }
            onGoingRetryableActions.clear();
        });
    }

    private <T> void executeRetryableAction(Consumer<ActionListener<T>> action, ActionListener<T> listener, TimeValue timeout) {
        final Object key = new Object();
        final ActionListener<T> removeListener = ActionListener.runBefore(listener, () -> onGoingRetryableActions.remove(key));
        final TimeValue initialDelay = TimeValue.timeValueMillis(200);
        final RetryableAction<T> retryableAction = new RetryableAction<>(logger, threadPool, initialDelay, timeout, removeListener) {

            @Override
            public void tryAction(ActionListener<T> listener) {
                action.accept(listener);
            }

            @Override
            public boolean shouldRetry(Exception e) {
                return retryableException(e);
            }
        };
        onGoingRetryableActions.put(key, retryableAction);
        retryableAction.run();
        if (isCancelled) {
            retryableAction.cancel(new CancellableThreads.ExecutionCancelledException("recovery was cancelled"));
        }
    }

    private static boolean retryableException(Exception e) {
        if (e instanceof RemoteTransportException) {
            final Throwable cause = ExceptionsHelper.unwrapCause(e);
            return cause instanceof CircuitBreakingException ||
                cause instanceof EsRejectedExecutionException;
        }
        return false;
    }
}
