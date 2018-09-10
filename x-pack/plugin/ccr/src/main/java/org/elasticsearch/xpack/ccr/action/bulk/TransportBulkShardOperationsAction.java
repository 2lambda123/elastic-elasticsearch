/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.bulk;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class TransportBulkShardOperationsAction
        extends TransportWriteAction<BulkShardOperationsRequest, BulkShardOperationsRequest, BulkShardOperationsResponse> {

    @Inject
    public TransportBulkShardOperationsAction(
            final Settings settings,
            final TransportService transportService,
            final ClusterService clusterService,
            final IndicesService indicesService,
            final ThreadPool threadPool,
            final ShardStateAction shardStateAction,
            final ActionFilters actionFilters,
            final IndexNameExpressionResolver indexNameExpressionResolver) {
        super(
                settings,
                BulkShardOperationsAction.NAME,
                transportService,
                clusterService,
                indicesService,
                threadPool,
                shardStateAction,
                actionFilters,
                indexNameExpressionResolver,
                BulkShardOperationsRequest::new,
                BulkShardOperationsRequest::new,
                ThreadPool.Names.WRITE);
    }

    @Override
    protected WritePrimaryResult<BulkShardOperationsRequest, BulkShardOperationsResponse> shardOperationOnPrimary(
            final BulkShardOperationsRequest request, final IndexShard primary) throws Exception {
        return shardOperationOnPrimary(request.shardId(), request.getOperations(), primary, logger);
    }

    // public for testing purposes only
    public static WritePrimaryResult<BulkShardOperationsRequest, BulkShardOperationsResponse> shardOperationOnPrimary(
            final ShardId shardId,
            final List<Translog.Operation> sourceOperations,
            final IndexShard primary,
            final Logger logger) throws IOException {
        final List<Translog.Operation> targetOperations = sourceOperations.stream().map(operation -> {
            final Translog.Operation operationWithPrimaryTerm;
            switch (operation.opType()) {
                case INDEX:
                    final Translog.Index index = (Translog.Index) operation;
                    operationWithPrimaryTerm = new Translog.Index(
                            index.type(),
                            index.id(),
                            index.seqNo(),
                            primary.getOperationPrimaryTerm(),
                            index.version(),
                            index.versionType(),
                            BytesReference.toBytes(index.source()),
                            index.routing(),
                            index.parent(),
                            index.getAutoGeneratedIdTimestamp());
                    break;
                case DELETE:
                    final Translog.Delete delete = (Translog.Delete) operation;
                    operationWithPrimaryTerm = new Translog.Delete(
                            delete.type(),
                            delete.id(),
                            delete.uid(),
                            delete.seqNo(),
                            primary.getOperationPrimaryTerm(),
                            delete.version(),
                            delete.versionType());
                    break;
                case NO_OP:
                    final Translog.NoOp noOp = (Translog.NoOp) operation;
                    operationWithPrimaryTerm = new Translog.NoOp(noOp.seqNo(), primary.getOperationPrimaryTerm(), noOp.reason());
                    break;
                default:
                    throw new IllegalStateException("unexpected operation type [" + operation.opType() + "]");
            }
            return operationWithPrimaryTerm;
        }).collect(Collectors.toList());
        final Translog.Location location = applyTranslogOperations(targetOperations, primary, Engine.Operation.Origin.PRIMARY);
        final BulkShardOperationsRequest replicaRequest = new BulkShardOperationsRequest(shardId, targetOperations);
        return new CcrWritePrimaryResult(replicaRequest, location, primary, logger);
    }

    @Override
    protected WriteReplicaResult<BulkShardOperationsRequest> shardOperationOnReplica(
            final BulkShardOperationsRequest request, final IndexShard replica) throws Exception {
        final Translog.Location location = applyTranslogOperations(request.getOperations(), replica, Engine.Operation.Origin.REPLICA);
        return new WriteReplicaResult<>(request, location, null, replica, logger);
    }

    // public for testing purposes only
    public static Translog.Location applyTranslogOperations(
            final List<Translog.Operation> operations, final IndexShard shard, final Engine.Operation.Origin origin) throws IOException {
        Translog.Location location = null;
        for (final Translog.Operation operation : operations) {
            final Engine.Result result = shard.applyTranslogOperation(operation, origin);
            assert result.getSeqNo() == operation.seqNo();
            assert result.getResultType() == Engine.Result.Type.SUCCESS;
            location = locationToSync(location, result.getTranslogLocation());
        }
        assert operations.size() == 0 || location != null;
        return location;
    }

    @Override
    protected BulkShardOperationsResponse newResponseInstance() {
        return new BulkShardOperationsResponse();
    }

    /**
     * Custom write result to include global checkpoint after ops have been replicated.
     */
    static class CcrWritePrimaryResult extends WritePrimaryResult<BulkShardOperationsRequest, BulkShardOperationsResponse> {

        CcrWritePrimaryResult(BulkShardOperationsRequest request, Translog.Location location, IndexShard primary, Logger logger) {
            super(request, new BulkShardOperationsResponse(), location, null, primary, logger);
        }

        @Override
        public synchronized void respond(ActionListener<BulkShardOperationsResponse> listener) {
            final BulkShardOperationsResponse response = finalResponseIfSuccessful;
            final SeqNoStats seqNoStats = primary.seqNoStats();
            // return a fresh global checkpoint after the operations have been replicated for the shard follow task
            response.setGlobalCheckpoint(seqNoStats.getGlobalCheckpoint());
            response.setMaxSeqNo(seqNoStats.getMaxSeqNo());
            listener.onResponse(response);
        }

    }

}
