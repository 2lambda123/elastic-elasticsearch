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

package org.elasticsearch.index.seqno;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

public class RetentionLeaseSyncAction extends
        TransportReplicationAction<RetentionLeaseSyncAction.Request, RetentionLeaseSyncAction.Request, ReplicationResponse> {

    public static String ACTION_NAME = "indices:admin/seq_no/retention_lease_sync";

    private static final Logger LOGGER = LogManager.getLogger(RetentionLeaseSyncAction.class);

    protected Logger getLogger() {
        return LOGGER;
    }

    @Inject
    public RetentionLeaseSyncAction(
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
                ACTION_NAME,
                transportService,
                clusterService,
                indicesService,
                threadPool,
                shardStateAction,
                actionFilters,
                indexNameExpressionResolver,
                RetentionLeaseSyncAction.Request::new,
                RetentionLeaseSyncAction.Request::new,
                ThreadPool.Names.MANAGEMENT);
    }

    public void updateRetentionLeaseForShard(final ShardId shardId, final Collection<RetentionLease> retentionLeases) {
        Objects.requireNonNull(shardId);
        Objects.requireNonNull(retentionLeases);
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            // we have to execute under the system context so that if security is enabled the sync is authorized
            threadContext.markAsSystemContext();
            execute(
                    new RetentionLeaseSyncAction.Request(shardId, retentionLeases),
                    ActionListener.wrap(
                            r -> {},
                            e -> {
                                if (ExceptionsHelper.unwrap(e, AlreadyClosedException.class, IndexShardClosedException.class) == null) {
                                    getLogger().warn(new ParameterizedMessage("{} retention lease sync failed", shardId), e);
                                }
                            }));
        }
    }

    @Override
    protected PrimaryResult<Request, ReplicationResponse> shardOperationOnPrimary(
            final Request request,
            final IndexShard primary) throws Exception {
        Objects.requireNonNull(request);
        Objects.requireNonNull(primary);
        return new PrimaryResult<>(request, new ReplicationResponse());
    }

    @Override
    protected ReplicaResult shardOperationOnReplica(final Request request, final IndexShard replica) throws Exception {
        Objects.requireNonNull(request);
        Objects.requireNonNull(replica);
        replica.updateRetentionLeasesOnReplica(request.getRetentionLeases());
        return new ReplicaResult();
    }

    public static final class Request extends ReplicationRequest<Request> {

        private Collection<RetentionLease> retentionLeases;

        public Collection<RetentionLease> getRetentionLeases() {
            return retentionLeases;
        }

        public Request() {

        }

        public Request(final ShardId shardId, final Collection<RetentionLease> retentionLeases) {
            super(Objects.requireNonNull(shardId));
            this.retentionLeases = Objects.requireNonNull(retentionLeases);
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            retentionLeases = in.readList(RetentionLease::new);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(Objects.requireNonNull(out));
            out.writeCollection(retentionLeases);
        }

        @Override
        public String toString() {
            return "ReplicaRequest{" +
                    "shardId=" + shardId +
                    ", retentionLeases=" + retentionLeases +
                    '}';
        }

    }

    @Override
    protected ReplicationResponse newResponseInstance() {
        return new ReplicationResponse();
    }

}
