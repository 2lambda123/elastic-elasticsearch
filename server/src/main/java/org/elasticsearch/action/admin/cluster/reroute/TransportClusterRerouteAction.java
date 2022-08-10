/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.reroute;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresRequest;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.cluster.routing.allocation.command.AbstractAllocateAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TransportClusterRerouteAction extends TransportMasterNodeAction<ClusterRerouteRequest, ClusterRerouteResponse> {

    private static final Logger logger = LogManager.getLogger(TransportClusterRerouteAction.class);

    private final AllocationService allocationService;

    @Inject
    public TransportClusterRerouteAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        AllocationService allocationService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterRerouteAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterRerouteRequest::new,
            indexNameExpressionResolver,
            ClusterRerouteResponse::new,
            ThreadPool.Names.SAME
        );
        this.allocationService = allocationService;
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterRerouteRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(
        Task task,
        final ClusterRerouteRequest request,
        final ClusterState state,
        final ActionListener<ClusterRerouteResponse> listener
    ) {
        Map<String, List<AbstractAllocateAllocationCommand>> stalePrimaryAllocations = new HashMap<>();
        for (AllocationCommand command : request.getCommands().commands()) {
            if (command instanceof final AllocateStalePrimaryAllocationCommand cmd) {
                stalePrimaryAllocations.computeIfAbsent(cmd.index(), k -> new ArrayList<>()).add(cmd);
            }
        }
        if (stalePrimaryAllocations.isEmpty()) {
            submitStateUpdate(request, listener);
        } else {
            verifyThenSubmitUpdate(request, listener, stalePrimaryAllocations);
        }
    }

    private void verifyThenSubmitUpdate(
        ClusterRerouteRequest request,
        ActionListener<ClusterRerouteResponse> listener,
        Map<String, List<AbstractAllocateAllocationCommand>> stalePrimaryAllocations
    ) {
        transportService.sendRequest(
            transportService.getLocalNode(),
            IndicesShardStoresAction.NAME,
            new IndicesShardStoresRequest().indices(stalePrimaryAllocations.keySet().toArray(Strings.EMPTY_ARRAY)),
            new ActionListenerResponseHandler<>(ActionListener.wrap(response -> {
                Map<String, Map<Integer, List<IndicesShardStoresResponse.StoreStatus>>> status = response.getStoreStatuses();
                Exception e = null;
                for (Map.Entry<String, List<AbstractAllocateAllocationCommand>> entry : stalePrimaryAllocations.entrySet()) {
                    final String index = entry.getKey();
                    final Map<Integer, List<IndicesShardStoresResponse.StoreStatus>> indexStatus = status.get(index);
                    if (indexStatus == null) {
                        // The index in the stale primary allocation request was green and hence filtered out by the store status
                        // request. We ignore it here since the relevant exception will be thrown by the reroute action later on.
                        continue;
                    }
                    for (AbstractAllocateAllocationCommand command : entry.getValue()) {
                        final List<IndicesShardStoresResponse.StoreStatus> shardStatus = indexStatus.get(command.shardId());
                        if (shardStatus == null || shardStatus.isEmpty()) {
                            e = ExceptionsHelper.useOrSuppress(
                                e,
                                new IllegalArgumentException(
                                    "No data for shard [" + command.shardId() + "] of index [" + index + "] found on any node"
                                )
                            );
                        } else if (shardStatus.stream().noneMatch(storeStatus -> {
                            final DiscoveryNode node = storeStatus.getNode();
                            final String nodeInCommand = command.node();
                            return nodeInCommand.equals(node.getName()) || nodeInCommand.equals(node.getId());
                        })) {
                            e = ExceptionsHelper.useOrSuppress(
                                e,
                                new IllegalArgumentException(
                                    "No data for shard ["
                                        + command.shardId()
                                        + "] of index ["
                                        + index
                                        + "] found on node ["
                                        + command.node()
                                        + ']'
                                )
                            );
                        }
                    }
                }
                if (e == null) {
                    submitStateUpdate(request, listener);
                } else {
                    listener.onFailure(e);
                }
            }, listener::onFailure), IndicesShardStoresResponse::new)
        );
    }

    private static final String TASK_SOURCE = "cluster_reroute (api)";

    private void submitStateUpdate(final ClusterRerouteRequest request, final ActionListener<ClusterRerouteResponse> listener) {
        submitUnbatchedTask(
            TASK_SOURCE,
            new ClusterRerouteResponseAckedClusterStateUpdateTask(logger, allocationService, request, listener.map(response -> {
                if (request.dryRun() == false) {
                    response.getExplanations().getYesDecisionMessages().forEach(logger::info);
                }
                return response;
            }))
        );
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    static class ClusterRerouteResponseAckedClusterStateUpdateTask extends ClusterStateUpdateTask implements ClusterStateAckListener {

        private final ClusterRerouteRequest request;
        private final Listener listener;
        private final Logger logger;
        private final AllocationService allocationService;

        ClusterRerouteResponseAckedClusterStateUpdateTask(
            Logger logger,
            AllocationService allocationService,
            ClusterRerouteRequest request,
            ActionListener<ClusterRerouteResponse> listener
        ) {
            super(Priority.IMMEDIATE);
            this.request = request;
            this.listener = new Listener(listener);
            this.logger = logger;
            this.allocationService = allocationService;
        }

        @Override
        public boolean mustAck(DiscoveryNode discoveryNode) {
            return true;
        }

        @Override
        public void onAllNodesAcked() {
            listener.state().onResponse(AcknowledgedResponse.TRUE);
        }

        @Override
        public void onAckFailure(Exception e) {
            listener.state().onResponse(AcknowledgedResponse.FALSE);
        }

        @Override
        public void onAckTimeout() {
            listener.delegate.onResponse(new ClusterRerouteResponse(false, listener.clusterState, new RoutingExplanations()));
        }

        @Override
        public void onFailure(Exception e) {
            logger.debug("failed to perform [" + TASK_SOURCE + "]", e);
            listener.delegate.onFailure(e);
        }

        @Override
        public TimeValue ackTimeout() {
            return request.ackTimeout();
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            var newClusterState = allocationService.reroute(
                currentState,
                request.getCommands(),
                request.explain(),
                request.isRetryFailed(),
                request.dryRun(),
                listener.explanations(),
                listener.reroute()
            );
            listener.setClusterState(newClusterState);
            return request.dryRun() ? currentState : newClusterState;
        }
    }

    private static final class Listener {

        private final ActionListener<ClusterRerouteResponse> delegate;
        private final AtomicInteger components = new AtomicInteger(3);
        private volatile boolean acked;
        private volatile ClusterState clusterState;
        private volatile RoutingExplanations explanations;

        private Listener(ActionListener<ClusterRerouteResponse> delegate) {
            this.delegate = delegate;
        }

        private void setClusterState(ClusterState clusterState) {
            this.clusterState = clusterState;
        }

        private ActionListener<AcknowledgedResponse> state() {
            return new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    Listener.this.acked = acknowledgedResponse.isAcknowledged();
                    triggerDelegateIfReady();
                }

                @Override
                public void onFailure(Exception e) {
                    delegate.onFailure(e);
                }
            };
        }

        private ActionListener<RoutingExplanations> explanations() {
            return new ActionListener<>() {
                @Override
                public void onResponse(RoutingExplanations explanations) {
                    Listener.this.explanations = explanations;
                    triggerDelegateIfReady();
                }

                @Override
                public void onFailure(Exception e) {
                    delegate.onFailure(e);
                }
            };
        }

        private ActionListener<Void> reroute() {
            return new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    triggerDelegateIfReady();
                }

                @Override
                public void onFailure(Exception e) {
                    delegate.onFailure(e);
                }
            };
        }

        private void triggerDelegateIfReady() {
            if (components.decrementAndGet() == 0) {
                delegate.onResponse(new ClusterRerouteResponse(acked, clusterState, explanations));
            }
        }
    }
}
