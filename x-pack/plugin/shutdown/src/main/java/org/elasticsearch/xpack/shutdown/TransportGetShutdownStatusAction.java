/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.ShutdownPersistentTasksStatus;
import org.elasticsearch.cluster.metadata.ShutdownPluginsStatus;
import org.elasticsearch.cluster.metadata.ShutdownShardMigrationStatus;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.shutdown.PluginShutdownService;
import org.elasticsearch.snapshots.SnapshotsInfoService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class TransportGetShutdownStatusAction extends TransportMasterNodeAction<
    GetShutdownStatusAction.Request,
    GetShutdownStatusAction.Response> {

    private final AllocationDeciders allocationDeciders;
    private final AllocationService allocationService;
    private final ClusterInfoService clusterInfoService;
    private final SnapshotsInfoService snapshotsInfoService;
    private final PluginShutdownService pluginShutdownService;

    @Inject
    public TransportGetShutdownStatusAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        AllocationService allocationService,
        AllocationDeciders allocationDeciders,
        ClusterInfoService clusterInfoService,
        SnapshotsInfoService snapshotsInfoService
        PluginShutdownService pluginShutdownService
    ) {
        super(
            GetShutdownStatusAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetShutdownStatusAction.Request::readFrom,
            indexNameExpressionResolver,
            GetShutdownStatusAction.Response::new,
            ThreadPool.Names.SAME
        );
        this.allocationService = allocationService;
        this.allocationDeciders = allocationDeciders;
        this.clusterInfoService = clusterInfoService;
        this.snapshotsInfoService = snapshotsInfoService;
        this.pluginShutdownService = pluginShutdownService;
    }

    @Override
    protected void masterOperation(
        Task task,
        GetShutdownStatusAction.Request request,
        ClusterState state,
        ActionListener<GetShutdownStatusAction.Response> listener
    ) throws Exception {
        NodesShutdownMetadata nodesShutdownMetadata = state.metadata().custom(NodesShutdownMetadata.TYPE);

        GetShutdownStatusAction.Response response;
        if (nodesShutdownMetadata == null) {
            response = new GetShutdownStatusAction.Response(new ArrayList<>());
        } else if (request.getNodeIds().length == 0) {
            final List<SingleNodeShutdownStatus> shutdownStatuses = nodesShutdownMetadata.getAllNodeMetadataMap()
                .values()
                .stream()
                .map(
                    ns -> new SingleNodeShutdownStatus(
                        ns,
                        shardMigrationStatus(state, ns.getNodeId()),
                        new ShutdownPersistentTasksStatus(),
                        new ShutdownPluginsStatus(pluginShutdownService.readyToShutdown(ns.getNodeId(), ns.getType()))
                    )
                )
                .collect(Collectors.toList());
            response = new GetShutdownStatusAction.Response(shutdownStatuses);
        } else {
            new ArrayList<>();
            final Map<String, SingleNodeShutdownMetadata> nodeShutdownMetadataMap = nodesShutdownMetadata.getAllNodeMetadataMap();
            final List<SingleNodeShutdownStatus> shutdownStatuses = Arrays.stream(request.getNodeIds())
                .map(nodeShutdownMetadataMap::get)
                .filter(Objects::nonNull)
                .map(
                    ns -> new SingleNodeShutdownStatus(
                        ns,
                        shardMigrationStatus(state, ns.getNodeId()),
                        new ShutdownPersistentTasksStatus(),
                        new ShutdownPluginsStatus(pluginShutdownService.readyToShutdown(ns.getNodeId(), ns.getType()))
                    )

                )
                .collect(Collectors.toList());
            response = new GetShutdownStatusAction.Response(shutdownStatuses);
        }

        listener.onResponse(response);
    }

    private ShutdownShardMigrationStatus shardMigrationStatus(ClusterState currentState, String nodeId) {
        // First, check if there are any shards currently on this node, and if there are any relocating shards
        int currentShardsOnNode = currentState.getRoutingNodes().node(nodeId).numberOfShardsWithState(ShardRoutingState.STARTED);
        int currentlyRelocatingShards = currentState.getRoutingNodes().node(nodeId).numberOfShardsWithState(ShardRoutingState.RELOCATING);
        int totalRemainingShards = currentlyRelocatingShards + currentShardsOnNode;

        // If there's relocating shards, or no shards on this node, we'll just use the number of shards left to move
        if (currentlyRelocatingShards > 0 || (currentlyRelocatingShards == 0 && currentShardsOnNode == 0)) {
            SingleNodeShutdownMetadata.Status shardStatus = totalRemainingShards == 0
                ? SingleNodeShutdownMetadata.Status.COMPLETE
                : SingleNodeShutdownMetadata.Status.IN_PROGRESS;
            return new ShutdownShardMigrationStatus(shardStatus, totalRemainingShards);
        }

        // If there's no relocating shards and shards still on this node, we need to figure out why
        final RoutingAllocation allocation = new RoutingAllocation(
            allocationDeciders,
            currentState.getRoutingNodes(),
            currentState,
            clusterInfoService.getClusterInfo(),
            snapshotsInfoService.snapshotShardSizes(),
            System.nanoTime()
        );
        allocation.debugDecision(true);

        // Explain shard allocations until we find one that can't move, then stop (as `findFirst` short-circuits)
        final Optional<Tuple<ShardRouting, ShardAllocationDecision>> unmovableShard = currentState.getRoutingNodes()
            .node(nodeId)
            .shardsWithState(ShardRoutingState.STARTED)
            .stream()
            .map(shardRouting -> new Tuple<>(shardRouting, allocationService.explainShardAllocation(shardRouting, allocation)))
            .filter(pair -> pair.v2().getMoveDecision().canRemain() == false && pair.v2().getMoveDecision().forceMove() == false)
            .findFirst();

        if (unmovableShard.isPresent()) {
            // We found a shard that can't be moved, so shard relocation is stalled. Blame the unmovable shard.
            ShardRouting shardRouting = unmovableShard.get().v1();

            return new ShutdownShardMigrationStatus(
                SingleNodeShutdownMetadata.Status.STALLED,
                totalRemainingShards,
                new ParameterizedMessage(
                    "shard [{}] [{}] of index [{}] cannot move, see Cluster Allocation Explain API for details",
                    shardRouting.shardId().getId(),
                    shardRouting.primary() ? "primary" : "replica",
                    shardRouting.index().getName()
                ).getFormattedMessage()
            );
        } else {
            // We couldn't find any shards that can't be moved, so we're just waiting for other recoveries to complete
            return new ShutdownShardMigrationStatus(SingleNodeShutdownMetadata.Status.IN_PROGRESS, totalRemainingShards);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(GetShutdownStatusAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
