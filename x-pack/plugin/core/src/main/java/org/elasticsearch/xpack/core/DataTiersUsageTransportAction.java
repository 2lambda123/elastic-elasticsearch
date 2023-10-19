/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.cluster.routing.allocation.stats.NodeDataTiersUsage;
import org.elasticsearch.xpack.cluster.routing.allocation.stats.NodesDataTiersUsageAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class merges the node specific data tier usage stats that were locally sourced.
 */
public class DataTiersUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final Client client;

    @Inject
    public DataTiersUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        super(
            XPackUsageFeatureAction.DATA_TIERS.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver
        );
        this.client = client;
    }

    @Override
    protected void masterOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        new ParentTaskAssigningClient(client, clusterService.localNode(), task).admin()
            .cluster()
            .execute(
                NodesDataTiersUsageAction.INSTANCE,
                new NodesDataTiersUsageAction.NodesRequest(),
                listener.delegateFailureAndWrap((delegate, response) -> {
                    // Generate tier specific stats for the nodes and indices
                    delegate.onResponse(new XPackUsageFeatureResponse(new DataTiersFeatureSetUsage(aggregateStats(response.getNodes()))));
                })
            );
    }

    /**
     * Accumulator to hold intermediate data tier stats before final calculation.
     */
    private static class TierStatsAccumulator {
        int nodeCount = 0;
        Set<String> indexNames = new HashSet<>();
        int totalShardCount = 0;
        long totalByteCount = 0;
        long docCount = 0;
        int primaryShardCount = 0;
        long primaryByteCount = 0L;
        final TDigestState valueSketch = TDigestState.create(1000);
    }

    // Visible for testing
    static Map<String, DataTiersFeatureSetUsage.TierSpecificStats> aggregateStats(List<NodeDataTiersUsage> nodeDataTiersUsages) {
        Map<String, TierStatsAccumulator> statsAccumulators = new HashMap<>();
        for (NodeDataTiersUsage nodeDataTiersUsage : nodeDataTiersUsages) {
            // This ensures we only count the nodes that responded
            aggregateDataTierNodeCounts(nodeDataTiersUsage, statsAccumulators);
            aggregateDataTierIndexStats(nodeDataTiersUsage, statsAccumulators);
        }
        Map<String, DataTiersFeatureSetUsage.TierSpecificStats> results = new HashMap<>();
        for (Map.Entry<String, TierStatsAccumulator> entry : statsAccumulators.entrySet()) {
            results.put(entry.getKey(), aggregateFinalTierStats(entry.getValue()));
        }
        return results;
    }

    /**
     * Determine which data tiers each node belongs to (if any), and increment the node counts for those tiers.
     */
    private static void aggregateDataTierNodeCounts(NodeDataTiersUsage nodeStats, Map<String, TierStatsAccumulator> tiersStats) {
        nodeStats.getNode()
            .getRoles()
            .stream()
            .map(DiscoveryNodeRole::roleName)
            .filter(DataTier::validTierName)
            .forEach(tier -> tiersStats.computeIfAbsent(tier, k -> new TierStatsAccumulator()).nodeCount++);
    }

    /**
     * Iterate the preferred tiers of the indices for a node and aggregate their stats.
     */
    private static void aggregateDataTierIndexStats(NodeDataTiersUsage nodeDataTiersUsage, Map<String, TierStatsAccumulator> accumulators) {
        for (Map.Entry<String, NodeDataTiersUsage.UsageStats> entry : nodeDataTiersUsage.getUsageStatsByTier().entrySet()) {
            String tier = entry.getKey();
            NodeDataTiersUsage.UsageStats usage = entry.getValue();
            if (DataTier.validTierName(tier)) {
                TierStatsAccumulator accumulator = accumulators.computeIfAbsent(tier, k -> new TierStatsAccumulator());
                accumulator.indexNames.addAll(usage.getIndices());
                accumulator.docCount += usage.getDocCount();
                accumulator.totalByteCount += usage.getTotalSize();
                accumulator.totalShardCount += usage.getTotalShardCount();
                for (Long primaryShardSize : usage.getPrimaryShardSizes()) {
                    accumulator.primaryShardCount += 1;
                    accumulator.primaryByteCount += primaryShardSize;
                    accumulator.valueSketch.add(primaryShardSize);
                }
            }
        }
    }

    private static DataTiersFeatureSetUsage.TierSpecificStats aggregateFinalTierStats(TierStatsAccumulator accumulator) {
        long primaryShardSizeMedian = (long) accumulator.valueSketch.quantile(0.5);
        long primaryShardSizeMAD = computeMedianAbsoluteDeviation(accumulator.valueSketch);
        return new DataTiersFeatureSetUsage.TierSpecificStats(
            accumulator.nodeCount,
            accumulator.indexNames.size(),
            accumulator.totalShardCount,
            accumulator.primaryShardCount,
            accumulator.docCount,
            accumulator.totalByteCount,
            accumulator.primaryByteCount,
            primaryShardSizeMedian,
            primaryShardSizeMAD
        );
    }

    // Visible for testing
    static long computeMedianAbsoluteDeviation(TDigestState valuesSketch) {
        if (valuesSketch.size() == 0) {
            return 0;
        } else {
            final double approximateMedian = valuesSketch.quantile(0.5);
            final TDigestState approximatedDeviationsSketch = TDigestState.createUsingParamsFrom(valuesSketch);
            valuesSketch.centroids().forEach(centroid -> {
                final double deviation = Math.abs(approximateMedian - centroid.mean());
                approximatedDeviationsSketch.add(deviation, centroid.count());
            });

            return (long) approximatedDeviationsSketch.quantile(0.5);
        }
    }
}
