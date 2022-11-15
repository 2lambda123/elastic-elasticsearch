/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.shutdown.CheckShardsOnDataPathRequest;
import org.elasticsearch.action.admin.cluster.node.shutdown.CheckShardsOnDataPathResponse;
import org.elasticsearch.action.admin.cluster.node.shutdown.NodeCheckShardsOnDataPathResponse;
import org.elasticsearch.action.admin.cluster.node.shutdown.TransportCheckShardsOnDataPathAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.oneOf;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CheckShardsOnDataPathIT extends ESIntegTestCase {

    public void testCheckShards() throws Exception {
        internalCluster().startMasterOnlyNode();
        String node1 = internalCluster().startDataOnlyNode();
        String node2 = internalCluster().startDataOnlyNode();
        String index1Name = "index1";
        int index1shards = randomIntBetween(1, 5);
        createIndex("index1", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, index1shards).build());
        ensureGreen(index1Name);
        var shardIds = clusterService().state()
            .routingTable()
            .allShards(index1Name)
            .stream()
            .map(ShardRouting::shardId)
            .collect(Collectors.toSet());
        String node1Id = internalCluster().clusterService(node1).localNode().getId();
        String node2Id = internalCluster().clusterService(node2).localNode().getId();
        Set<ShardId> shardIdsToCheck = new HashSet<>(shardIds);
        boolean includeUnknownShardId = randomBoolean();
        if (includeUnknownShardId) {
            shardIdsToCheck.add(new ShardId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(), randomIntBetween(0, 10)));
        }
        CheckShardsOnDataPathRequest req = new CheckShardsOnDataPathRequest(shardIdsToCheck, node1Id, node2Id);
        CheckShardsOnDataPathResponse resp = client().execute(TransportCheckShardsOnDataPathAction.TYPE, req).get();
        var nodeResponses = resp.getNodes();
        var nodeFailures = resp.failures();
        if (includeUnknownShardId) {
            assertTrue(nodeResponses.isEmpty());
            assertThat(nodeFailures.size(), equalTo(2));
            for (FailedNodeException exception: nodeFailures) {
                assertThat(exception.nodeId(), oneOf(node1Id, node2Id));
                assertThat(exception.getDetailedMessage(), containsString("node doesn't have metadata for index"));
            }
        } else {
            assertThat(nodeResponses.size(), equalTo(2));
            assertTrue(resp.failures().isEmpty());
            for (NodeCheckShardsOnDataPathResponse nodeResponse : nodeResponses) {
                assertThat(nodeResponse.getShardIds(), equalTo(shardIds));
            }
        }
    }
}
