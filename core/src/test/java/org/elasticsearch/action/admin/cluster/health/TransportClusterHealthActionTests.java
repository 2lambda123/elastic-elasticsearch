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

package org.elasticsearch.action.admin.cluster.health;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.core.IsEqual.equalTo;

public class TransportClusterHealthActionTests extends ESTestCase {

    public void testWaitForInitializingShards() throws Exception {
        final String[] indices = {"test"};
        final ClusterHealthRequest request = new ClusterHealthRequest();
        request.waitForNoInitializingShards(true);
        ClusterState clusterState = randomClusterStateWithInitializingShards("test", 0);
        ClusterHealthResponse response = new ClusterHealthResponse("", indices, clusterState);
        assertThat(TransportClusterHealthAction.prepareResponse(request, response, clusterState, null), equalTo(1));

        request.waitForNoInitializingShards(true);
        clusterState = randomClusterStateWithInitializingShards("test", between(1, 10));
        response = new ClusterHealthResponse("", indices, clusterState);
        assertThat(TransportClusterHealthAction.prepareResponse(request, response, clusterState, null), equalTo(0));

        request.waitForNoInitializingShards(false);
        clusterState = randomClusterStateWithInitializingShards("test", randomInt(20));
        response = new ClusterHealthResponse("", indices, clusterState);
        assertThat(TransportClusterHealthAction.prepareResponse(request, response, clusterState, null), equalTo(0));
    }

    ClusterState randomClusterStateWithInitializingShards(String index, int initializingShards) {
        final IndexMetaData indexMetaData = IndexMetaData
            .builder(index)
            .settings(settings(Version.CURRENT))
            .numberOfShards(between(1, 10))
            .numberOfReplicas(randomInt(20))
            .build();
        final ShardId shardId = new ShardId(new Index("index", "uuid"), 0);
        final IndexRoutingTable.Builder routingTable = new IndexRoutingTable.Builder(indexMetaData.getIndex())
            .addShard(TestShardRouting.newShardRouting(shardId, "node-0", true, ShardRoutingState.STARTED));
        for (int i = 0; i < initializingShards; i++) {
            routingTable.addShard(TestShardRouting.newShardRouting(shardId, "node" + i, randomBoolean(), ShardRoutingState.INITIALIZING));
        }
        return ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metaData(MetaData.builder().put(indexMetaData, true))
            .routingTable(RoutingTable.builder().add(routingTable.build()).build())
            .build();
    }
}
