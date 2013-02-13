/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.discovery;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.rest.RestStatus;

/**
 * A pluggable module allowing to implement discovery of other nodes, publishing of the cluster
 * state to all nodes, electing a master of the cluster that raises cluster state change
 * events.
 */
public interface Discovery extends LifecycleComponent<Discovery> {

    final ClusterBlock NO_MASTER_BLOCK = new ClusterBlock(2, "no master", true, true, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL);

    DiscoveryNode localNode();

    void addListener(InitialStateDiscoveryListener listener);

    void removeListener(InitialStateDiscoveryListener listener);

    String nodeDescription();

    /**
     * Here as a hack to solve dep injection problem...
     */
    void setNodeService(@Nullable NodeService nodeService);

    /**
     * Another hack to solve dep injection problem..., note, this will be called before
     * any start is called.
     */
    void setAllocationService(AllocationService allocationService);

    /**
     * Publish all the changes to the cluster from the master (can be called just by the master). The publish
     * process should not publish this state to the master as well! (the master is sending it...).
     */
    void publish(ClusterState clusterState);
}
