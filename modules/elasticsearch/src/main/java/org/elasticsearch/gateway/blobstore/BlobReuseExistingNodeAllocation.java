/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.gateway.blobstore;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.*;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.index.gateway.CommitPoint;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.transport.ConnectTransportException;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * @author kimchy (shay.banon)
 */
public class BlobReuseExistingNodeAllocation extends NodeAllocation {

    private final Node node;

    private final TransportNodesListShardStoreMetaData transportNodesListShardStoreMetaData;

    private final TimeValue listTimeout;

    private final ConcurrentMap<ShardId, CommitPoint> cachedCommitPoints = ConcurrentCollections.newConcurrentMap();

    private final ConcurrentMap<ShardId, ConcurrentMap<DiscoveryNode, TransportNodesListShardStoreMetaData.StoreFilesMetaData>> cachedStores = ConcurrentCollections.newConcurrentMap();

    @Inject public BlobReuseExistingNodeAllocation(Settings settings, Node node,
                                                   TransportNodesListShardStoreMetaData transportNodesListShardStoreMetaData) {
        super(settings);
        this.node = node; // YACK!, we need the Gateway, but it creates crazy circular dependency
        this.transportNodesListShardStoreMetaData = transportNodesListShardStoreMetaData;

        this.listTimeout = componentSettings.getAsTime("list_timeout", TimeValue.timeValueSeconds(30));
    }

    @Override public void applyStartedShards(NodeAllocations nodeAllocations, StartedRerouteAllocation allocation) {
        for (ShardRouting shardRouting : allocation.startedShards()) {
            cachedCommitPoints.remove(shardRouting.shardId());
            cachedStores.remove(shardRouting.shardId());
        }
    }

    @Override public void applyFailedShards(NodeAllocations nodeAllocations, FailedRerouteAllocation allocation) {
        for (ShardRouting shardRouting : allocation.failedShards()) {
            cachedCommitPoints.remove(shardRouting.shardId());
            cachedStores.remove(shardRouting.shardId());
        }
    }

    @Override public boolean allocateUnassigned(NodeAllocations nodeAllocations, RoutingAllocation allocation) {
        boolean changed = false;

        DiscoveryNodes nodes = allocation.nodes();
        RoutingNodes routingNodes = allocation.routingNodes();

        if (nodes.dataNodes().isEmpty()) {
            return changed;
        }

        if (!routingNodes.hasUnassigned()) {
            return changed;
        }

        Iterator<MutableShardRouting> unassignedIterator = routingNodes.unassigned().iterator();
        while (unassignedIterator.hasNext()) {
            MutableShardRouting shard = unassignedIterator.next();

            // pre-check if it can be allocated to any node that currently exists, so we won't list the store for it for nothing
            boolean canBeAllocatedToAtLeastOneNode = false;
            for (DiscoveryNode discoNode : nodes.dataNodes().values()) {
                RoutingNode node = routingNodes.node(discoNode.id());
                if (node == null) {
                    continue;
                }
                // if its THROTTLING, we are not going to allocate it to this node, so ignore it as well
                if (nodeAllocations.canAllocate(shard, node, allocation).allocate()) {
                    canBeAllocatedToAtLeastOneNode = true;
                    break;
                }
            }

            if (!canBeAllocatedToAtLeastOneNode) {
                continue;
            }

            ConcurrentMap<DiscoveryNode, TransportNodesListShardStoreMetaData.StoreFilesMetaData> shardStores = buildShardStores(nodes, shard);

            long lastSizeMatched = 0;
            DiscoveryNode lastDiscoNodeMatched = null;
            RoutingNode lastNodeMatched = null;

            for (Map.Entry<DiscoveryNode, TransportNodesListShardStoreMetaData.StoreFilesMetaData> nodeStoreEntry : shardStores.entrySet()) {
                DiscoveryNode discoNode = nodeStoreEntry.getKey();
                TransportNodesListShardStoreMetaData.StoreFilesMetaData storeFilesMetaData = nodeStoreEntry.getValue();
                logger.trace("{}: checking node [{}]", shard, discoNode);

                if (storeFilesMetaData == null) {
                    // already allocated on that node...
                    continue;
                }

                RoutingNode node = routingNodes.node(discoNode.id());
                if (node == null) {
                    continue;
                }

                // check if we can allocate on that node...
                // we only check for NO, since if this node is THROTTLING and it has enough "same data"
                // then we will try and assign it next time
                if (nodeAllocations.canAllocate(shard, node, allocation) == Decision.NO) {
                    continue;
                }

                // if it is already allocated, we can't assign to it...
                if (storeFilesMetaData.allocated()) {
                    continue;
                }


                // if its a primary, it will be recovered from the gateway, find one that is closet to it
                if (shard.primary()) {
                    try {
                        CommitPoint commitPoint = cachedCommitPoints.get(shard.shardId());
                        if (commitPoint == null) {
                            commitPoint = ((BlobStoreGateway) ((InternalNode) this.node).injector().getInstance(Gateway.class)).findCommitPoint(shard.index(), shard.id());
                            if (commitPoint != null) {
                                cachedCommitPoints.put(shard.shardId(), commitPoint);
                            } else {
                                cachedCommitPoints.put(shard.shardId(), CommitPoint.NULL);
                            }
                        } else if (commitPoint == CommitPoint.NULL) {
                            commitPoint = null;
                        }

                        if (commitPoint == null) {
                            break;
                        }

                        long sizeMatched = 0;
                        for (StoreFileMetaData storeFileMetaData : storeFilesMetaData) {
                            CommitPoint.FileInfo fileInfo = commitPoint.findPhysicalIndexFile(storeFileMetaData.name());
                            if (fileInfo != null) {
                                if (fileInfo.isSame(storeFileMetaData)) {
                                    logger.trace("{}: [{}] reusing file since it exists on remote node and on gateway", shard, storeFileMetaData.name());
                                    sizeMatched += storeFileMetaData.length();
                                } else {
                                    logger.trace("{}: [{}] ignore file since it exists on remote node and on gateway but is different", shard, storeFileMetaData.name());
                                }
                            } else {
                                logger.trace("{}: [{}] exists on remote node, does not exists on gateway", shard, storeFileMetaData.name());
                            }
                        }
                        if (sizeMatched > lastSizeMatched) {
                            lastSizeMatched = sizeMatched;
                            lastDiscoNodeMatched = discoNode;
                            lastNodeMatched = node;
                            logger.trace("{}: node elected for pre_allocation [{}], total_size_matched [{}]", shard, discoNode, new ByteSizeValue(sizeMatched));
                        } else {
                            logger.trace("{}: node ignored for pre_allocation [{}], total_size_matched [{}] smaller than last_size_matched [{}]", shard, discoNode, new ByteSizeValue(sizeMatched), new ByteSizeValue(lastSizeMatched));
                        }
                    } catch (Exception e) {
                        // failed, log and try and allocate based on size
                        logger.debug("Failed to guess allocation of primary based on gateway for " + shard, e);
                    }
                } else {
                    // if its backup, see if there is a primary that *is* allocated, and try and assign a location that is closest to it
                    // note, since we replicate operations, this might not be the same (different flush intervals)
                    MutableShardRouting primaryShard = routingNodes.findPrimaryForReplica(shard);
                    if (primaryShard != null && primaryShard.active()) {
                        DiscoveryNode primaryNode = nodes.get(primaryShard.currentNodeId());
                        if (primaryNode != null) {
                            TransportNodesListShardStoreMetaData.StoreFilesMetaData primaryNodeStore = shardStores.get(primaryNode);
                            if (primaryNodeStore != null && primaryNodeStore.allocated()) {
                                long sizeMatched = 0;

                                for (StoreFileMetaData storeFileMetaData : storeFilesMetaData) {
                                    if (primaryNodeStore.fileExists(storeFileMetaData.name()) && primaryNodeStore.file(storeFileMetaData.name()).isSame(storeFileMetaData)) {
                                        sizeMatched += storeFileMetaData.length();
                                    }
                                }
                                if (sizeMatched > lastSizeMatched) {
                                    lastSizeMatched = sizeMatched;
                                    lastDiscoNodeMatched = discoNode;
                                    lastNodeMatched = node;
                                }
                            }
                        }
                    }
                }
            }

            if (lastNodeMatched != null) {
                if (nodeAllocations.canAllocate(shard, lastNodeMatched, allocation) == NodeAllocation.Decision.THROTTLE) {
                    if (logger.isTraceEnabled()) {
                        logger.debug("[{}][{}]: throttling allocation [{}] to [{}] in order to reuse its unallocated persistent store with total_size [{}]", shard.index(), shard.id(), shard, lastDiscoNodeMatched, new ByteSizeValue(lastSizeMatched));
                    }
                    // we are throttling this, but we have enough to allocate to this node, ignore it for now
                    unassignedIterator.remove();
                    routingNodes.ignoredUnassigned().add(shard);
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}][{}]: allocating [{}] to [{}] in order to reuse its unallocated persistent store with total_size [{}]", shard.index(), shard.id(), shard, lastDiscoNodeMatched, new ByteSizeValue(lastSizeMatched));
                    }
                    // we found a match
                    changed = true;
                    lastNodeMatched.add(shard);
                    unassignedIterator.remove();
                }
            }
        }

        return changed;
    }

    private ConcurrentMap<DiscoveryNode, TransportNodesListShardStoreMetaData.StoreFilesMetaData> buildShardStores(DiscoveryNodes nodes, MutableShardRouting shard) {
        ConcurrentMap<DiscoveryNode, TransportNodesListShardStoreMetaData.StoreFilesMetaData> shardStores = cachedStores.get(shard.shardId());
        if (shardStores == null) {
            shardStores = ConcurrentCollections.newConcurrentMap();
            TransportNodesListShardStoreMetaData.NodesStoreFilesMetaData nodesStoreFilesMetaData = transportNodesListShardStoreMetaData.list(shard.shardId(), false, nodes.dataNodes().keySet(), listTimeout).actionGet();
            if (logger.isDebugEnabled()) {
                if (nodesStoreFilesMetaData.failures().length > 0) {
                    StringBuilder sb = new StringBuilder(shard + ": failures when trying to list stores on nodes:");
                    for (int i = 0; i < nodesStoreFilesMetaData.failures().length; i++) {
                        Throwable cause = ExceptionsHelper.unwrapCause(nodesStoreFilesMetaData.failures()[i]);
                        if (cause instanceof ConnectTransportException) {
                            continue;
                        }
                        sb.append("\n    -> ").append(nodesStoreFilesMetaData.failures()[i].getDetailedMessage());
                    }
                    logger.debug(sb.toString());
                }
            }

            for (TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData nodeStoreFilesMetaData : nodesStoreFilesMetaData) {
                if (nodeStoreFilesMetaData.storeFilesMetaData() != null) {
                    shardStores.put(nodeStoreFilesMetaData.node(), nodeStoreFilesMetaData.storeFilesMetaData());
                }
            }
            cachedStores.put(shard.shardId(), shardStores);
        } else {
            // clean nodes that have failed
            for (DiscoveryNode node : shardStores.keySet()) {
                if (!nodes.nodeExists(node.id())) {
                    shardStores.remove(node);
                }
            }

            // we have stored cached from before, see if the nodes changed, if they have, go fetch again
            Set<String> fetchedNodes = Sets.newHashSet();
            for (DiscoveryNode node : nodes.dataNodes().values()) {
                if (!shardStores.containsKey(node)) {
                    fetchedNodes.add(node.id());
                }
            }

            if (!fetchedNodes.isEmpty()) {
                TransportNodesListShardStoreMetaData.NodesStoreFilesMetaData nodesStoreFilesMetaData = transportNodesListShardStoreMetaData.list(shard.shardId(), false, fetchedNodes, listTimeout).actionGet();
                if (logger.isDebugEnabled()) {
                    if (nodesStoreFilesMetaData.failures().length > 0) {
                        StringBuilder sb = new StringBuilder(shard + ": failures when trying to list stores on nodes:");
                        for (int i = 0; i < nodesStoreFilesMetaData.failures().length; i++) {
                            Throwable cause = ExceptionsHelper.unwrapCause(nodesStoreFilesMetaData.failures()[i]);
                            if (cause instanceof ConnectTransportException) {
                                continue;
                            }
                            sb.append("\n    -> ").append(nodesStoreFilesMetaData.failures()[i].getDetailedMessage());
                        }
                        logger.debug(sb.toString());
                    }
                }

                for (TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData nodeStoreFilesMetaData : nodesStoreFilesMetaData) {
                    if (nodeStoreFilesMetaData.storeFilesMetaData() != null) {
                        shardStores.put(nodeStoreFilesMetaData.node(), nodeStoreFilesMetaData.storeFilesMetaData());
                    }
                }
            }
        }
        return shardStores;
    }
}
