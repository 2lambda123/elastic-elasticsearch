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

package org.elasticsearch.action.admin.cluster.ping.replication;

import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.support.replication.TransportIndicesReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 */
public class TransportReplicationPingAction extends TransportIndicesReplicationOperationAction<ReplicationPingRequest, ReplicationPingResponse, IndexReplicationPingRequest, IndexReplicationPingResponse, ShardReplicationPingRequest, ShardReplicationPingRequest, ShardReplicationPingResponse> {

    @Inject
    public TransportReplicationPingAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool, TransportIndexReplicationPingAction indexAction) {
        super(settings, transportService, clusterService, threadPool, indexAction);
    }

    @Override
    protected ReplicationPingRequest newRequestInstance() {
        return new ReplicationPingRequest();
    }

    @Override
    protected ReplicationPingResponse newResponseInstance(ReplicationPingRequest request, AtomicReferenceArray indexResponses) {
        ReplicationPingResponse response = new ReplicationPingResponse();
        for (int i = 0; i < indexResponses.length(); i++) {
            IndexReplicationPingResponse indexResponse = (IndexReplicationPingResponse) indexResponses.get(i);
            if (indexResponse != null) {
                response.indices().put(indexResponse.index(), indexResponse);
            }
        }
        return response;
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }

    @Override
    protected String transportAction() {
        return TransportActions.Admin.Cluster.Ping.REPLICATION;
    }

    @Override
    protected IndexReplicationPingRequest newIndexRequestInstance(ReplicationPingRequest request, String index, Set<String> routing) {
        return new IndexReplicationPingRequest(request, index);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, ReplicationPingRequest replicationPingRequest) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, ReplicationPingRequest replicationPingRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.WRITE, concreteIndices);
    }
}