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

package org.elasticsearch.action.admin.indices.dangling;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.List;

/**
 * Models a response to a {@link ListDanglingIndicesRequest}. A list request queries every node in the
 * cluster and aggregates their responses. When the aggregated response is converted to {@link XContent},
 * information for each dangling index is presented under the "dangling_indices" key. If any nodes
 * in the cluster failed to answer, the details are presented under the "failed_nodes" key.
 */
public class ListDanglingIndicesResponse extends BaseNodesResponse<NodeDanglingIndicesResponse> implements StatusToXContentObject {

    public ListDanglingIndicesResponse(StreamInput in) throws IOException {
        super(in);
    }

    public ListDanglingIndicesResponse(
        ClusterName clusterName,
        List<NodeDanglingIndicesResponse> nodes,
        List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    @Override
    public RestStatus status() {
        return this.hasFailures() ? RestStatus.SERVICE_UNAVAILABLE : RestStatus.OK;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.startArray("dangling_indices");
        for (NodeDanglingIndicesResponse nodeResponse : this.getNodes()) {
            for (IndexMetaData indexMetaData : nodeResponse.getDanglingIndices()) {
                DanglingIndexInfo danglingIndexInfo = new DanglingIndexInfo(
                    nodeResponse.getNode(),
                    indexMetaData.getIndex().getName(),
                    indexMetaData.getIndexUUID(),
                    indexMetaData.getCreationDate()
                );
                danglingIndexInfo.toXContent(builder, params);
            }
        }
        builder.endArray();

        if (this.hasFailures()) {
            builder.startArray("failed_nodes");
            for (FailedNodeException failure : this.failures()) {
                failure.toXContent(builder, params);
            }
            builder.endArray();
        }

        return builder.endObject();
    }

    @Override
    protected List<NodeDanglingIndicesResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodeDanglingIndicesResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeDanglingIndicesResponse> nodes) throws IOException {
        out.writeList(nodes);
    }
}
