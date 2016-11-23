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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Represents a decision to relocate a started shard from its current node.
 */
public abstract class RelocationDecision implements ToXContent, Writeable {
    @Nullable
    private final Decision.Type finalDecision;
    @Nullable
    private final String assignedNodeId;
    @Nullable
    private final String assignedNodeName;

    protected RelocationDecision(Decision.Type finalDecision, DiscoveryNode assignedNode) {
        this.finalDecision = finalDecision;
        if (assignedNode != null) {
            this.assignedNodeId = assignedNode.getId();
            this.assignedNodeName = assignedNode.getName();
        } else {
            this.assignedNodeId = null;
            this.assignedNodeName = null;
        }
    }

    public RelocationDecision(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            finalDecision = Decision.Type.readFrom(in);
        } else {
            finalDecision = null;
        }
        assignedNodeId = in.readOptionalString();
        assignedNodeName = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (finalDecision != null) {
            out.writeBoolean(true);
            finalDecision.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalString(assignedNodeId);
        out.writeOptionalString(assignedNodeName);
    }

    /**
     * Returns {@code true} if a decision was taken by the allocator, {@code false} otherwise.
     * If no decision was taken, then the rest of the fields in this object are meaningless and return {@code null}.
     */
    public boolean isDecisionTaken() {
        return finalDecision != null;
    }

    /**
     * Returns the final decision made by the allocator on whether to assign the shard, and
     * {@code null} if no decision was taken.
     */
    public Decision.Type getFinalDecisionType() {
        return finalDecision;
    }

    /**
     * Get the node id that the allocator will assign the shard to, unless {@link #getFinalDecisionType()} returns
     * a value other than {@link Decision.Type#YES}, in which case this returns {@code null}.
     */
    @Nullable
    public String getAssignedNodeId() {
        return assignedNodeId;
    }

    /**
     * Get the node name that the allocator will assign the shard to, unless {@link #getFinalDecisionType()} returns
     * a value other than {@link Decision.Type#YES}, in which case this returns {@code null}.
     */
    @Nullable
    public String getAssignedNodeName() {
        return assignedNodeName;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (finalDecision != null) {
            builder.field("final_decision", finalDecision);
        }
        builder.field("final_explanation", getFinalExplanation());
        if (assignedNodeId != null) {
            builder.startObject("assigned_node");
            builder.field("id", assignedNodeId);
            builder.field("name", assignedNodeName);
            builder.endObject();
        }
        return builder;
    }

    /**
     * Gets the final explanation for the decision taken.
     */
    public abstract String getFinalExplanation();
}
