/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

public final class RoleMappingMetadata extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

    public static final String TYPE = "role_mappings";

    private static final RoleMappingMetadata EMPTY = new RoleMappingMetadata(Set.of());

    public static RoleMappingMetadata getFromClusterState(ClusterState clusterState) {
        return clusterState.custom(RoleMappingMetadata.TYPE, RoleMappingMetadata.EMPTY);
    }

    private final Set<ExpressionRoleMapping> roleMappings;

    public RoleMappingMetadata(Set<ExpressionRoleMapping> roleMappings) {
        this.roleMappings = roleMappings;
    }

    public RoleMappingMetadata(StreamInput input) throws IOException {
        this.roleMappings = input.readCollectionAsSet(ExpressionRoleMapping::new);
    }

    public Set<ExpressionRoleMapping> getRoleMappings() {
        return this.roleMappings;
    }

    public boolean isEmpty() {
        return roleMappings.isEmpty();
    }

    public ClusterState updateClusterState(ClusterState clusterState) {
        if (isEmpty()) {
            // prefer no role mapping custom metadata to the empty role mapping metadata
            return clusterState.copyAndUpdate(b -> b.removeCustom(RoleMappingMetadata.TYPE));
        } else {
            return clusterState.copyAndUpdate(b -> b.putCustom(RoleMappingMetadata.TYPE, this));
        }
    }

    public static NamedDiff<ClusterState.Custom> readDiffFrom(StreamInput streamInput) throws IOException {
        return readDiffFrom(ClusterState.Custom.class, TYPE, streamInput);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(ChunkedToXContentHelper.startArray(TYPE), roleMappings.iterator(), ChunkedToXContentHelper.endArray());
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.SECURITY_ROLE_MAPPINGS_IN_CLUSTER_STATE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(roleMappings);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final var other = (RoleMappingMetadata) o;
        return Objects.equals(roleMappings, other.roleMappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(roleMappings);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("RoleMapping[entries=[");
        final Iterator<ExpressionRoleMapping> entryList = roleMappings.iterator();
        boolean firstEntry = true;
        while (entryList.hasNext()) {
            if (firstEntry == false) {
                builder.append(",");
            }
            builder.append(entryList.next().toString());
            firstEntry = false;
        }
        return builder.append("]]").toString();
    }
}
