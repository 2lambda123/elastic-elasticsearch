/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.resolve;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.RemoteClusterService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class ResolveClusterActionRequest extends ActionRequest implements IndicesRequest.Replaceable {

    // only allow querying against open, non-hidden indices
    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandOpen();

    private String[] names;
    // tracks whether the user originally requested any local indices
    // this info can be lost when the indices(String... indices) method is called
    // to overwrite the indices array - it can remove local indices when none match
    private boolean localIndicesRequested = false;
    private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;

    public ResolveClusterActionRequest(String[] names) {
        this.names = names;
        this.localIndicesRequested = localIndicesPresent(names);
    }

    public ResolveClusterActionRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().before(TransportVersions.RESOLVE_CLUSTER_ENDPOINT_ADDED)) {
            throw new UnsupportedOperationException(
                "ResolveClusterAction requires at least Transport Version "
                    + TransportVersions.RESOLVE_CLUSTER_ENDPOINT_ADDED
                    + " but was "
                    + in.getTransportVersion()
            );
        }
        this.names = in.readStringArray();
        this.localIndicesRequested = localIndicesPresent(names);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().before(TransportVersions.RESOLVE_CLUSTER_ENDPOINT_ADDED)) {
            throw new UnsupportedOperationException(
                "ResolveClusterAction requires at least Transport Version "
                    + TransportVersions.RESOLVE_CLUSTER_ENDPOINT_ADDED
                    + " but was "
                    + out.getTransportVersion()
            );
        }
        out.writeStringArray(names);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResolveClusterActionRequest request = (ResolveClusterActionRequest) o;
        return Arrays.equals(names, request.names);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(names);
    }

    @Override
    public String[] indices() {
        return names;
    }

    public boolean isLocalIndicesRequested() {
        return localIndicesRequested;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public IndicesRequest indices(String... indices) {
        this.names = indices;
        return this;
    }

    @Override
    public boolean allowsRemoteIndices() {
        return true;
    }

    @Override
    public boolean includeDataStreams() {
        // request must allow data streams because the index name expression resolver for the action handler assumes it
        return true;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers) {
            @Override
            public String getDescription() {
                return "resolve/cluster for " + Arrays.toString(indices());
            }
        };
    }

    // MP TODO: add test for this
    private boolean localIndicesPresent(String[] indices) {
        for (String index : indices) {
            // ensure that `index` is a remote name and not a date math expression which includes ':' symbol
            // since date math expression after evaluation should not contain ':' symbol
            String indexExpression = IndexNameExpressionResolver.resolveDateMathExpression(index);
            if (indexExpression.indexOf(RemoteClusterService.REMOTE_CLUSTER_INDEX_SEPARATOR) < 0) {
                return true;
            }
        }
        return false;
    }
}
