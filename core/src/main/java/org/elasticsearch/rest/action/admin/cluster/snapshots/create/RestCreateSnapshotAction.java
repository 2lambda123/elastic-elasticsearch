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

package org.elasticsearch.rest.action.admin.cluster.snapshots.create;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.BaseMultiMethodRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestGlobalContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.action.support.RestToXContentListener;

import static org.elasticsearch.client.Requests.createSnapshotRequest;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * Creates a new snapshot
 */
public class RestCreateSnapshotAction extends BaseMultiMethodRestHandler {
    public RestCreateSnapshotAction(RestGlobalContext context) {
        super(context, new Method[] {PUT, POST}, "/_snapshot/{repository}/{snapshot}");
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        CreateSnapshotRequest createSnapshotRequest = createSnapshotRequest(request.param("repository"), request.param("snapshot"));
        createSnapshotRequest.source(request.content().toUtf8());
        createSnapshotRequest.masterNodeTimeout(request.paramAsTime("master_timeout", createSnapshotRequest.masterNodeTimeout()));
        createSnapshotRequest.waitForCompletion(request.paramAsBoolean("wait_for_completion", false));
        client.admin().cluster().createSnapshot(createSnapshotRequest, new RestToXContentListener<CreateSnapshotResponse>(channel));
    }
}
