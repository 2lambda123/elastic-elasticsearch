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


package org.elasticsearch.index.reindex;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;

public class RestGetReindexTaskAction extends BaseRestHandler {

    public RestGetReindexTaskAction(final RestController controller) {
        controller.registerHandler(RestRequest.Method.GET, "/_reindex/{task_id}", this);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String taskId = restRequest.param("task_id");
        GetReindexTaskAction.Request request = new GetReindexTaskAction.Request(taskId);
        return channel -> client.execute(GetReindexTaskAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
