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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Expose reindex over rest.
 */
public class RestReindexAction extends AbstractBaseReindexRestHandler<ReindexRequest, ReindexAction> {

    public RestReindexAction(Settings settings, RestController controller) {
        super(settings, ReindexAction.INSTANCE);
        controller.registerHandler(POST, "/_reindex", this);
    }

    @Override
    public String getName() {
        return "reindex_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Build the internal request
        StartReindexJobAction.Request internal = new StartReindexJobAction.Request(setCommonOptions(request, buildRequest(request)));

        // Executes the request and waits for completion
        if (request.paramAsBoolean("wait_for_completion", true)) {
            Map<String, String> params = new HashMap<>();
            params.put(BulkByScrollTask.Status.INCLUDE_CREATED, Boolean.toString(true));
            params.put(BulkByScrollTask.Status.INCLUDE_UPDATED, Boolean.toString(true));

            internal.setWaitForCompletion(true);
            return channel -> client.execute(StartReindexJobAction.INSTANCE, internal, new ActionListener<>() {

                private BulkIndexByScrollResponseContentListener listener = new BulkIndexByScrollResponseContentListener(channel, params);

                @Override
                public void onResponse(StartReindexJobAction.Response response) {
                    listener.onResponse(response.getReindexResponse());
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } else {
            internal.getReindexRequest().setShouldStoreResult(true);
            internal.setWaitForCompletion(false);

            /*
             * Let's try and validate before forking so the user gets some error. The
             * task can't totally validate until it starts but this is better than
             * nothing.
             */
            ActionRequestValidationException validationException = internal.getReindexRequest().validate();
            if (validationException != null) {
                throw validationException;
            }

            return channel -> client.execute(StartReindexJobAction.INSTANCE, internal, new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(StartReindexJobAction.Response response, XContentBuilder builder) throws Exception {
                    builder.startObject();
                    builder.field("task", response.getTaskId());
                    builder.endObject();

                    // TODO: Are there error conditions for the non-wait case?
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            });
        }

    }

    @Override
    protected ReindexRequest buildRequest(RestRequest request) throws IOException {
        if (request.hasParam("pipeline")) {
            throw new IllegalArgumentException("_reindex doesn't support [pipeline] as a query parameter. "
                    + "Specify it in the [dest] object instead.");
        }

        ReindexRequest internal;
        try (XContentParser parser = request.contentParser()) {
            internal = ReindexRequest.fromXContent(parser);
        }

        if (request.hasParam("scroll")) {
            internal.setScroll(parseTimeValue(request.param("scroll"), "scroll"));
        }
        return internal;
    }
}
