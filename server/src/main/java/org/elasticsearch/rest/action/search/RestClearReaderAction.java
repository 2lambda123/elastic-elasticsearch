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

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.search.ClearReaderAction;
import org.elasticsearch.action.search.ClearReaderRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestClearReaderAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_search/reader"));
    }

    @Override
    public String getName() {
        return "clear_reader_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final ClearReaderRequest clearRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            clearRequest = ClearReaderRequest.fromXContent(parser);
        }
        return channel -> client.execute(ClearReaderAction.INSTANCE, clearRequest, new RestStatusToXContentListener<>(channel));
    }
}
