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

package org.elasticsearch.rest.action.admin.cluster.dangling;

import org.elasticsearch.action.admin.indices.dangling.RestoreDanglingIndexRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestRestoreDanglingIndexAction extends BaseRestHandler {
    public RestRestoreDanglingIndexAction(RestController controller) {
        controller.registerHandler(POST, "/_dangling/{indexUuid}", this);
    }

    @Override
    public String getName() {
        return "restore_dangling_index";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, NodeClient client) throws IOException {
        final RestoreDanglingIndexRequest restoreRequest = new RestoreDanglingIndexRequest();
        restoreRequest.setIndexUuid(request.param("indexUuid"));
        request.applyContentParser(p -> restoreRequest.source(p.mapOrdered()));

        return channel -> client.admin().cluster().restoreDanglingIndex(restoreRequest, new RestStatusToXContentListener<>(channel));
    }
}
