/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ToXContentObject;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.client.Requests.cleanupRepositoryRequest;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Cleans up a repository
 */
public class RestCleanupRepositoryAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_snapshot/{repository}/_cleanup"));
    }

    @Override
    public String getName() {
        return "cleanup_repository_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        CleanupRepositoryRequest cleanupRepositoryRequest = cleanupRepositoryRequest(request.param("repository"));
        cleanupRepositoryRequest.timeout(request.paramAsTime("timeout", cleanupRepositoryRequest.timeout()));
        cleanupRepositoryRequest.masterNodeTimeout(request.paramAsTime("master_timeout", cleanupRepositoryRequest.masterNodeTimeout()));
        return channel -> {
            final RestToXContentListener<ToXContentObject> restListener = new RestToXContentListener<>(channel);
            client.admin().cluster().cleanupRepository(cleanupRepositoryRequest, new ActionListener<>() {
                @Override
                public void onResponse(CleanupRepositoryResponse cleanupRepositoryResponse) {
                    restListener.onResponse(cleanupRepositoryResponse);
                }

                @Override
                public void onFailure(Exception e) {
                    if (request.getRestApiVersion().equals(RestApiVersion.V_7)) {
                        restListener.onFailure(new IllegalStateException(e.getMessage()));
                    } else {
                        restListener.onFailure(e);
                    }
                }
            });
        };
    }
}
