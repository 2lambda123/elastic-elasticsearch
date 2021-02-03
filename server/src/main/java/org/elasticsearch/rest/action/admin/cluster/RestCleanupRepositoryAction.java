/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.client.Requests.cleanupRepositoryRequest;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Cleans up a repository
 */
public class RestCleanupRepositoryAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_snapshot/{repository}/_cleanup"));
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
        return channel -> client.admin().cluster().cleanupRepository(cleanupRepositoryRequest, new RestToXContentListener<>(channel));
    }
}
