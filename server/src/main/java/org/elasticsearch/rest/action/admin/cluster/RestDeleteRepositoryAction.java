/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ToXContentObject;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.client.Requests.deleteRepositoryRequest;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 * Unregisters a repository
 */
public class RestDeleteRepositoryAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_snapshot/{repository}"));
    }

    @Override
    public String getName() {
        return "delete_repository_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        DeleteRepositoryRequest deleteRepositoryRequest = deleteRepositoryRequest(request.param("repository"));
        deleteRepositoryRequest.timeout(request.paramAsTime("timeout", deleteRepositoryRequest.timeout()));
        deleteRepositoryRequest.masterNodeTimeout(request.paramAsTime("master_timeout", deleteRepositoryRequest.masterNodeTimeout()));
        return channel -> {
            final RestToXContentListener<ToXContentObject> restListener = new RestToXContentListener<>(channel);
            client.admin().cluster().deleteRepository(deleteRepositoryRequest, new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    restListener.onResponse(acknowledgedResponse);
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
