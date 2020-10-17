/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.logstash.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.logstash.Logstash;

import static org.elasticsearch.xpack.core.ClientHelper.LOGSTASH_MANAGEMENT_ORIGIN;

public class TransportDeletePipelineAction extends HandledTransportAction<DeletePipelineRequest, DeletePipelineResponse> {

    private final Client client;

    @Inject
    public TransportDeletePipelineAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(DeletePipelineAction.NAME, transportService, actionFilters, DeletePipelineRequest::new);
        this.client = new OriginSettingClient(client, LOGSTASH_MANAGEMENT_ORIGIN);
    }

    @Override
    protected void doExecute(Task task, DeletePipelineRequest request, ActionListener<DeletePipelineResponse> listener) {
        client.prepareDelete()
            .setIndex(Logstash.LOGSTASH_CONCRETE_INDEX_NAME)
            .setId(request.id())
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute(
                ActionListener.wrap(
                    deleteResponse -> listener.onResponse(new DeletePipelineResponse(deleteResponse.getResult() == Result.DELETED)),
                    e -> handleFailure(e, listener)
                )
            );
    }

    private void handleFailure(Exception e, ActionListener<DeletePipelineResponse> listener) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        if (cause instanceof IndexNotFoundException) {
            listener.onResponse(new DeletePipelineResponse(false));
        } else {
            listener.onFailure(e);
        }
    }
}
