/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetMlAutoscalingStats;
import org.elasticsearch.xpack.core.ml.action.GetMlAutoscalingStats.Request;
import org.elasticsearch.xpack.core.ml.action.GetMlAutoscalingStats.Response;
import org.elasticsearch.xpack.ml.autoscaling.MlAutoscalingResourceTracker;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

public class TransportGetMlAutoscalingStats extends TransportMasterNodeAction<Request, Response> {

    private final Client client;
    private final MlMemoryTracker mlMemoryTracker;

    @Inject
    public TransportGetMlAutoscalingStats(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client,
        MlMemoryTracker mlMemoryTracker
    ) {
        super(
            GetMlAutoscalingStats.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            indexNameExpressionResolver,
            Response::new,
            ThreadPool.Names.SAME
        );
        this.client = client;
        this.mlMemoryTracker = mlMemoryTracker;
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
        TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        ParentTaskAssigningClient parentTaskAssigningClient = new ParentTaskAssigningClient(client, parentTaskId);

        if (mlMemoryTracker.isRecentlyRefreshed()) {
            MlAutoscalingResourceTracker.getMlAutoscalingStats(
                state,
                parentTaskAssigningClient,
                request.timeout(),
                mlMemoryTracker,
                ActionListener.wrap(autoscalingResources -> listener.onResponse(new Response(autoscalingResources)), listener::onFailure)
            );
        } else {
            mlMemoryTracker.refresh(
                state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE),
                ActionListener.wrap(
                    ignored -> MlAutoscalingResourceTracker.getMlAutoscalingStats(
                        state,
                        parentTaskAssigningClient,
                        request.timeout(),
                        mlMemoryTracker,
                        ActionListener.wrap(
                            autoscalingResources -> listener.onResponse(new Response(autoscalingResources)),
                            listener::onFailure
                        )
                    ),
                    listener::onFailure
                )
            );
        }
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
