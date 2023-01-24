/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.slm.action.GetSLMStatusAction;

import static org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata.currentSLMMode;

public class TransportGetSLMStatusAction extends TransportMasterNodeAction<GetSLMStatusAction.Request, GetSLMStatusAction.Response> {

    @Inject
    public TransportGetSLMStatusAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetSLMStatusAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetSLMStatusAction.Request::new,
            indexNameExpressionResolver,
            GetSLMStatusAction.Response::new,
            ThreadPool.Names.SAME
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        GetSLMStatusAction.Request request,
        ClusterState state,
        ActionListener<GetSLMStatusAction.Response> listener
    ) {
        listener.onResponse(new GetSLMStatusAction.Response(currentSLMMode(state)));
    }

    @Override
    protected ClusterBlockException checkBlock(GetSLMStatusAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
