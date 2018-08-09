/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.indexlifecycle.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.protocol.xpack.indexlifecycle.OperationMode;
import org.elasticsearch.protocol.xpack.indexlifecycle.StartILMRequest;
import org.elasticsearch.protocol.xpack.indexlifecycle.StartILMResponse;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.indexlifecycle.action.StartILMAction;
import org.elasticsearch.xpack.indexlifecycle.OperationModeUpdateTask;

public class TransportStartILMAction extends TransportMasterNodeAction<StartILMRequest, StartILMResponse> {

    @Inject
    public TransportStartILMAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                           ThreadPool threadPool, ActionFilters actionFilters,
                                           IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, StartILMAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                StartILMRequest::new);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected StartILMResponse newResponse() {
        return new StartILMResponse();
    }

    @Override
    protected void masterOperation(StartILMRequest request, ClusterState state, ActionListener<StartILMResponse> listener) {
        clusterService.submitStateUpdateTask("ilm_operation_mode_update",
                new AckedClusterStateUpdateTask<StartILMResponse>(request, listener) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                        return (new OperationModeUpdateTask(OperationMode.RUNNING)).execute(currentState);
                }

                @Override
                    protected StartILMResponse newResponse(boolean acknowledged) {
                        return new StartILMResponse(acknowledged);
                }
            });
    }

    @Override
    protected ClusterBlockException checkBlock(StartILMRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
