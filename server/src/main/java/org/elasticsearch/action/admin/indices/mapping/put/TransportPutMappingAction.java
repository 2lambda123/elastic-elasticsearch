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

package org.elasticsearch.action.admin.indices.mapping.put;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;

/**
 * Put mapping action.
 */
public class TransportPutMappingAction extends TransportMasterNodeAction<PutMappingRequest, AcknowledgedResponse> {

    private final MetaDataMappingService metaDataMappingService;
    private final RequestValidators requestValidators;

    @Inject
    public TransportPutMappingAction(TransportService transportService, ClusterService clusterService,
                                     ThreadPool threadPool, MetaDataMappingService metaDataMappingService,
                                     ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                     RequestValidators requestValidators) {
        super(PutMappingAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
            PutMappingRequest::new);
        this.metaDataMappingService = metaDataMappingService;
        this.requestValidators = requestValidators;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(PutMappingRequest request, ClusterState state) {
        String[] indices;
        if (request.getConcreteIndex() == null) {
            indices = indexNameExpressionResolver.concreteIndexNames(state, request);
        } else {
            indices = new String[] {request.getConcreteIndex().getName()};
        }
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indices);
    }

    @Override
    protected void masterOperation(final PutMappingRequest request, final ClusterState state,
                                   final ActionListener<AcknowledgedResponse> listener) {
        try {
            final Index[] concreteIndices = request.getConcreteIndex() == null ?
                indexNameExpressionResolver.concreteIndices(state, request)
                : new Index[] {request.getConcreteIndex()};
            final Exception validationException = requestValidators.validateRequest(request, state, concreteIndices);
            if (validationException != null) {
                listener.onFailure(validationException);
                return;
            }
            PutMappingClusterStateUpdateRequest updateRequest = new PutMappingClusterStateUpdateRequest()
                    .ackTimeout(request.timeout()).masterNodeTimeout(request.masterNodeTimeout())
                    .indices(concreteIndices).type(request.type())
                    .source(request.source());

            metaDataMappingService.putMapping(updateRequest, new ActionListener<ClusterStateUpdateResponse>() {

                @Override
                public void onResponse(ClusterStateUpdateResponse response) {
                    listener.onResponse(new AcknowledgedResponse(response.isAcknowledged()));
                }

                @Override
                public void onFailure(Exception t) {
                    logger.debug(() -> new ParameterizedMessage("failed to put mappings on indices [{}], type [{}]",
                        concreteIndices, request.type()), t);
                    listener.onFailure(t);
                }
            });
        } catch (IndexNotFoundException ex) {
            logger.debug(() -> new ParameterizedMessage("failed to put mappings on indices [{}], type [{}]",
                request.indices(), request.type()), ex);
            throw ex;
        }
    }


    public static class RequestValidators {
        private final Collection<MappingRequestValidator> validators;

        public RequestValidators(Collection<MappingRequestValidator> validators) {
            this.validators = validators;
        }

        Exception validateRequest(PutMappingRequest request, ClusterState state, Index[] indices) {
            Exception firstException = null;
            for (MappingRequestValidator validator : validators) {
                final Exception e = validator.validateRequest(request, state, indices);
                if (e == null) continue;
                if (firstException == null) {
                    firstException = e;
                } else {
                    firstException.addSuppressed(e);
                }
            }
            return firstException;
        }
    }
}
