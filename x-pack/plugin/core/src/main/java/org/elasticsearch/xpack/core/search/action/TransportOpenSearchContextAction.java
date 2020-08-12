/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class TransportOpenSearchContextAction extends HandledTransportAction<OpenSearchContextRequest, OpenSearchContextResponse> {
    public static final String OPEN_SHARD_READER_CONTEXT_NAME = "indices:data/read/open_reader_context";

    private final TransportSearchAction transportSearchAction;
    private final TransportService transportService;
    private final SearchService searchService;

    @Inject
    public TransportOpenSearchContextAction(
        TransportService transportService,
        SearchService searchService,
        ActionFilters actionFilters,
        TransportSearchAction transportSearchAction
    ) {
        super(OpenSearchContextAction.NAME, transportService, actionFilters, OpenSearchContextRequest::new);
        this.transportService = transportService;
        this.transportSearchAction = transportSearchAction;
        this.searchService = searchService;
        transportService.registerRequestHandler(
            OPEN_SHARD_READER_CONTEXT_NAME,
            ThreadPool.Names.SAME,
            ShardOpenReaderRequest::new,
            new ShardOpenReaderRequestHandler()
        );
        TransportActionProxy.registerProxyAction(
            transportService,
            OPEN_SHARD_READER_CONTEXT_NAME,
            TransportOpenSearchContextAction.ShardOpenReaderResponse::new
        );
    }

    @Override
    protected void doExecute(Task task, OpenSearchContextRequest request, ActionListener<OpenSearchContextResponse> listener) {
        final SearchRequest searchRequest = new SearchRequest().indices(request.indices())
            .indicesOptions(request.indicesOptions())
            .preference(request.preference())
            .routing(request.routing())
            .allowPartialSearchResults(false);
        transportSearchAction.executeRequest(
            task,
            searchRequest,
            "open_search_context",
            true,
            (searchTask, shardTarget, connection, phaseListener) -> {
                final ShardOpenReaderRequest shardRequest = new ShardOpenReaderRequest(
                    shardTarget.getShardId(),
                    shardTarget.getOriginalIndices(),
                    request.keepAlive()
                );
                transportService.sendChildRequest(
                    connection,
                    OPEN_SHARD_READER_CONTEXT_NAME,
                    shardRequest,
                    searchTask,
                    new ActionListenerResponseHandler<SearchPhaseResult>(phaseListener, ShardOpenReaderResponse::new)
                );
            },
            ActionListener.map(listener, r -> new OpenSearchContextResponse(r.searchContextId()))
        );
    }

    private static final class ShardOpenReaderRequest extends TransportRequest implements IndicesRequest {
        final ShardId shardId;
        final OriginalIndices originalIndices;
        final TimeValue keepAlive;

        ShardOpenReaderRequest(ShardId shardId, OriginalIndices originalIndices, TimeValue keepAlive) {
            this.shardId = shardId;
            this.originalIndices = originalIndices;
            this.keepAlive = keepAlive;
        }

        ShardOpenReaderRequest(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            originalIndices = OriginalIndices.readOriginalIndices(in);
            keepAlive = in.readTimeValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            OriginalIndices.writeOriginalIndices(originalIndices, out);
            out.writeTimeValue(keepAlive);
        }

        public ShardId getShardId() {
            return shardId;
        }

        @Override
        public String[] indices() {
            return originalIndices.indices();
        }

        @Override
        public IndicesOptions indicesOptions() {
            return originalIndices.indicesOptions();
        }
    }

    private static final class ShardOpenReaderResponse extends SearchPhaseResult {
        ShardOpenReaderResponse(ShardSearchContextId contextId) {
            this.contextId = contextId;
        }

        ShardOpenReaderResponse(StreamInput in) throws IOException {
            super(in);
            contextId = new ShardSearchContextId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            contextId.writeTo(out);
        }
    }

    private class ShardOpenReaderRequestHandler implements TransportRequestHandler<ShardOpenReaderRequest> {
        @Override
        public void messageReceived(ShardOpenReaderRequest request, TransportChannel channel, Task task) throws Exception {
            searchService.openReaderContext(
                request.getShardId(),
                request.keepAlive,
                ActionListener.map(
                    new ChannelActionListener<>(channel, OPEN_SHARD_READER_CONTEXT_NAME, request),
                    ShardOpenReaderResponse::new
                )
            );
        }
    }
}
