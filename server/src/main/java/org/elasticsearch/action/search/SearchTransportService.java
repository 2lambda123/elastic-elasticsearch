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

package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ScrollQueryFetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchRequest;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.SearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.ScrollQuerySearchResult;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

import static org.elasticsearch.search.SearchService.SEND_CONTEXT_HEARTBEAT_INTERVAL_SETTING;

/**
 * An encapsulation of {@link org.elasticsearch.search.SearchService} operations exposed through
 * transport.
 */
public class SearchTransportService {

    public static final String FREE_CONTEXT_SCROLL_ACTION_NAME = "indices:data/read/search[free_context/scroll]";
    public static final String FREE_CONTEXT_ACTION_NAME = "indices:data/read/search[free_context]";
    public static final String CLEAR_SCROLL_CONTEXTS_ACTION_NAME = "indices:data/read/search[clear_scroll_contexts]";
    public static final String DFS_ACTION_NAME = "indices:data/read/search[phase/dfs]";
    public static final String QUERY_ACTION_NAME = "indices:data/read/search[phase/query]";
    public static final String QUERY_ID_ACTION_NAME = "indices:data/read/search[phase/query/id]";
    public static final String QUERY_SCROLL_ACTION_NAME = "indices:data/read/search[phase/query/scroll]";
    public static final String QUERY_FETCH_SCROLL_ACTION_NAME = "indices:data/read/search[phase/query+fetch/scroll]";
    public static final String FETCH_ID_SCROLL_ACTION_NAME = "indices:data/read/search[phase/fetch/id/scroll]";
    public static final String FETCH_ID_ACTION_NAME = "indices:data/read/search[phase/fetch/id]";
    public static final String QUERY_CAN_MATCH_NAME = "indices:data/read/search[can_match]";
    public static final String SEARCH_CONTEXT_HEARTBEAT_ACTION_NAME = "internal:admin/search/context_heartbeat";

    private static final Logger logger = LogManager.getLogger(SearchTransportService.class);

    private final TransportService transportService;
    private final BiFunction<Transport.Connection, SearchActionListener, ActionListener> responseWrapper;
    private final Map<String, Long> clientConnections = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
    private final SearchContextHeartbeatSender searchContextHeartbeatSender;

    public SearchTransportService(Settings settings, TransportService transportService,
                                  BiFunction<Transport.Connection, SearchActionListener, ActionListener> responseWrapper) {
        this.transportService = transportService;
        this.responseWrapper = responseWrapper;
        this.searchContextHeartbeatSender = new SearchContextHeartbeatSender(
            transportService, logger, transportService.getThreadPool(), SEND_CONTEXT_HEARTBEAT_INTERVAL_SETTING.get(settings));
    }

    public void sendFreeContext(Transport.Connection connection, final SearchContextId contextId, OriginalIndices originalIndices) {
        transportService.sendRequest(connection, FREE_CONTEXT_ACTION_NAME, new SearchFreeContextRequest(originalIndices, contextId),
            TransportRequestOptions.EMPTY, new ActionListenerResponseHandler<>(new ActionListener<SearchFreeContextResponse>() {
                @Override
                public void onResponse(SearchFreeContextResponse response) {
                    // no need to respond if it was freed or not
                }

                @Override
                public void onFailure(Exception e) {

                }
            }, SearchFreeContextResponse::new));
    }

    public void sendFreeContext(Transport.Connection connection, SearchContextId contextId,
                                ActionListener<SearchFreeContextResponse> listener) {
        transportService.sendRequest(connection, FREE_CONTEXT_SCROLL_ACTION_NAME, new ScrollFreeContextRequest(contextId),
            TransportRequestOptions.EMPTY, new ActionListenerResponseHandler<>(listener, SearchFreeContextResponse::new));
    }

    public void sendCanMatch(Transport.Connection connection, final ShardSearchRequest request, SearchTask task, final
                            ActionListener<SearchService.CanMatchResponse> listener) {
        transportService.sendChildRequest(connection, QUERY_CAN_MATCH_NAME, request, task,
            TransportRequestOptions.EMPTY, new ActionListenerResponseHandler<>(listener, SearchService.CanMatchResponse::new));
    }

    public void sendClearAllScrollContexts(Transport.Connection connection, final ActionListener<TransportResponse> listener) {
        transportService.sendRequest(connection, CLEAR_SCROLL_CONTEXTS_ACTION_NAME, TransportRequest.Empty.INSTANCE,
            TransportRequestOptions.EMPTY, new ActionListenerResponseHandler<>(listener, (in) -> TransportResponse.Empty.INSTANCE));
    }

    public void sendExecuteDfs(Transport.Connection connection, final ShardSearchRequest request, SearchTask task,
                               final SearchActionListener<DfsSearchResult> listener) {
        transportService.sendChildRequest(connection, DFS_ACTION_NAME, request, task,
                new ConnectionCountingHandler<>(listener, DfsSearchResult::new, clientConnections, connection.getNode().getId()));
    }

    public void sendExecuteQuery(Transport.Connection connection, final ShardSearchRequest request, SearchTask task,
                                 final SearchActionListener<SearchPhaseResult> listener) {
        // we optimize this and expect a QueryFetchSearchResult if we only have a single shard in the search request
        // this used to be the QUERY_AND_FETCH which doesn't exist anymore.
        final boolean fetchDocuments = request.numberOfShards() == 1;
        Writeable.Reader<SearchPhaseResult> reader = fetchDocuments ? QueryFetchSearchResult::new : QuerySearchResult::new;

        final ActionListener handler = responseWrapper.apply(connection, listener);
        transportService.sendChildRequest(connection, QUERY_ACTION_NAME, request, task,
                new ConnectionCountingHandler<>(handler, reader, clientConnections, connection.getNode().getId()));
    }

    public void sendExecuteQuery(Transport.Connection connection, final QuerySearchRequest request, SearchTask task,
                                 final SearchActionListener<QuerySearchResult> listener) {
        transportService.sendChildRequest(connection, QUERY_ID_ACTION_NAME, request, task,
                new ConnectionCountingHandler<>(listener, QuerySearchResult::new, clientConnections, connection.getNode().getId()));
    }

    public void sendExecuteScrollQuery(Transport.Connection connection, final InternalScrollSearchRequest request, SearchTask task,
                                       final SearchActionListener<ScrollQuerySearchResult> listener) {
        transportService.sendChildRequest(connection, QUERY_SCROLL_ACTION_NAME, request, task,
                new ConnectionCountingHandler<>(listener, ScrollQuerySearchResult::new, clientConnections, connection.getNode().getId()));
    }

    public void sendExecuteScrollFetch(Transport.Connection connection, final InternalScrollSearchRequest request, SearchTask task,
                                       final SearchActionListener<ScrollQueryFetchSearchResult> listener) {
        transportService.sendChildRequest(connection, QUERY_FETCH_SCROLL_ACTION_NAME, request, task,
                new ConnectionCountingHandler<>(listener, ScrollQueryFetchSearchResult::new, clientConnections,
                    connection.getNode().getId()));
    }

    public void sendExecuteFetch(Transport.Connection connection, final ShardFetchSearchRequest request, SearchTask task,
                                 final SearchActionListener<FetchSearchResult> listener) {
        sendExecuteFetch(connection, FETCH_ID_ACTION_NAME, request, task, listener);
    }

    public void sendExecuteFetchScroll(Transport.Connection connection, final ShardFetchRequest request, SearchTask task,
                                       final SearchActionListener<FetchSearchResult> listener) {
        sendExecuteFetch(connection, FETCH_ID_SCROLL_ACTION_NAME, request, task, listener);
    }

    private void sendExecuteFetch(Transport.Connection connection, String action, final ShardFetchRequest request, SearchTask task,
                                  final SearchActionListener<FetchSearchResult> listener) {
        transportService.sendChildRequest(connection, action, request, task,
                new ConnectionCountingHandler<>(listener, FetchSearchResult::new, clientConnections, connection.getNode().getId()));
    }

    /**
     * Used by {@link TransportSearchAction} to send the expand queries (field collapsing).
     */
    void sendExecuteMultiSearch(final MultiSearchRequest request, SearchTask task,
                                final ActionListener<MultiSearchResponse> listener) {
        final Transport.Connection connection = transportService.getConnection(transportService.getLocalNode());
        transportService.sendChildRequest(connection, MultiSearchAction.NAME, request, task,
                new ConnectionCountingHandler<>(listener, MultiSearchResponse::new, clientConnections, connection.getNode().getId()));
    }

    public RemoteClusterService getRemoteClusterService() {
        return transportService.getRemoteClusterService();
    }

    /**
     * Return a map of nodeId to pending number of search requests.
     * This is a snapshot of the current pending search and not a live map.
     */
    public Map<String, Long> getPendingSearchRequests() {
        return new HashMap<>(clientConnections);
    }

    static class ScrollFreeContextRequest extends TransportRequest {
        private SearchContextId contextId;

        ScrollFreeContextRequest(SearchContextId contextId) {
            this.contextId = Objects.requireNonNull(contextId);
        }

        ScrollFreeContextRequest(StreamInput in) throws IOException {
            super(in);
            contextId = new SearchContextId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            contextId.writeTo(out);
        }

        public SearchContextId id() {
            return this.contextId;
        }

    }

    static class SearchFreeContextRequest extends ScrollFreeContextRequest implements IndicesRequest {
        private OriginalIndices originalIndices;

        SearchFreeContextRequest(OriginalIndices originalIndices, SearchContextId id) {
            super(id);
            this.originalIndices = originalIndices;
        }

        SearchFreeContextRequest(StreamInput in) throws IOException {
            super(in);
            originalIndices = OriginalIndices.readOriginalIndices(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            OriginalIndices.writeOriginalIndices(originalIndices, out);
        }

        @Override
        public String[] indices() {
            if (originalIndices == null) {
                return null;
            }
            return originalIndices.indices();
        }

        @Override
        public IndicesOptions indicesOptions() {
            if (originalIndices == null) {
                return null;
            }
            return originalIndices.indicesOptions();
        }

        }

    public static class SearchFreeContextResponse extends TransportResponse {

        private boolean freed;

        SearchFreeContextResponse(StreamInput in) throws IOException {
            freed = in.readBoolean();
        }

        SearchFreeContextResponse(boolean freed) {
            this.freed = freed;
        }

        public boolean isFreed() {
            return freed;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(freed);
        }
    }

    public static void registerRequestHandler(TransportService transportService, SearchService searchService) {
        transportService.registerRequestHandler(FREE_CONTEXT_SCROLL_ACTION_NAME, ThreadPool.Names.SAME, ScrollFreeContextRequest::new,
            (request, channel, task) -> {
                boolean freed = searchService.freeContext(request.id());
                channel.sendResponse(new SearchFreeContextResponse(freed));
        });
        TransportActionProxy.registerProxyAction(transportService, FREE_CONTEXT_SCROLL_ACTION_NAME, SearchFreeContextResponse::new);
        transportService.registerRequestHandler(FREE_CONTEXT_ACTION_NAME, ThreadPool.Names.SAME, SearchFreeContextRequest::new,
            (request, channel, task) -> {
                boolean freed = searchService.freeContext(request.id());
                channel.sendResponse(new SearchFreeContextResponse(freed));
        });
        TransportActionProxy.registerProxyAction(transportService, FREE_CONTEXT_ACTION_NAME, SearchFreeContextResponse::new);
        transportService.registerRequestHandler(CLEAR_SCROLL_CONTEXTS_ACTION_NAME, ThreadPool.Names.SAME,
            TransportRequest.Empty::new,
            (request, channel, task) -> {
                searchService.freeAllScrollContexts();
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
        });
        TransportActionProxy.registerProxyAction(transportService, CLEAR_SCROLL_CONTEXTS_ACTION_NAME,
            (in) -> TransportResponse.Empty.INSTANCE);

        transportService.registerRequestHandler(DFS_ACTION_NAME, ThreadPool.Names.SAME, ShardSearchRequest::new,
            (request, channel, task) ->
                searchService.executeDfsPhase(request, (SearchShardTask) task,
                    new ChannelActionListener<>(channel, DFS_ACTION_NAME, request))
        );

        TransportActionProxy.registerProxyAction(transportService, DFS_ACTION_NAME, DfsSearchResult::new);

        transportService.registerRequestHandler(QUERY_ACTION_NAME, ThreadPool.Names.SAME, ShardSearchRequest::new,
            (request, channel, task) -> {
                searchService.executeQueryPhase(request, (SearchShardTask) task,
                    new ChannelActionListener<>(channel, QUERY_ACTION_NAME, request));
            });
        TransportActionProxy.registerProxyActionWithDynamicResponseType(transportService, QUERY_ACTION_NAME,
            (request) -> ((ShardSearchRequest)request).numberOfShards() == 1 ? QueryFetchSearchResult::new : QuerySearchResult::new);

        transportService.registerRequestHandler(QUERY_ID_ACTION_NAME, ThreadPool.Names.SAME, QuerySearchRequest::new,
            (request, channel, task) -> {
                searchService.executeQueryPhase(request, (SearchShardTask) task,
                    new ChannelActionListener<>(channel, QUERY_ID_ACTION_NAME, request));
            });
        TransportActionProxy.registerProxyAction(transportService, QUERY_ID_ACTION_NAME, QuerySearchResult::new);

        transportService.registerRequestHandler(QUERY_SCROLL_ACTION_NAME, ThreadPool.Names.SAME, InternalScrollSearchRequest::new,
            (request, channel, task) -> {
                searchService.executeQueryPhase(request, (SearchShardTask) task,
                    new ChannelActionListener<>(channel, QUERY_SCROLL_ACTION_NAME, request));
            });
        TransportActionProxy.registerProxyAction(transportService, QUERY_SCROLL_ACTION_NAME, ScrollQuerySearchResult::new);

        transportService.registerRequestHandler(QUERY_FETCH_SCROLL_ACTION_NAME, ThreadPool.Names.SAME, InternalScrollSearchRequest::new,
            (request, channel, task) -> {
                searchService.executeFetchPhase(request, (SearchShardTask) task,
                    new ChannelActionListener<>(channel, QUERY_FETCH_SCROLL_ACTION_NAME, request));
            });
        TransportActionProxy.registerProxyAction(transportService, QUERY_FETCH_SCROLL_ACTION_NAME, ScrollQueryFetchSearchResult::new);

        transportService.registerRequestHandler(FETCH_ID_SCROLL_ACTION_NAME, ThreadPool.Names.SAME, ShardFetchRequest::new,
            (request, channel, task) -> {
                searchService.executeFetchPhase(request, (SearchShardTask) task,
                    new ChannelActionListener<>(channel, FETCH_ID_SCROLL_ACTION_NAME, request));
            });
        TransportActionProxy.registerProxyAction(transportService, FETCH_ID_SCROLL_ACTION_NAME, FetchSearchResult::new);

        transportService.registerRequestHandler(FETCH_ID_ACTION_NAME, ThreadPool.Names.SAME, true, true, ShardFetchSearchRequest::new,
            (request, channel, task) -> {
                searchService.executeFetchPhase(request, (SearchShardTask) task,
                    new ChannelActionListener<>(channel, FETCH_ID_ACTION_NAME, request));
            });
        TransportActionProxy.registerProxyAction(transportService, FETCH_ID_ACTION_NAME, FetchSearchResult::new);

        // this is cheap, it does not fetch during the rewrite phase, so we can let it quickly execute on a networking thread
        transportService.registerRequestHandler(QUERY_CAN_MATCH_NAME, ThreadPool.Names.SAME, ShardSearchRequest::new,
            (request, channel, task) -> {
                searchService.canMatch(request, new ChannelActionListener<>(channel, QUERY_CAN_MATCH_NAME, request));
            });
        TransportActionProxy.registerProxyAction(transportService, QUERY_CAN_MATCH_NAME, SearchService.CanMatchResponse::new);

        transportService.registerRequestHandler(SEARCH_CONTEXT_HEARTBEAT_ACTION_NAME, ThreadPool.Names.SAME,
            SearchContextHeartbeatRequest::new, new SearchContextHeartbeatRequestHandler(searchService));
        TransportActionProxy.registerProxyAction(transportService, SEARCH_CONTEXT_HEARTBEAT_ACTION_NAME,
            in -> TransportResponse.Empty.INSTANCE);
    }


    /**
     * Returns a connection to the given node on the provided cluster. If the cluster alias is <code>null</code> the node will be resolved
     * against the local cluster.
     * @param clusterAlias the cluster alias the node should be resolved against
     * @param node the node to resolve
     * @return a connection to the given node belonging to the cluster with the provided alias.
     */
    Transport.Connection getConnection(@Nullable String clusterAlias, DiscoveryNode node) {
        if (clusterAlias == null) {
            return transportService.getConnection(node);
        } else {
            return transportService.getRemoteClusterService().getConnection(node, clusterAlias);
        }
    }

    final class ConnectionCountingHandler<Response extends TransportResponse> extends ActionListenerResponseHandler<Response> {
        private final Map<String, Long> clientConnections;
        private final String nodeId;

        ConnectionCountingHandler(final ActionListener<? super Response> listener, final Writeable.Reader<Response> responseReader,
                                  final Map<String, Long> clientConnections, final String nodeId) {
            super(listener, responseReader);
            this.clientConnections = clientConnections;
            this.nodeId = nodeId;
            // Increment the number of connections for this node by one
            clientConnections.compute(nodeId, (id, conns) -> conns == null ? 1 : conns + 1);
        }

        @Override
        public void handleResponse(Response response) {
            super.handleResponse(response);
            // Decrement the number of connections or remove it entirely if there are no more connections
            // We need to remove the entry here so we don't leak when nodes go away forever
            assert assertNodePresent();
            clientConnections.computeIfPresent(nodeId, (id, conns) -> conns.longValue() == 1 ? null : conns - 1);
        }

        @Override
        public void handleException(TransportException e) {
            super.handleException(e);
            // Decrement the number of connections or remove it entirely if there are no more connections
            // We need to remove the entry here so we don't leak when nodes go away forever
            assert assertNodePresent();
            clientConnections.computeIfPresent(nodeId, (id, conns) -> conns.longValue() == 1 ? null : conns - 1);
        }

        private boolean assertNodePresent() {
            clientConnections.compute(nodeId, (id, conns) -> {
                assert conns != null : "number of connections for " + id + " is null, but should be an integer";
                assert conns >= 1 : "number of connections for " + id + " should be >= 1 but was " + conns;
                return conns;
            });
            // Always return true, there is additional asserting here, the boolean is just so this
            // can be skipped when assertions are not enabled
            return true;
        }
    }

    public void startSendingSearchContextHeartbeat(SearchContextId contextId, Transport.Connection connection) {
        if (connection.getVersion().onOrAfter(Version.V_8_0_0)) {
            searchContextHeartbeatSender.addTarget(contextId, connection);
        }
    }

    public void stopSendingSearchContextHeartbeat(SearchContextId contextId) {
        searchContextHeartbeatSender.removeTarget(contextId);
    }

    static class SearchContextHeartbeatSender extends AbstractAsyncTask {
        private final Logger logger;
        private final TransportService transportService;
        private final ThreadPool threadPool;
        private final Map<SearchContextId, Transport.Connection> targets = ConcurrentCollections.newConcurrentMap();
        private final Set<Transport.Connection> ongoingConnections = ConcurrentCollections.newConcurrentSet();

        SearchContextHeartbeatSender(TransportService transportService, Logger logger, ThreadPool threadPool, TimeValue sendInterval) {
            super(logger, threadPool, sendInterval, true);
            this.logger = logger;
            this.transportService = transportService;
            this.threadPool = threadPool;
            rescheduleIfNecessary();
        }

        @Override
        protected boolean mustReschedule() {
            return true;
        }

        @Override
        protected void runInternal() {
            if (targets.isEmpty()) {
                return;
            }
            final Map<Transport.Connection, Set<SearchContextId>> perConnections = new HashMap<>();
            for (Map.Entry<SearchContextId, Transport.Connection> entry : targets.entrySet()) {
                perConnections.computeIfAbsent(entry.getValue(), k -> new HashSet<>()).add(entry.getKey());
            }
            final ThreadContext threadContext = threadPool.getThreadContext();
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                threadContext.markAsSystemContext();
                for (Map.Entry<Transport.Connection, Set<SearchContextId>> entry : perConnections.entrySet()) {
                    final Transport.Connection connection = entry.getKey();
                    final Set<SearchContextId> contextIds = entry.getValue();
                    if (ongoingConnections.add(connection) == false) {
                        continue; // let's wait for the next round
                    }
                    logger.trace("sending heartbeats {} to connection {}", contextIds, connection);
                    final SearchContextHeartbeatRequest request = new SearchContextHeartbeatRequest(contextIds);
                    transportService.sendRequest(connection, SEARCH_CONTEXT_HEARTBEAT_ACTION_NAME, request, TransportRequestOptions.EMPTY,
                        new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                            @Override
                            public void handleException(TransportException exp) {
                                assert ExceptionsHelper.unwrapCause(exp) instanceof ElasticsearchSecurityException == false;
                                ongoingConnections.remove(connection);
                                logger.debug("failed to send heartbeats {} to connection {}", contextIds, connection);
                            }

                            @Override
                            public void handleResponse(TransportResponse.Empty response) {
                                ongoingConnections.remove(connection);
                            }
                        });
                }
            }
        }

        void addTarget(SearchContextId contextId, Transport.Connection connection) {
            targets.put(contextId, connection);
        }

        void removeTarget(SearchContextId contextId) {
            targets.remove(contextId);
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.SAME;
        }
    }

    private static class SearchContextHeartbeatRequest extends TransportRequest {
        private final Collection<SearchContextId> contextIds;

        SearchContextHeartbeatRequest(Collection<SearchContextId> contextIds) {
            this.contextIds = Objects.requireNonNull(contextIds);
        }

        SearchContextHeartbeatRequest(StreamInput in) throws IOException {
            super(in);
            this.contextIds = in.readList(SearchContextId::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeCollection(contextIds);
        }
    }

    private static class SearchContextHeartbeatRequestHandler implements TransportRequestHandler<SearchContextHeartbeatRequest> {
        private final SearchService searchService;

        SearchContextHeartbeatRequestHandler(SearchService searchService) {
            this.searchService = searchService;
        }

        @Override
        public void messageReceived(SearchContextHeartbeatRequest request, TransportChannel channel, Task task) throws Exception {
            for (SearchContextId contextId : request.contextIds) {
                searchService.onHeartbeatReceived(contextId);
            }
        }
    }
}
