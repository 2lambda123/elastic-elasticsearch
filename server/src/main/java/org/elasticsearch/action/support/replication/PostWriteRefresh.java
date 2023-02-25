/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.replication;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.refresh.TransportUnpromotableShardRefreshAction;
import org.elasticsearch.action.admin.indices.refresh.UnpromotableShardRefreshRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class PostWriteRefresh {

    public static final String FORCED_REFRESH_AFTER_INDEX = "refresh_flag_index";
    private final TransportService transportService;

    public PostWriteRefresh(final TransportService transportService) {
        this.transportService = transportService;
    }

    public void refreshShard(
        WriteRequest.RefreshPolicy policy,
        IndexShard indexShard,
        @Nullable Translog.Location location,
        ActionListener<Boolean> listener
    ) {
        doRefreshShard(policy, true, transportService, indexShard, location, listener);
    }

    public static void refreshReplicaShard(
        WriteRequest.RefreshPolicy policy,
        IndexShard indexShard,
        @Nullable Translog.Location location,
        ActionListener<Boolean> listener
    ) {
        doRefreshShard(policy, false, null, indexShard, location, listener);
    }

    private static void doRefreshShard(
        WriteRequest.RefreshPolicy policy,
        boolean isPrimary,
        @Nullable TransportService transportService,
        IndexShard indexShard,
        @Nullable Translog.Location location,
        ActionListener<Boolean> listener
    ) {
        switch (policy) {
            case NONE -> listener.onResponse(false);
            case WAIT_UNTIL -> {
                if (location != null) {
                    indexShard.addRefreshListener(
                        location,
                        forced -> afterRefresh(indexShard, isPrimary, transportService, location, listener, forced, null)
                    );
                } else {
                    listener.onResponse(false);
                }
            }
            case IMMEDIATE -> {
                Engine.RefreshResult refreshResult = indexShard.refresh(FORCED_REFRESH_AFTER_INDEX);
                afterRefresh(indexShard, isPrimary, transportService, location, listener, true, refreshResult);
            }
            default -> throw new IllegalArgumentException("unknown refresh policy: " + policy);
        }
    }

    private static void afterRefresh(
        IndexShard indexShard,
        boolean isPrimary,
        @Nullable TransportService transportService,
        Translog.Location location,
        ActionListener<Boolean> listener,
        boolean wasForced,
        Engine.RefreshResult refreshResult
    ) {
        if (isPrimary && indexShard.getReplicationGroup().getRoutingTable().unpromotableShards().size() > 0) {
            // Was wait_for, still need to fetch generation
            if (refreshResult == null) {
                Engine engineOrNull = indexShard.getEngineOrNull();
                if (engineOrNull == null) {
                    listener.onFailure(new AlreadyClosedException("Engine closed during refresh."));
                    return;
                }
                engineOrNull.addFlushListener(location, new ActionListener<>() {
                    @Override
                    public void onResponse(Long generation) {
                        sendUnpromotableRequests(indexShard, transportService, generation, wasForced, listener);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });

            } else {
                sendUnpromotableRequests(indexShard, transportService, refreshResult.generation(), wasForced, listener);
            }
        } else {
            listener.onResponse(wasForced);
        }
    }

    private static void sendUnpromotableRequests(
        IndexShard indexShard,
        TransportService transportService,
        long generation,
        boolean wasForced,
        ActionListener<Boolean> listener
    ) {
        UnpromotableShardRefreshRequest unpromotableReplicaRequest = new UnpromotableShardRefreshRequest(
            indexShard.getReplicationGroup().getRoutingTable(),
            generation
        );
        transportService.sendRequest(
            transportService.getLocalNode(),
            TransportUnpromotableShardRefreshAction.NAME,
            unpromotableReplicaRequest,
            new ActionListenerResponseHandler<>(
                listener.delegateFailure((l, r) -> l.onResponse(wasForced)),
                (in) -> ActionResponse.Empty.INSTANCE,
                ThreadPool.Names.REFRESH
            )
        );
    }

}
