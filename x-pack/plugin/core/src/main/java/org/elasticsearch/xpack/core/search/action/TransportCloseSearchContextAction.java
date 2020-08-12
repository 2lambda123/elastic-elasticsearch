/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ClearScrollController;
import org.elasticsearch.action.search.SearchContextId;
import org.elasticsearch.action.search.SearchContextIdForNode;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;

public class TransportCloseSearchContextAction extends HandledTransportAction<CloseSearchContextRequest, CloseSearchContextResponse> {

    private final ClusterService clusterService;
    private final SearchTransportService searchTransportService;
    private final NamedWriteableRegistry namedWriteableRegistry;

    @Inject
    public TransportCloseSearchContextAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        SearchTransportService searchTransportService,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        super(CloseSearchContextAction.NAME, transportService, actionFilters, CloseSearchContextRequest::new);
        this.clusterService = clusterService;
        this.searchTransportService = searchTransportService;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    @Override
    protected void doExecute(Task task, CloseSearchContextRequest request, ActionListener<CloseSearchContextResponse> listener) {
        final SearchContextId searchContextId = SearchContextId.decode(namedWriteableRegistry, request.getId());
        final Collection<SearchContextIdForNode> contextIds = searchContextId.shards().values();
        ClearScrollController.closeContexts(
            clusterService.state().nodes(),
            searchTransportService,
            contextIds,
            ActionListener.map(listener, freed -> new CloseSearchContextResponse(freed == contextIds.size(), freed))
        );
    }
}
