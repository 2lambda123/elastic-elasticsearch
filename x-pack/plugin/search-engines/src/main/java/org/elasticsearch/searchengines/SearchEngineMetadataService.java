/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.searchengines;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.SearchEngine;
import org.elasticsearch.cluster.metadata.SearchEngineMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.searchengines.action.DeleteSearchEngineAction;
import org.elasticsearch.searchengines.action.PutSearchEngineAction;
import org.elasticsearch.searchengines.analytics.SearchEngineAnalyticsBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SearchEngineMetadataService {

    private static final Logger logger = LogManager.getLogger(SearchEngineMetadataService.class);

    private final ClusterService clusterService;
    private final Client client;

    /**
     * Cluster state task executor for ingest pipeline operations
     */
    static final ClusterStateTaskExecutor<SearchEngineMetadataService.ClusterStateUpdateTask> TASK_EXECUTOR = batchExecutionContext -> {
        final SearchEngineMetadata initialMetadata = batchExecutionContext.initialState()
            .metadata()
            .custom(SearchEngineMetadata.TYPE, SearchEngineMetadata.EMPTY);
        var currentMetadata = initialMetadata;
        for (final var taskContext : batchExecutionContext.taskContexts()) {
            try {
                final var task = taskContext.getTask();
                try (var ignored = taskContext.captureResponseHeaders()) {
                    currentMetadata = task.execute(currentMetadata, batchExecutionContext.initialState());
                }
                taskContext.success(() -> task.listener.onResponse(AcknowledgedResponse.TRUE));
            } catch (Exception e) {
                taskContext.onFailure(e);
            }
        }
        final var finalMetadata = currentMetadata;
        return finalMetadata == initialMetadata
            ? batchExecutionContext.initialState()
            : batchExecutionContext.initialState().copyAndUpdateMetadata(b -> { b.put(finalMetadata); });
    };

    /**
     * Specialized cluster state update task specifically for ingest pipeline operations.
     * These operations all receive an AcknowledgedResponse.
     */
    abstract static class ClusterStateUpdateTask implements ClusterStateTaskListener {
        final ActionListener<AcknowledgedResponse> listener;

        ClusterStateUpdateTask(ActionListener<AcknowledgedResponse> listener) {
            this.listener = listener;
        }

        public abstract SearchEngineMetadata execute(SearchEngineMetadata currentMetadata, ClusterState state);

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    public SearchEngineMetadataService(ClusterService clusterService, Client client) {
        this.clusterService = clusterService;
        this.client = client;
    }

    public void putSearchEngine(PutSearchEngineAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        try {
            if (Strings.hasText(request.getAnalyticsCollection())) {
                SearchEngineAnalyticsBuilder.ensureDataStreamExists(request.getAnalyticsCollection(), client);
            }
        } catch (Exception e) {
            logger.error("Error setting up analytics for engine " + request.getName(), e);
        } finally {
            clusterService.submitStateUpdateTask(
                "put-search-engine-" + request.getName(),
                new PutSearchEngineClusterStateUpdateTask(listener, request),
                ClusterStateTaskConfig.build(Priority.NORMAL, request.masterNodeTimeout()),
                TASK_EXECUTOR
            );
        }

    }

    public void deleteSearchEngine(DeleteSearchEngineAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask(
            "delete-search-engine-" + String.join("", request.getNames()),
            new DeleteSearchEngineClusterStateUpdateTask(listener, request),
            ClusterStateTaskConfig.build(Priority.NORMAL, request.masterNodeTimeout()),
            TASK_EXECUTOR
        );
    }

    static class PutSearchEngineClusterStateUpdateTask extends ClusterStateUpdateTask {
        private final PutSearchEngineAction.Request request;

        PutSearchEngineClusterStateUpdateTask(ActionListener<AcknowledgedResponse> listener, PutSearchEngineAction.Request request) {
            super(listener);
            this.request = request;
        }

        private void validate(PutSearchEngineAction.Request request, ClusterState state) {
            // - validate index names, make sure they exist
            List<String> missingIndices = new ArrayList<>();
            for (String index : request.indices()) {
                if ((state.routingTable().hasIndex(index)
                    || state.metadata().hasIndex(index)
                    || state.metadata().hasAlias(index)) == false) {
                    missingIndices.add(index);
                }
            }
            if (missingIndices.size() > 0) {
                throw new IndexNotFoundException(String.join(",", missingIndices));
            }
        }

        @Override
        public SearchEngineMetadata execute(SearchEngineMetadata currentMetadata, ClusterState state) {
            validate(request, state);

            Map<String, SearchEngine> searchEngines = new HashMap<>(currentMetadata.searchEngines());

            List<Index> indices = new ArrayList<>();
            for (String indexName : request.indices()) {
                indices.add(state.getMetadata().index(indexName).getIndex());
            }

            SearchEngine searchEngine = new SearchEngine(
                request.getName(),
                indices,
                false,
                false,
                request.getRelevanceSettingsId(),
                request.getAnalyticsCollection()
            );
            searchEngines.put(request.getName(), searchEngine);

            return new SearchEngineMetadata(searchEngines);
        }
    }

    static class DeleteSearchEngineClusterStateUpdateTask extends ClusterStateUpdateTask {
        private final DeleteSearchEngineAction.Request request;

        DeleteSearchEngineClusterStateUpdateTask(ActionListener<AcknowledgedResponse> listener, DeleteSearchEngineAction.Request request) {
            super(listener);
            this.request = request;
        }

        @Override
        public SearchEngineMetadata execute(SearchEngineMetadata currentMetadata, ClusterState state) {
            if (request.getResolved() == null || request.getResolved().isEmpty()) {
                throw new SearchEngineNotFoundException(String.join(",", request.getNames()));
            }

            Map<String, SearchEngine> searchEngines = new HashMap<>(currentMetadata.searchEngines());
            for (String name : request.getNames()) {
                searchEngines.remove(name);
            }

            return new SearchEngineMetadata(searchEngines);
        }
    }

    static class SearchEngineNotFoundException extends ResourceNotFoundException {
        SearchEngineNotFoundException(String engine) {
            this(engine, (Throwable) null);
        }

        SearchEngineNotFoundException(String engine, Throwable cause) {
            super("no such engine [" + engine + "]", cause);
        }

        SearchEngineNotFoundException(StreamInput in) throws IOException {
            super(in);
        }
    }
}
