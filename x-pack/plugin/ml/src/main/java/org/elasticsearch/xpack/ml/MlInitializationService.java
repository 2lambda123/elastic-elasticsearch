/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_HIDDEN;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class MlInitializationService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(MlInitializationService.class);

    private final Client client;
    private final ThreadPool threadPool;
    private final AtomicBoolean isIndexCreationInProgress = new AtomicBoolean(false);
    private final AtomicBoolean mlInternalIndicesHidden = new AtomicBoolean(false);
    private volatile String previousException;

    private final MlDailyMaintenanceService mlDailyMaintenanceService;

    private boolean isMaster = false;

    MlInitializationService(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        Client client,
        MlAssignmentNotifier mlAssignmentNotifier
    ) {
        this(
            client,
            threadPool,
            new MlDailyMaintenanceService(
                settings,
                Objects.requireNonNull(clusterService).getClusterName(),
                threadPool,
                client,
                clusterService,
                mlAssignmentNotifier
            ),
            clusterService
        );
    }

    // For testing
    public MlInitializationService(
        Client client,
        ThreadPool threadPool,
        MlDailyMaintenanceService dailyMaintenanceService,
        ClusterService clusterService
    ) {
        this.client = Objects.requireNonNull(client);
        this.threadPool = threadPool;
        this.mlDailyMaintenanceService = dailyMaintenanceService;
        clusterService.addListener(this);
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void afterStart() {
                clusterService.getClusterSettings()
                    .addSettingsUpdateConsumer(
                        MachineLearning.NIGHTLY_MAINTENANCE_REQUESTS_PER_SECOND,
                        mlDailyMaintenanceService::setDeleteExpiredDataRequestsPerSecond
                    );
            }

            @Override
            public void beforeStop() {
                offMaster();
            }
        });
    }

    public void onMaster() {
        mlDailyMaintenanceService.start();
        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(this::makeMlInternalIndicesHidden);
    }

    public void offMaster() {
        mlDailyMaintenanceService.stop();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final boolean prevIsMaster = this.isMaster;
        if (prevIsMaster != event.localNodeMaster()) {
            this.isMaster = event.localNodeMaster();
            if (this.isMaster) {
                onMaster();
            } else {
                offMaster();
            }
        }

        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // Wait until the gateway has recovered from disk.
            return;
        }

        // The atomic flag prevents multiple simultaneous attempts to create the
        // index if there is a flurry of cluster state updates in quick succession
        if (this.isMaster && isIndexCreationInProgress.compareAndSet(false, true)) {
            AnnotationIndex.createAnnotationsIndexIfNecessary(
                client,
                event.state(),
                MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT,
                ActionListener.wrap(r -> isIndexCreationInProgress.set(false), e -> {
                    if (e.getMessage().equals(previousException)) {
                        logger.debug("Error creating ML annotations index or aliases", e);
                    } else {
                        previousException = e.getMessage();
                        logger.error("Error creating ML annotations index or aliases", e);
                    }
                    isIndexCreationInProgress.set(false);
                })
            );
        }
    }

    /** For testing */
    MlDailyMaintenanceService getDailyMaintenanceService() {
        return mlDailyMaintenanceService;
    }

    /** For testing */
    public boolean areMlInternalIndicesHidden() {
        return mlInternalIndicesHidden.get();
    }

    private void makeMlInternalIndicesHidden() {
        String[] mlHiddenIndexPatterns = MachineLearning.getMlHiddenIndexPatterns();

        // Step 5: Handle errors encountered on the way.
        ActionListener<AcknowledgedResponse> finalListener = ActionListener.wrap(updateAliasesResponse -> {
            if (updateAliasesResponse.isAcknowledged() == false) {
                logger.error("One or more of the ML internal aliases could not be made hidden.");
                return;
            }
            mlInternalIndicesHidden.set(true);
        }, e -> logger.error("An error occurred while making ML internal indices and aliases hidden", e));

        // Step 4: Extract ML internal aliases that are not hidden and make them hidden.
        ActionListener<GetAliasesResponse> getAliasesResponseListener = ActionListener.wrap(getAliasesResponse -> {
            IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
            for (ObjectObjectCursor<String, List<AliasMetadata>> entry : getAliasesResponse.getAliases()) {
                String index = entry.key;
                String[] nonHiddenAliases = entry.value.stream()
                    .filter(metadata -> metadata.isHidden() == null || metadata.isHidden() == false)
                    .map(AliasMetadata::alias)
                    .toArray(String[]::new);
                if (nonHiddenAliases.length == 0) {
                    continue;
                }
                indicesAliasesRequest.addAliasAction(
                    IndicesAliasesRequest.AliasActions.add()
                        .index(index)
                        .aliases(entry.value.stream().map(AliasMetadata::alias).toArray(String[]::new))
                        .isHidden(true)
                );
            }
            if (indicesAliasesRequest.getAliasActions().isEmpty()) {
                logger.debug("There are no ML internal aliases that need to be made hidden, [{}]", getAliasesResponse.getAliases());
                finalListener.onResponse(AcknowledgedResponse.TRUE);
                return;
            }
            String indicesWithNonHiddenAliasesString = indicesAliasesRequest.getAliasActions()
                .stream()
                .map(aliasAction -> aliasAction.indices()[0] + ": " + String.join(",", aliasAction.aliases()))
                .collect(Collectors.joining("; "));
            logger.debug("The following ML internal aliases will now be made hidden: [{}]", indicesWithNonHiddenAliasesString);
            executeAsyncWithOrigin(client, ML_ORIGIN, IndicesAliasesAction.INSTANCE, indicesAliasesRequest, finalListener);
        }, finalListener::onFailure);

        // Step 3: Once indices are hidden, fetch ML internal aliases to find out whether the aliases are hidden or not.
        ActionListener<AcknowledgedResponse> updateSettingsListener = ActionListener.wrap(updateSettingsResponse -> {
            if (updateSettingsResponse.isAcknowledged() == false) {
                logger.error("One or more of the ML internal indices could not be made hidden.");
                return;
            }
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest().indices(mlHiddenIndexPatterns)
                .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN);
            executeAsyncWithOrigin(client, ML_ORIGIN, GetAliasesAction.INSTANCE, getAliasesRequest, getAliasesResponseListener);
        }, finalListener::onFailure);

        // Step 2: Extract ML internal indices that are not hidden and make them hidden.
        ActionListener<GetSettingsResponse> getSettingsListener = ActionListener.wrap(getSettingsResponse -> {
            String[] nonHiddenIndices = getSettingsResponse.getIndexToSettings()
                .stream()
                .filter(e -> e.getValue().getAsBoolean(SETTING_INDEX_HIDDEN, false) == false)
                .map(Map.Entry::getKey)
                .toArray(String[]::new);
            if (nonHiddenIndices.length == 0) {
                logger.debug("There are no ML internal indices that need to be made hidden, [{}]", getSettingsResponse);
                updateSettingsListener.onResponse(AcknowledgedResponse.TRUE);
                return;
            }
            String nonHiddenIndicesString = Arrays.stream(nonHiddenIndices).collect(Collectors.joining(", "));
            logger.debug("The following ML internal indices will now be made hidden: [{}]", nonHiddenIndicesString);
            UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest().indices(nonHiddenIndices)
                .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN)
                .settings(Collections.singletonMap(SETTING_INDEX_HIDDEN, true));
            executeAsyncWithOrigin(client, ML_ORIGIN, UpdateSettingsAction.INSTANCE, updateSettingsRequest, updateSettingsListener);
        }, finalListener::onFailure);

        // Step 1: Fetch ML internal indices settings to find out whether they are already hidden or not.
        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices(mlHiddenIndexPatterns)
            .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN);
        client.admin().indices().getSettings(getSettingsRequest, getSettingsListener);
    }
}
