/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xpack.core.security.support.MigrateSecurityIndexFieldTaskParams;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.PRIMARY_SHARDS;

public class MigrateSecurityIndexFieldServiceExecutor extends PersistentTasksExecutor<MigrateSecurityIndexFieldTaskParams> {
    private static final Logger logger = LogManager.getLogger(MigrateSecurityIndexFieldServiceExecutor.class);
    private final SecuritySystemIndices securitySystemIndices;
    private final Client client;

    private static final String MIGRATE_SECURITY_INDEX_FIELD_COMPLETED_META_KEY = "migrate-security-index-field-completed";
    private final MasterServiceTaskQueue<UpdateSecurityIndexFieldMigrationCompleteTask> migrateSecurityIndexFieldCompletedTaskQueue;

    public MigrateSecurityIndexFieldServiceExecutor(
        ClusterService clusterService,
        String taskName,
        Executor executor,
        SecuritySystemIndices securitySystemIndices,
        Client client
    ) {
        super(taskName, executor);
        this.securitySystemIndices = securitySystemIndices;
        this.client = client;
        this.migrateSecurityIndexFieldCompletedTaskQueue = clusterService.createTaskQueue(
            "security-index-field-migration-completed-queue",
            Priority.LOW,
            MIGRATE_SECURITY_INDEX_FIELD_COMPLETED_TASK_EXECUTOR
        );
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, MigrateSecurityIndexFieldTaskParams params, PersistentTaskState state) {
        SecurityIndexManager securityIndex = securitySystemIndices.getMainIndexManager();
        final SecurityIndexManager frozenSecurityIndex = securityIndex.defensiveCopy();

        if (params.getMigrationNeeded() == false) {
            this.writeMetadataMigrated(
                ActionListener.wrap(
                    (response) -> logger.info("Migration not needed, updated."),
                    (exception -> logger.warn("Updating migration status failed: " + exception))
                ),
                true
            );
        }

        ActionListener<Void> listener = ActionListener.wrap((res) -> {
            logger.info("Security Index Field Migration complete - written to cluster state");
            task.markAsCompleted();
        }, (exception) -> {
            logger.warn("Security Index Field Migration failed: " + exception);
            task.markAsFailed(exception);
        });

        if (frozenSecurityIndex.indexExists() == false || frozenSecurityIndex.isAvailable(PRIMARY_SHARDS) == false) {
            // TODO should we handle isAvailable better?
            logger.info("security index not available");
            this.writeMetadataMigrated(listener, true);
        } else {
            UpdateByQueryRequestBuilder updateByQueryRequestBuilder = new UpdateByQueryRequestBuilder(client);
            updateByQueryRequestBuilder.filter(
                // Skipping api key since already migrated
                new BoolQueryBuilder().should(QueryBuilders.termQuery("type", "user"))
                    .should(QueryBuilders.termQuery("type", "role"))
                    .should(QueryBuilders.termQuery("type", "role-mapping"))
                    .should(QueryBuilders.termQuery("type", "application-privilege"))
                    .should(QueryBuilders.termQuery("doc_type", "role-mapping"))
            );
            updateByQueryRequestBuilder.source(securitySystemIndices.getMainIndexManager().aliasName());
            updateByQueryRequestBuilder.script(
                new Script(ScriptType.INLINE, "painless", "ctx._source.metadata_flattened = ctx._source.metadata", Collections.emptyMap())
            );

            securityIndex.checkIndexVersionThenExecute(
                (exception) -> logger.warn("Couldn't query security index " + exception),
                () -> executeAsyncWithOrigin(
                    client,
                    SECURITY_ORIGIN,
                    UpdateByQueryAction.INSTANCE,
                    updateByQueryRequestBuilder.request(),
                    ActionListener.wrap(bulkByScrollResponse -> {
                        logger.info("Migrated [{}] security index fields", bulkByScrollResponse.getUpdated());
                        this.writeMetadataMigrated(listener, true);
                    }, (exception) -> logger.info("Updating security index fields failed!" + exception))
                )
            );
        }
    }

    public boolean shouldStartMetadataMigration(ClusterState clusterState) {
        IndexMetadata indexMetadata = resolveConcreteIndex(clusterState.metadata());
        if (indexMetadata != null) {
            Map<String, String> customMetadata = indexMetadata.getCustomData(MIGRATE_SECURITY_INDEX_FIELD_COMPLETED_META_KEY);
            if (customMetadata != null) {
                String result = customMetadata.get("completed");
                logger.info("COMPLETED? " + result);
                return result == null || Boolean.parseBoolean(result) == false;
            }
        }
        return true;
    }

    public void writeMetadataMigrated(ActionListener<Void> listener, boolean value) {
        this.migrateSecurityIndexFieldCompletedTaskQueue.submitTask(
            "Updating cluster state to show that the security index field migration has been completed",
            new UpdateSecurityIndexFieldMigrationCompleteTask(value, listener),
            null
        );
    }

    private static final SimpleBatchedExecutor<
        UpdateSecurityIndexFieldMigrationCompleteTask,
        Void> MIGRATE_SECURITY_INDEX_FIELD_COMPLETED_TASK_EXECUTOR = new SimpleBatchedExecutor<>() {
            @Override
            public Tuple<ClusterState, Void> executeTask(UpdateSecurityIndexFieldMigrationCompleteTask task, ClusterState clusterState) {
                return Tuple.tuple(task.execute(clusterState), null);
            }

            @Override
            public void taskSucceeded(UpdateSecurityIndexFieldMigrationCompleteTask task, Void unused) {
                task.listener.onResponse(null);
            }
        };

    /**
     * Resolves a concrete index name or alias to a {@link IndexMetadata} instance.  Requires
     * that if supplied with an alias, the alias resolves to at most one concrete index.
     */
    private static IndexMetadata resolveConcreteIndex(final Metadata metadata) {
        final IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(SecuritySystemIndices.SECURITY_MAIN_ALIAS);
        if (indexAbstraction != null) {
            final List<Index> indices = indexAbstraction.getIndices();
            if (indexAbstraction.getType() != IndexAbstraction.Type.CONCRETE_INDEX && indices.size() > 1) {
                throw new IllegalStateException(
                    "Alias [" + SecuritySystemIndices.SECURITY_MAIN_ALIAS + "] points to more than one index: " + indices
                );
            }
            return metadata.index(indices.get(0));
        }
        return null;
    }

    public static class UpdateSecurityIndexFieldMigrationCompleteTask implements ClusterStateTaskListener {
        private final ActionListener<Void> listener;
        private final boolean value;

        UpdateSecurityIndexFieldMigrationCompleteTask(boolean value, ActionListener<Void> listener) {
            this.value = value;
            this.listener = listener;
        }

        ClusterState execute(ClusterState currentState) {
            IndexMetadata indexMetadata = resolveConcreteIndex(currentState.metadata());
            if (indexMetadata != null) {
                logger.info("Updating cluster state with security index migration completed");
                IndexMetadata updatededIndexMetadata = new IndexMetadata.Builder(indexMetadata).putCustom(
                    MIGRATE_SECURITY_INDEX_FIELD_COMPLETED_META_KEY,
                    Map.of("completed", Boolean.toString(value))
                ).build();
                Metadata metadata = Metadata.builder(currentState.metadata()).put(updatededIndexMetadata, true).build();
                return ClusterState.builder(currentState).metadata(metadata).build();
            }
            return currentState;
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }
}
