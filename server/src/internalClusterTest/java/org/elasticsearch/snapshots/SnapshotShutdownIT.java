/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.oneOf;

public class SnapshotShutdownIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    public void testRestartNodeDuringSnapshot() throws Exception {
        // Marking a node for restart has no impact on snapshots (see #71333 for how to handle this case)
        internalCluster().ensureAtLeastNumDataNodes(1);
        final var originalNode = internalCluster().startDataOnlyNode();
        final var indexName = randomIdentifier();
        createIndexWithContent(
            indexName,
            indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", originalNode).build()
        );

        final var repoName = randomIdentifier();
        createRepository(repoName, "mock");

        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var snapshotFuture = startFullSnapshotBlockedOnDataNode(randomIdentifier(), repoName, originalNode);
        final var snapshotCompletesWithoutPausingListener = ClusterServiceUtils.addTemporaryStateListener(clusterService, state -> {
            final var entriesForRepo = SnapshotsInProgress.get(state).forRepo(repoName);
            if (entriesForRepo.isEmpty()) {
                return true;
            }
            assertThat(entriesForRepo, hasSize(1));
            final var shardSnapshotStatuses = entriesForRepo.iterator().next().shards().values();
            assertThat(shardSnapshotStatuses, hasSize(1));
            assertThat(
                shardSnapshotStatuses.iterator().next().state(),
                oneOf(SnapshotsInProgress.ShardState.INIT, SnapshotsInProgress.ShardState.SUCCESS)
            );
            return false;
        });

        PlainActionFuture.<Void, RuntimeException>get(
            fut -> putShutdownMetadata(
                clusterService,
                SingleNodeShutdownMetadata.builder()
                    .setType(SingleNodeShutdownMetadata.Type.RESTART)
                    .setStartedAtMillis(clusterService.threadPool().absoluteTimeInMillis())
                    .setReason("test"),
                originalNode,
                fut
            ),
            10,
            TimeUnit.SECONDS
        );
        assertFalse(snapshotCompletesWithoutPausingListener.isDone());
        unblockAllDataNodes(repoName); // lets the shard snapshot continue so the snapshot can succeed
        assertEquals(SnapshotState.SUCCESS, snapshotFuture.get(10, TimeUnit.SECONDS).getSnapshotInfo().state());
        safeAwait(snapshotCompletesWithoutPausingListener);
        clearShutdownMetadata(clusterService);
    }

    public void testRemoveNodeDuringSnapshot() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        final var originalNode = internalCluster().startDataOnlyNode();
        final var indexName = randomIdentifier();
        createIndexWithContent(
            indexName,
            indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", originalNode).build()
        );

        final var repoName = randomIdentifier();
        createRepository(repoName, "mock");

        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var snapshotFuture = startFullSnapshotBlockedOnDataNode(randomIdentifier(), repoName, originalNode);
        final var snapshotPausedListener = createSnapshotPausedListener(clusterService, repoName);

        updateIndexSettings(Settings.builder().putNull(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name"), indexName);
        putShutdownForRemovalMetadata(originalNode, clusterService);
        unblockAllDataNodes(repoName); // lets the shard snapshot abort, which frees up the shard so it can move
        safeAwait(snapshotPausedListener);

        // snapshot completes when the node vacates even though it hasn't been removed yet
        assertEquals(SnapshotState.SUCCESS, snapshotFuture.get(10, TimeUnit.SECONDS).getSnapshotInfo().state());

        if (randomBoolean()) {
            internalCluster().stopNode(originalNode);
        }

        clearShutdownMetadata(clusterService);
    }

    public void testStartRemoveNodeButDoNotComplete() throws Exception {
        final var primaryNode = internalCluster().startDataOnlyNode();
        final var indexName = randomIdentifier();
        createIndexWithContent(
            indexName,
            indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", primaryNode).build()
        );

        final var repoName = randomIdentifier();
        createRepository(repoName, "mock");

        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var snapshotFuture = startFullSnapshotBlockedOnDataNode(randomIdentifier(), repoName, primaryNode);
        final var snapshotPausedListener = createSnapshotPausedListener(clusterService, repoName);

        putShutdownForRemovalMetadata(primaryNode, clusterService);
        unblockAllDataNodes(repoName); // lets the shard snapshot abort, but allocation filtering stops it from moving
        safeAwait(snapshotPausedListener);
        assertFalse(snapshotFuture.isDone());

        // give up on the node shutdown so the shard snapshot can restart
        clearShutdownMetadata(clusterService);

        assertEquals(SnapshotState.SUCCESS, snapshotFuture.get(10, TimeUnit.SECONDS).getSnapshotInfo().state());
    }

    public void testShutdownWhileSuccessInFlight() throws Exception {
        final var primaryNode = internalCluster().startDataOnlyNode();
        final var indexName = randomIdentifier();
        createIndexWithContent(
            indexName,
            indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", primaryNode).build()
        );

        final var repoName = randomIdentifier();
        createRepository(repoName, "mock");

        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var masterTransportService = MockTransportService.getInstance(internalCluster().getMasterName());
        masterTransportService.addRequestHandlingBehavior(
            SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME,
            (handler, request, channel, task) -> SubscribableListener

                .<Void>newForked(
                    l -> putShutdownMetadata(
                        clusterService,
                        SingleNodeShutdownMetadata.builder()
                            .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                            .setStartedAtMillis(clusterService.threadPool().absoluteTimeInMillis())
                            .setReason("test"),
                        primaryNode,
                        l
                    )
                )

                .<Void>andThen((l, ignored) -> flushMasterQueue(clusterService, l))

                .addListener(ActionTestUtils.assertNoFailureListener(ignored -> handler.messageReceived(request, channel, task)))
        );

        assertEquals(
            SnapshotState.SUCCESS,
            startFullSnapshot(repoName, randomIdentifier()).get(10, TimeUnit.SECONDS).getSnapshotInfo().state()
        );
        clearShutdownMetadata(clusterService);
    }

    private static SubscribableListener<Void> createSnapshotPausedListener(ClusterService clusterService, String repoName) {
        return ClusterServiceUtils.addTemporaryStateListener(clusterService, state -> {
            final var entriesForRepo = SnapshotsInProgress.get(state).forRepo(repoName);
            assertThat(entriesForRepo, hasSize(1));
            final var shardSnapshotStatuses = entriesForRepo.iterator().next().shards().values();
            assertThat(shardSnapshotStatuses, hasSize(1));
            final var shardState = shardSnapshotStatuses.iterator().next().state();
            assertThat(shardState, oneOf(SnapshotsInProgress.ShardState.INIT, SnapshotsInProgress.ShardState.WAITING));
            return shardState == SnapshotsInProgress.ShardState.WAITING;
        });
    }

    private static void putShutdownForRemovalMetadata(String nodeName, ClusterService clusterService) {
        PlainActionFuture.<Void, RuntimeException>get(
            fut -> SubscribableListener

                .<Void>newForked(l ->

                putShutdownMetadata(
                    clusterService,
                    SingleNodeShutdownMetadata.builder()
                        .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                        .setStartedAtMillis(clusterService.threadPool().absoluteTimeInMillis())
                        .setReason("test"),
                    nodeName,
                    l
                ))

                .<Void>andThen((l, ignored) -> flushMasterQueue(clusterService, l))

                .addListener(fut),
            10,
            TimeUnit.SECONDS
        );
    }

    private static void flushMasterQueue(ClusterService clusterService, ActionListener<Void> listener) {
        clusterService.submitUnbatchedStateUpdateTask("flush queue", new ClusterStateUpdateTask(Priority.LANGUID) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                listener.onResponse(null);
            }
        });
    }

    private static void putShutdownMetadata(
        ClusterService clusterService,
        SingleNodeShutdownMetadata.Builder shutdownMetadataBuilder,
        String nodeName,
        ActionListener<Void> listener
    ) {
        clusterService.submitUnbatchedStateUpdateTask("mark node for removal", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final var nodeId = currentState.nodes().resolveNode(nodeName).getId();
                return currentState.copyAndUpdateMetadata(
                    mdb -> mdb.putCustom(
                        NodesShutdownMetadata.TYPE,
                        new NodesShutdownMetadata(Map.of(nodeId, shutdownMetadataBuilder.setNodeId(nodeId).build()))
                    )
                );
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                listener.onResponse(null);
            }
        });
    }

    private static void clearShutdownMetadata(ClusterService clusterService) {
        PlainActionFuture.get(fut -> clusterService.submitUnbatchedStateUpdateTask("remove restart marker", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return currentState.copyAndUpdateMetadata(mdb -> mdb.putCustom(NodesShutdownMetadata.TYPE, NodesShutdownMetadata.EMPTY));
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                fut.onResponse(null);
            }
        }), 10, TimeUnit.SECONDS);
    }
}
