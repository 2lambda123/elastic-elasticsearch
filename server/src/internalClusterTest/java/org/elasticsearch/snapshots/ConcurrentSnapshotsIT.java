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
package org.elasticsearch.snapshots;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.discovery.AbstractDisruptionTestCase;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFileExists;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ConcurrentSnapshotsIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockRepository.Plugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put(AbstractDisruptionTestCase.DEFAULT_SETTINGS)
                .build();
    }

    public void testLongRunningSnapshotAllowsConcurrentSnapshot() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepoPath());
        createIndexWithContent("index-slow");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture =
                startFullSnapshotBlockedOnDataNode("slow-snapshot", repoName, dataNode);

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        final String indexFast = "index-fast";
        createIndexWithContent(indexFast, dataNode2, dataNode);

        assertSuccessful(client().admin().cluster().prepareCreateSnapshot(repoName, "fast-snapshot")
                .setIndices(indexFast).setWaitForCompletion(true).execute());

        assertThat(createSlowFuture.isDone(), is(false));
        unblockNode(repoName, dataNode);

        assertSuccessful(createSlowFuture);
    }

    public void testDeletesAreBatched() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepoPath());

        createIndex("foo");
        ensureGreen();

        final int numSnapshots = randomIntBetween(1, 4);
        final PlainActionFuture<Collection<CreateSnapshotResponse>> allSnapshotsDone = PlainActionFuture.newFuture();
        final ActionListener<CreateSnapshotResponse> snapshotsListener = new GroupedActionListener<>(allSnapshotsDone, numSnapshots);
        final Collection<String> snapshotNames = new HashSet<>();
        for (int i = 0; i < numSnapshots; i++) {
            final String snapshot = "snap-" + i;
            snapshotNames.add(snapshot);
            client().admin().cluster().prepareCreateSnapshot(repoName, snapshot).setWaitForCompletion(true)
                    .execute(snapshotsListener);
        }
        final Collection<CreateSnapshotResponse> snapshotResponses = allSnapshotsDone.get();
        for (CreateSnapshotResponse snapshotResponse : snapshotResponses) {
            assertThat(snapshotResponse.getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
        }

        createIndexWithContent("index-slow");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture =
                startFullSnapshotBlockedOnDataNode("blocked-snapshot", repoName, dataNode);

        final Collection<StepListener<AcknowledgedResponse>> deleteFutures = new ArrayList<>();
        while (snapshotNames.isEmpty() == false) {
            final Collection<String> toDelete = randomSubsetOf(snapshotNames);
            if (toDelete.isEmpty()) {
                continue;
            }
            snapshotNames.removeAll(toDelete);
            final StepListener<AcknowledgedResponse> future = new StepListener<>();
            client().admin().cluster().prepareDeleteSnapshot(repoName, toDelete.toArray(Strings.EMPTY_ARRAY)).execute(future);
            deleteFutures.add(future);
        }

        assertThat(createSlowFuture.isDone(), is(false));

        final long repoGenAfterInitialSnapshots = getRepositoryData(repoName).getGenId();
        assertThat(repoGenAfterInitialSnapshots, is(numSnapshots - 1L));
        unblockNode(repoName, dataNode);

        final SnapshotInfo slowSnapshotInfo = assertSuccessful(createSlowFuture);

        logger.info("--> waiting for batched deletes to finish");
        final PlainActionFuture<Collection<AcknowledgedResponse>> allDeletesDone = new PlainActionFuture<>();
        final ActionListener<AcknowledgedResponse> deletesListener = new GroupedActionListener<>(allDeletesDone, deleteFutures.size());
        for (StepListener<AcknowledgedResponse> deleteFuture : deleteFutures) {
            deleteFuture.whenComplete(deletesListener::onResponse, deletesListener::onFailure);
        }
        allDeletesDone.get();

        logger.info("--> verifying repository state");
        final RepositoryData repositoryDataAfterDeletes = getRepositoryData(repoName);
        // One increment for snapshot, one for all the deletes
        assertThat(repositoryDataAfterDeletes.getGenId(), is(repoGenAfterInitialSnapshots + 2));
        assertThat(repositoryDataAfterDeletes.getSnapshotIds(), contains(slowSnapshotInfo.snapshotId()));
    }

    public void testBlockedRepoDoesNotBlockOtherRepos() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String blockedRepoName = "test-repo-blocked";
        final String otherRepoName = "test-repo";
        createRepository(blockedRepoName, "mock", randomRepoPath());
        createRepository(otherRepoName, "fs", randomRepoPath());
        createIndex("foo");
        ensureGreen();
        createIndexWithContent("index-slow");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture =
                startAndBlockFailingFullSnapshot(blockedRepoName, "blocked-snapshot");

        client().admin().cluster().prepareCreateSnapshot(otherRepoName, "snapshot")
                .setIndices("does-not-exist-*")
                .setWaitForCompletion(false).get();

        unblockNode(blockedRepoName, internalCluster().getMasterName());
        expectThrows(SnapshotException.class, createSlowFuture::actionGet);

        assertBusy(() -> assertThat(currentSnapshots(otherRepoName), empty()), 30L, TimeUnit.SECONDS);
    }

    public void testMultipleReposAreIndependent() throws Exception {
        internalCluster().startMasterOnlyNode();
        // We're blocking a some of the snapshot threads when we block the first repo below so we have to make sure we have enough threads
        // left for the second concurrent snapshot.
        final String dataNode = internalCluster().startDataOnlyNode(Settings.builder()
                .put("thread_pool.snapshot.core", 5).put("thread_pool.snapshot.max", 5).build());
        final String blockedRepoName = "test-repo-blocked";
        final String otherRepoName = "test-repo";
        createRepository(blockedRepoName, "mock", randomRepoPath());
        createRepository(otherRepoName, "fs", randomRepoPath());
        createIndexWithContent("test-index");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture =
                startFullSnapshotBlockedOnDataNode("blocked-snapshot", blockedRepoName, dataNode);

        logger.info("--> waiting for concurrent snapshot to finish");
        createFullSnapshot(otherRepoName, "snapshot");

        logger.info("--> unblocking data node");
        unblockNode(blockedRepoName, dataNode);
        assertSuccessful(createSlowFuture);
    }

    public void testSnapshotRunsAfterInProgressDelete() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepoPath());

        ensureGreen();
        createIndexWithContent("index-test");

        final String firstSnapshot = "first-snapshot";
        createFullSnapshot(repoName, firstSnapshot);

        blockMasterFromFinalizingSnapshotOnIndexFile(repoName);
        final ActionFuture<AcknowledgedResponse> deleteFuture = startDelete(repoName, firstSnapshot);
        waitForBlock(masterNode, repoName, TimeValue.timeValueSeconds(30L));

        final ActionFuture<CreateSnapshotResponse> snapshotFuture = startFullSnapshot(repoName, "second-snapshot");

        logger.info("--> unblocking master node");
        unblockNode(repoName, masterNode);
        final UncategorizedExecutionException ex = expectThrows(UncategorizedExecutionException.class, deleteFuture::actionGet);
        assertThat(ex.getRootCause(), instanceOf(IOException.class));

        assertSuccessful(snapshotFuture);
    }

    public void testAbortOneOfMultipleSnapshots() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepoPath());
        final String firstIndex = "index-one";
        createIndexWithContent(firstIndex);

        final String firstSnapshot = "snapshot-one";
        final ActionFuture<CreateSnapshotResponse> firstSnapshotResponse =
                startFullSnapshotBlockedOnDataNode(firstSnapshot, repoName, dataNode);

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        final String secondIndex = "index-two";
        createIndexWithContent(secondIndex, dataNode2, dataNode);

        final String secondSnapshot = "snapshot-two";
        final ActionFuture<CreateSnapshotResponse> secondSnapshotResponse = startFullSnapshot(repoName, secondSnapshot);

        logger.info("--> wait for snapshot on second data node to finish");
        awaitClusterState(state -> {
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE);
            return snapshotsInProgress != null && snapshotsInProgress.entries().size() == 2
                    && snapshotHasCompletedShard(secondSnapshot, snapshotsInProgress);
        });

        final ActionFuture<AcknowledgedResponse> deleteSnapshotsResponse = startDelete(repoName, firstSnapshot);
        awaitNDeletionsInProgress(1);

        logger.info("--> start third snapshot");
        final ActionFuture<CreateSnapshotResponse> thirdSnapshotResponse = client().admin().cluster()
                .prepareCreateSnapshot(repoName, "snapshot-three").setIndices(secondIndex).setWaitForCompletion(true).execute();

        assertThat(firstSnapshotResponse.isDone(), is(false));
        assertThat(secondSnapshotResponse.isDone(), is(false));

        logger.info("--> unblocking data node");
        unblockNode(repoName, dataNode);
        assertThat(firstSnapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.FAILED));

        final SnapshotInfo secondSnapshotInfo = assertSuccessful(secondSnapshotResponse);
        final SnapshotInfo thirdSnapshotInfo = assertSuccessful(thirdSnapshotResponse);

        assertThat(deleteSnapshotsResponse.get().isAcknowledged(), is(true));

        logger.info("--> verify that the first snapshot is gone");
        assertThat(client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots(repoName),
                containsInAnyOrder(secondSnapshotInfo, thirdSnapshotInfo));
    }

    public void testCascadedAborts() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepoPath());
        createIndexWithContent("index-one");

        final String firstSnapshot = "snapshot-one";
        final ActionFuture<CreateSnapshotResponse> firstSnapshotResponse =
                startFullSnapshotBlockedOnDataNode(firstSnapshot, repoName, dataNode);

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        createIndexWithContent("index-two", dataNode2, dataNode);

        final String secondSnapshot = "snapshot-two";
        final ActionFuture<CreateSnapshotResponse> secondSnapshotResponse = startFullSnapshot(repoName, secondSnapshot);

        logger.info("--> wait for snapshot on second data node to finish");
        awaitClusterState(state -> {
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE);
            return snapshotsInProgress != null && snapshotsInProgress.entries().size() == 2 &&
                    snapshotHasCompletedShard(secondSnapshot, snapshotsInProgress);
        });

        final ActionFuture<AcknowledgedResponse> deleteSnapshotsResponse = startDelete(repoName, firstSnapshot);
        awaitNDeletionsInProgress(1);

        final ActionFuture<CreateSnapshotResponse> thirdSnapshotResponse = startFullSnapshot(repoName, "snapshot-three");

        assertThat(firstSnapshotResponse.isDone(), is(false));
        assertThat(secondSnapshotResponse.isDone(), is(false));

        logger.info("--> waiting for all three snapshots to show up as in-progress");
        assertBusy(() -> assertThat(currentSnapshots(repoName), hasSize(3)), 30L, TimeUnit.SECONDS);

        final ActionFuture<AcknowledgedResponse> allDeletedResponse = startDelete(repoName, "*");

        logger.info("--> waiting for second and third snapshot to finish");
        assertBusy(() -> {
            assertThat(currentSnapshots(repoName), hasSize(1));
            final SnapshotsInProgress snapshotsInProgress = clusterService().state().custom(SnapshotsInProgress.TYPE);
            assertThat(snapshotsInProgress.entries().get(0).state(), is(SnapshotsInProgress.State.ABORTED));
        }, 30L, TimeUnit.SECONDS);

        logger.info("--> unblocking data node");
        unblockNode(repoName, dataNode);

        logger.info("--> verify all snapshots were aborted");
        assertThat(firstSnapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.FAILED));
        assertThat(secondSnapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.FAILED));
        assertThat(thirdSnapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.FAILED));

        logger.info("--> verify both deletes have completed");
        assertAcked(deleteSnapshotsResponse.get());
        assertAcked(allDeletedResponse.get());

        logger.info("--> verify that all snapshots are gone");
        assertThat(client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots(repoName), empty());
    }

    public void testMasterFailOverWithQueuedDeletes() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepoPath());

        final String firstIndex = "index-one";
        createIndexWithContent(firstIndex);

        final String firstSnapshot = "snapshot-one";
        blockDataNode(repoName, dataNode);
        final ActionFuture<CreateSnapshotResponse> firstSnapshotResponse = startFullSnapshotFromNonMasterClient(repoName, firstSnapshot);
        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(5);
        final String secondIndex = "index-two";
        createIndexWithContent(secondIndex, dataNode2, dataNode);

        final String secondSnapshot = "snapshot-two";
        final ActionFuture<CreateSnapshotResponse> secondSnapshotResponse = startFullSnapshot(repoName, secondSnapshot);

        logger.info("--> wait for snapshot on second data node to finish");
        awaitClusterState(state -> {
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE);
            return snapshotsInProgress != null && snapshotsInProgress.entries().size() == 2
                    && snapshotHasCompletedShard(secondSnapshot, snapshotsInProgress);
        });

        final ActionFuture<AcknowledgedResponse> firstDeleteFuture = startDeleteFromNonMasterClient(repoName, firstSnapshot);
        awaitNDeletionsInProgress(1);

        blockDataNode(repoName, dataNode2);
        final ActionFuture<CreateSnapshotResponse> snapshotThreeFuture = startFullSnapshotFromNonMasterClient(repoName, "snapshot-three");
        waitForBlock(dataNode2, repoName, TimeValue.timeValueSeconds(30L));

        assertThat(firstSnapshotResponse.isDone(), is(false));
        assertThat(secondSnapshotResponse.isDone(), is(false));

        logger.info("--> waiting for all three snapshots to show up as in-progress");
        assertBusy(() -> assertThat(currentSnapshots(repoName), hasSize(3)), 30L, TimeUnit.SECONDS);

        final ActionFuture<AcknowledgedResponse> deleteAllSnapshots = startDeleteFromNonMasterClient(repoName, "*");
        logger.info("--> wait for delete to be enqueued in cluster state");
        awaitClusterState(state -> {
            final SnapshotDeletionsInProgress deletionsInProgress = state.custom(SnapshotDeletionsInProgress.TYPE);
            return deletionsInProgress.getEntries().size() == 1 && deletionsInProgress.getEntries().get(0).getSnapshots().size() == 3;
        });

        logger.info("--> waiting for second snapshot to finish and the other two snapshots to become aborted");
        assertBusy(() -> {
            assertThat(currentSnapshots(repoName), hasSize(2));
            final SnapshotsInProgress snapshotsInProgress = clusterService().state().custom(SnapshotsInProgress.TYPE);
            for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
                assertThat(entry.state(), is(SnapshotsInProgress.State.ABORTED));
                assertThat(entry.snapshot().getSnapshotId().getName(), not(secondSnapshot));
            }
        }, 30L, TimeUnit.SECONDS);

        logger.info("--> stopping current master node");
        internalCluster().stopCurrentMasterNode();

        logger.info("--> unblocking data nodes");
        unblockNode(repoName, dataNode);
        unblockNode(repoName, dataNode2);

        assertAcked(firstDeleteFuture.get());
        assertAcked(deleteAllSnapshots.get());
        expectThrows(SnapshotException.class, snapshotThreeFuture::actionGet);

        logger.info("--> verify that all snapshots are gone and no more work is left in the cluster state");
        assertBusy(() -> {
            assertThat(client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots(repoName), empty());
            final ClusterState state = clusterService().state();
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE);
            assertThat(snapshotsInProgress.entries(), empty());
            final SnapshotDeletionsInProgress snapshotDeletionsInProgress = state.custom(SnapshotDeletionsInProgress.TYPE);
            assertThat(snapshotDeletionsInProgress.getEntries(), empty());
        }, 30L, TimeUnit.SECONDS);
    }

    public void testAssertMultipleSnapshotsAndPrimaryFailOver() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepoPath());

        final String testIndex = "index-one";
        createIndex(testIndex, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 1).build());
        ensureYellow(testIndex);
        indexDoc(testIndex, "some_id", "foo", "bar");

        blockDataNode(repoName, dataNode);
        final ActionFuture<CreateSnapshotResponse> firstSnapshotResponse = startFullSnapshotFromMasterClient(repoName, "snapshot-one");
        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));

        internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        ensureGreen(testIndex);

        final String secondSnapshot = "snapshot-two";
        final ActionFuture<CreateSnapshotResponse> secondSnapshotResponse = startFullSnapshotFromMasterClient(repoName, secondSnapshot);

        internalCluster().restartNode(dataNode, InternalTestCluster.EMPTY_CALLBACK);

        assertThat(firstSnapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.PARTIAL));
        assertThat(secondSnapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.PARTIAL));
    }

    public void testQueuedDeletesWithFailures() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepoPath());
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        blockMasterFromFinalizingSnapshotOnIndexFile(repoName);
        final ActionFuture<AcknowledgedResponse> firstDeleteFuture = startDelete(repoName, "*");
        waitForBlock(masterNode, repoName, TimeValue.timeValueSeconds(30L));

        final ActionFuture<CreateSnapshotResponse> thirdSnapshotFuture = startFullSnapshot(repoName, "snapshot-three");
        awaitClusterState(state -> {
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE);
            return snapshotsInProgress.entries().isEmpty() == false;
        });

        final ActionFuture<AcknowledgedResponse> secondDeleteFuture = startDelete(repoName, "*");
        awaitNDeletionsInProgress(2);

        unblockNode(repoName, masterNode);
        expectThrows(UncategorizedExecutionException.class, firstDeleteFuture::actionGet);

        // Second delete works out cleanly since the repo is unblocked now
        assertThat(secondDeleteFuture.get().isAcknowledged(), is(true));
        // Snapshot should have been aborted
        assertThat(thirdSnapshotFuture.get().getSnapshotInfo().state(), is(SnapshotState.FAILED));

        assertThat(client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots(repoName), empty());
    }

    public void testQueuedDeletesWithOverlap() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepoPath());
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final ActionFuture<AcknowledgedResponse> firstDeleteFuture = startAndBlockOnDeleteSnapshot(repoName, "*");
        final ActionFuture<CreateSnapshotResponse> thirdSnapshotFuture = startFullSnapshot(repoName, "snapshot-three");
        awaitClusterState(state -> {
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE);
            return snapshotsInProgress.entries().isEmpty() == false;
        });

        final ActionFuture<AcknowledgedResponse> secondDeleteFuture = startDelete(repoName, "*");
        awaitNDeletionsInProgress(2);

        unblockNode(repoName, masterNode);
        assertThat(firstDeleteFuture.get().isAcknowledged(), is(true));

        // Second delete works out cleanly since the repo is unblocked now
        assertThat(secondDeleteFuture.get().isAcknowledged(), is(true));
        // Snapshot should have been aborted
        assertThat(thirdSnapshotFuture.get().getSnapshotInfo().state(), is(SnapshotState.FAILED));

        assertThat(client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots(repoName), empty());
    }

    public void testQueuedOperationsOnMasterRestart() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepoPath());
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        startAndBlockOnDeleteSnapshot(repoName, "*");

        client().admin().cluster().prepareCreateSnapshot(repoName, "snapshot-three").setWaitForCompletion(false).get();

        startDelete(repoName, "*");
        awaitNDeletionsInProgress(2);

        internalCluster().stopCurrentMasterNode();
        ensureStableCluster(3);

        awaitNoMoreRunningOperations();
    }

    public void testQueuedOperationsOnMasterDisconnect() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepoPath());
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final String masterNode = internalCluster().getMasterName();
        NetworkDisruption networkDisruption = new NetworkDisruption(
                new NetworkDisruption.TwoPartitions(
                        Collections.singleton(masterNode),
                        Arrays.stream(internalCluster().getNodeNames()).filter(name -> name.equals(masterNode) == false)
                                .collect(Collectors.toSet())),
                new NetworkDisruption.NetworkDisconnect());
        internalCluster().setDisruptionScheme(networkDisruption);

        blockNodeOnAnyFiles(repoName, masterNode);
        ActionFuture<AcknowledgedResponse> firstDeleteFuture = client(masterNode).admin().cluster()
                .prepareDeleteSnapshot(repoName, "*").execute();
        waitForBlock(masterNode, repoName, TimeValue.timeValueSeconds(30L));

        final ActionFuture<CreateSnapshotResponse> createThirdSnapshot = client(masterNode).admin().cluster()
                .prepareCreateSnapshot(repoName, "snapshot-three").setWaitForCompletion(true).execute();
        awaitClusterState(state -> {
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE);
            return snapshotsInProgress != null && snapshotsInProgress.entries().size() == 1;
        });

        final ActionFuture<AcknowledgedResponse> secondDeleteFuture =
                client(masterNode).admin().cluster().prepareDeleteSnapshot(repoName, "*").execute();
        awaitNDeletionsInProgress(2);

        networkDisruption.startDisrupting();
        ensureStableCluster(3, dataNode);
        unblockNode(repoName, masterNode);
        networkDisruption.stopDisrupting();

        logger.info("--> make sure all failing requests get a response");
        expectThrows(RepositoryException.class, firstDeleteFuture::actionGet);
        expectThrows(RepositoryException.class, secondDeleteFuture::actionGet);
        expectThrows(RepositoryException.class, createThirdSnapshot::actionGet);

        awaitNoMoreRunningOperations();
        networkDisruption.stopDisrupting();
    }

    public void testQueuedOperationsAndBrokenRepoOnMasterFailOver() throws Exception {
        disableRepoConsistencyCheck("This test corrupts the repository on purpose");

        internalCluster().startMasterOnlyNodes(3);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "mock", repoPath);
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final long generation = getRepositoryData(repoName).getGenId();

        startAndBlockOnDeleteSnapshot(repoName, "*");

        corruptIndexN(repoPath, generation);

        client().admin().cluster().prepareCreateSnapshot(repoName, "snapshot-three").setWaitForCompletion(false).get();

        final ActionFuture<AcknowledgedResponse> deleteFuture = startDeleteFromNonMasterClient(repoName, "*");
        awaitNDeletionsInProgress(2);

        internalCluster().stopCurrentMasterNode();
        ensureStableCluster(3);

        awaitNoMoreRunningOperations();
        expectThrows(RepositoryException.class, deleteFuture::actionGet);
    }

    public void testQueuedSnapshotOperationsAndBrokenRepoOnMasterFailOver() throws Exception {
        disableRepoConsistencyCheck("This test corrupts the repository on purpose");

        internalCluster().startMasterOnlyNodes(3);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "mock", repoPath);
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final long generation = getRepositoryData(repoName).getGenId();
        final String masterNode = internalCluster().getMasterName();
        blockNodeOnAnyFiles(repoName, masterNode);
        final ActionFuture<CreateSnapshotResponse> snapshotThree = startFullSnapshotFromNonMasterClient(repoName, "snapshot-three");
        waitForBlock(masterNode, repoName, TimeValue.timeValueSeconds(30L));

        corruptIndexN(repoPath, generation);

        final ActionFuture<CreateSnapshotResponse> snapshotFour = startFullSnapshotFromNonMasterClient(repoName, "snapshot-four");
        internalCluster().stopCurrentMasterNode();
        ensureStableCluster(3);

        awaitNoMoreRunningOperations();
        expectThrows(ElasticsearchException.class, snapshotThree::actionGet);
        expectThrows(ElasticsearchException.class, snapshotFour::actionGet);
    }

    public void testMultipleSnapshotsQueuedAfterDelete() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepoPath());
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(1, 5));

        final ActionFuture<AcknowledgedResponse> deleteFuture = startAndBlockOnDeleteSnapshot(repoName, "*");
        final ActionFuture<CreateSnapshotResponse> snapshotThree = startFullSnapshot(repoName, "snapshot-three");
        final ActionFuture<CreateSnapshotResponse> snapshotFour = startFullSnapshot(repoName, "snapshot-four");

        unblockNode(repoName, masterNode);

        assertSuccessful(snapshotThree);
        assertSuccessful(snapshotFour);
        assertAcked(deleteFuture.get());
    }

    public void testQueuedSnapshotsWaitingForShardReady() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        final String repoName = "test-repo";
        createRepository(repoName, "fs", randomRepoPath());

        final String testIndex = "test-idx";
        // Create index on two nodes and make sure each node has a primary by setting no replicas
        assertAcked(prepareCreate(testIndex, 2, Settings.builder()
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put(SETTING_NUMBER_OF_SHARDS, between(2, 10))));

        ensureGreen(testIndex);

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            indexDoc(testIndex, Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client().prepareSearch(testIndex).setSize(0).get().getHits().getTotalHits().value, equalTo(100L));

        logger.info("--> start relocations");
        allowNodes(testIndex, 1);

        logger.info("--> wait for relocations to start");
        assertBusy(() -> assertThat(
                client().admin().cluster().prepareHealth(testIndex).execute().actionGet().getRelocatingShards(), greaterThan(0)),
                1L, TimeUnit.MINUTES);

        logger.info("--> start two snapshots");
        final String snapshotOne = "snap-1";
        final String snapshotTwo = "snap-2";
        final ActionFuture<CreateSnapshotResponse> snapOneResponse = client().admin().cluster()
                .prepareCreateSnapshot(repoName, snapshotOne).setWaitForCompletion(false).setIndices(testIndex).execute();
        final ActionFuture<CreateSnapshotResponse> snapTwoResponse = client().admin().cluster()
                .prepareCreateSnapshot(repoName, snapshotTwo).setWaitForCompletion(false).setIndices(testIndex).execute();

        snapOneResponse.get();
        snapTwoResponse.get();
        logger.info("--> wait for snapshot to complete");
        for (String snapshot : Arrays.asList(snapshotOne, snapshotTwo)) {
            SnapshotInfo snapshotInfo = waitForCompletion(repoName, snapshot, TimeValue.timeValueSeconds(600));
            assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
            assertThat(snapshotInfo.shardFailures().size(), equalTo(0));
        }
    }

    public void testBackToBackQueuedDeletes() throws Exception {
        final String masterName = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepoPath());
        createIndexWithContent("index-test");
        final List<String> snapshots = createNSnapshots(repoName, 2);
        final String snapshotOne = snapshots.get(0);
        final String snapshotTwo = snapshots.get(1);

        final ActionFuture<AcknowledgedResponse> deleteSnapshotOne = startAndBlockOnDeleteSnapshot(repoName, snapshotOne);
        final ActionFuture<AcknowledgedResponse> deleteSnapshotTwo = startDelete(repoName, snapshotTwo);
        awaitNDeletionsInProgress(2);

        unblockNode(repoName, masterName);
        assertAcked(deleteSnapshotOne.get());
        assertAcked(deleteSnapshotTwo.get());

        final RepositoryData repositoryData = getRepositoryData(repoName);
        assertThat(repositoryData.getSnapshotIds(), empty());
        // Two snapshots and two distinct delete operations move us 4 steps from -1 to 3
        assertThat(repositoryData.getGenId(), is(3L));
    }

    public void testQueuedOperationsAfterFinalizationFailure() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock", randomRepoPath());
        createIndexWithContent("index-test");

        final List<String> snapshotNames = createNSnapshots(repoName, randomIntBetween(2, 5));

        final ActionFuture<CreateSnapshotResponse> snapshotThree = startAndBlockFailingFullSnapshot(repoName, "snap-other");

        final String masterName = internalCluster().getMasterName();

        final String snapshotOne = snapshotNames.get(0);
        final ActionFuture<AcknowledgedResponse> deleteSnapshotOne = startDelete(repoName, snapshotOne);
        awaitNDeletionsInProgress(1);

        unblockNode(repoName, masterName);

        expectThrows(SnapshotException.class, snapshotThree::actionGet);
        assertAcked(deleteSnapshotOne.get());
    }

    private List<String> createNSnapshots(String repoName, int count) throws Exception {
        final List<String> snapshotNames = new ArrayList<>(count);
        final String prefix = "snap-" + UUIDs.randomBase64UUID(random()).toLowerCase(Locale.ROOT) + "-";
        for (int i = 0; i < count; i++) {
            final String name = prefix + i;
            createFullSnapshot(repoName, name);
            snapshotNames.add(name);
        }
        logger.info("--> created {} in [{}]", snapshotNames, repoName);
        return snapshotNames;
    }

    private void awaitNoMoreRunningOperations() throws Exception {
        logger.info("--> verify no more operations in the cluster state");
        awaitClusterState(state -> {
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE);
            final SnapshotDeletionsInProgress snapshotDeletionsInProgress = state.custom(SnapshotDeletionsInProgress.TYPE);
            if (snapshotsInProgress == null || snapshotDeletionsInProgress == null) {
                return false;
            }
            return snapshotsInProgress.entries().isEmpty() && snapshotDeletionsInProgress.hasDeletionsInProgress() == false;
        });
    }

    private ActionFuture<AcknowledgedResponse> startDeleteFromNonMasterClient(String repoName, String snapshotName) {
        logger.info("--> deleting snapshot [{}] from repo [{}] from non master client", snapshotName, repoName);
        return internalCluster().nonMasterClient().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName).execute();
    }

    private ActionFuture<AcknowledgedResponse> startDelete(String repoName, String snapshotName) {
        logger.info("--> deleting snapshot [{}] from repo [{}]", snapshotName, repoName);
        return client().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName).execute();
    }

    private ActionFuture<CreateSnapshotResponse> startFullSnapshotFromNonMasterClient(String repoName, String snapshotName) {
        logger.info("--> creating full snapshot [{}] to repo [{}] from non master client", snapshotName, repoName);
        return internalCluster().nonMasterClient().admin().cluster().prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(true).execute();
    }

    private ActionFuture<CreateSnapshotResponse> startFullSnapshotFromMasterClient(String repoName, String snapshotName) {
        logger.info("--> creating full snapshot [{}] to repo [{}] from master client", snapshotName, repoName);
        return internalCluster().masterClient().admin().cluster().prepareCreateSnapshot(repoName, snapshotName)
                .setWaitForCompletion(true).execute();
    }

    private ActionFuture<CreateSnapshotResponse> startFullSnapshot(String repoName, String snapshotName) {
        logger.info("--> creating full snapshot [{}] to repo [{}]", snapshotName, repoName);
        return client().admin().cluster().prepareCreateSnapshot(repoName, snapshotName).setWaitForCompletion(true).execute();
    }

    private void createFullSnapshot(String repoName, String snapshotName) throws Exception {
        assertSuccessful(startFullSnapshot(repoName, snapshotName));
    }

    private void awaitClusterState(Predicate<ClusterState> statePredicate) throws Exception {
        final ClusterService clusterService = internalCluster().getMasterNodeInstance(ClusterService.class);
        final ThreadPool threadPool = internalCluster().getMasterNodeInstance(ThreadPool.class);
        final ClusterStateObserver observer = new ClusterStateObserver(clusterService, logger, threadPool.getThreadContext());
        if (statePredicate.test(observer.setAndGetObservedState()) == false) {
            final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    future.onResponse(null);
                }

                @Override
                public void onClusterServiceClose() {
                    future.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    future.onFailure(new TimeoutException());
                }
            }, statePredicate);
            future.get(30L, TimeUnit.SECONDS);
        }
    }

    private static final Settings SINGLE_SHARD_NO_REPLICA =
            Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0).build();

    private void createIndexWithContent(String indexName) {
        createIndexWithContent(indexName, SINGLE_SHARD_NO_REPLICA);
    }

    private void createIndexWithContent(String indexName, String nodeInclude, String nodeExclude) {
        createIndexWithContent(indexName, Settings.builder().put(SINGLE_SHARD_NO_REPLICA)
                .put("index.routing.allocation.include._name", nodeInclude)
                .put("index.routing.allocation.exclude._name", nodeExclude).build());
    }

    private void createIndexWithContent(String indexName, Settings indexSettings) {
        logger.info("--> creating index [{}]", indexName);
        createIndex(indexName, indexSettings);
        ensureGreen(indexName);
        indexDoc(indexName, "some_id", "foo", "bar");
    }

    private static boolean snapshotHasCompletedShard(String snapshot, SnapshotsInProgress snapshotsInProgress) {
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
            if (entry.snapshot().getSnapshotId().getName().equals(snapshot)) {
                for (ObjectCursor<SnapshotsInProgress.ShardSnapshotStatus> shard : entry.shards().values()) {
                    if (shard.value.state().completed()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static SnapshotInfo assertSuccessful(ActionFuture<CreateSnapshotResponse> future) throws Exception {
        final SnapshotInfo snapshotInfo = future.get().getSnapshotInfo();
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
        return snapshotInfo;
    }

    private void corruptIndexN(Path repoPath, long generation) throws IOException {
        logger.info("--> corrupting [index-{}] in [{}]", generation, repoPath);
        Path indexNBlob = repoPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + generation);
        assertFileExists(indexNBlob);
        Files.write(indexNBlob, randomByteArrayOfLength(1), StandardOpenOption.TRUNCATE_EXISTING);
    }

    private void awaitNDeletionsInProgress(int count) throws Exception {
        logger.info("--> wait for [{}] deletions to show up in the cluster state", count);
        awaitClusterState(state -> {
            final SnapshotDeletionsInProgress deletionsInProgress = state.custom(SnapshotDeletionsInProgress.TYPE);
            return deletionsInProgress != null && deletionsInProgress.getEntries().size() == count;
        });
    }

    private static List<SnapshotInfo> currentSnapshots(String repoName) {
        return client().admin().cluster().prepareGetSnapshots(repoName).setSnapshots(GetSnapshotsRequest.CURRENT_SNAPSHOT)
                .get().getSnapshots(repoName);
    }

    private ActionFuture<AcknowledgedResponse> startAndBlockOnDeleteSnapshot(String repoName, String snapshotName)
            throws InterruptedException {
        final String masterName = internalCluster().getMasterName();
        blockNodeOnAnyFiles(repoName, masterName);
        final ActionFuture<AcknowledgedResponse> fut = startDelete(repoName, snapshotName);
        waitForBlock(masterName, repoName, TimeValue.timeValueSeconds(30L));
        return fut;
    }

    private ActionFuture<CreateSnapshotResponse> startAndBlockFailingFullSnapshot(String blockedRepoName, String snapshotName)
            throws InterruptedException {
        blockMasterFromFinalizingSnapshotOnIndexFile(blockedRepoName);
        final ActionFuture<CreateSnapshotResponse> fut = startFullSnapshot(blockedRepoName, snapshotName);
        waitForBlock(internalCluster().getMasterName(), blockedRepoName, TimeValue.timeValueSeconds(30L));
        return fut;
    }

    private ActionFuture<CreateSnapshotResponse> startFullSnapshotBlockedOnDataNode(String firstSnapshot, String repoName, String dataNode)
            throws InterruptedException {
        blockDataNode(repoName, dataNode);
        final ActionFuture<CreateSnapshotResponse> fut = startFullSnapshot(repoName, firstSnapshot);
        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));
        return fut;
    }
}
