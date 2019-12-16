/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ESIntegTestCase;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.StreamSupport;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotRepository.SNAPSHOT_DIRECTORY_FACTORY_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class SearchableSnapshotsIntegTests extends ESIntegTestCase {

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(SearchableSnapshots.class);
    }

    public void testCreateAndRestoreSearchableSnapshot() throws Exception {
        final String fsRepoName = randomAlphaOfLength(10);
        final String searchableRepoName = randomAlphaOfLength(10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String restoredIndexName = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        final Path repo = randomRepoPath();
        assertAcked(client().admin().cluster().preparePutRepository(fsRepoName)
            .setType("fs")
            .setSettings(Settings.builder()
                .put("location", repo)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        createIndex(indexName);
        final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = between(10, 50); i >= 0; i--) {
            indexRequestBuilders.add(client().prepareIndex(indexName).setSource("foo", randomBoolean() ? "bar" : "baz"));
        }
        // TODO NORELEASE no dummy docs since that includes deletes, yet we always copy the .liv file in peer recovery
        indexRandom(true, false, indexRequestBuilders);
        refresh(indexName);
        assertThat(client().admin().indices().prepareForceMerge(indexName)
            .setOnlyExpungeDeletes(true).setFlush(true) .get().getFailedShards(), equalTo(0));

        final TotalHits originalAllHits = internalCluster().client().prepareSearch(indexName).get().getHits().getTotalHits();
        final TotalHits originalBarHits = internalCluster().client().prepareSearch(indexName)
            .setQuery(matchQuery("foo", "bar")).get().getHits().getTotalHits();
        logger.info("--> [{}] in total, of which [{}] match the query", originalAllHits, originalBarHits);

        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot(fsRepoName, snapshotName)
            .setWaitForCompletion(true).get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));

        assertAcked(client().admin().indices().prepareDelete(indexName));

        assertAcked(client().admin().cluster().preparePutRepository(searchableRepoName)
            .setType("searchable")
            .setSettings(Settings.builder()
                .put("delegate_type", "fs")
                .put("location", repo)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        final RestoreSnapshotResponse restoreSnapshotResponse = client().admin().cluster()
            .prepareRestoreSnapshot(searchableRepoName, snapshotName).setIndices(indexName)
            .setRenamePattern(indexName).setRenameReplacement(restoredIndexName).setWaitForCompletion(true).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));

        final Settings settings
            = client().admin().indices().prepareGetSettings(restoredIndexName).get().getIndexToSettings().get(restoredIndexName);
        assertThat(SearchableSnapshotRepository.SNAPSHOT_REPOSITORY_SETTING.get(settings), equalTo(searchableRepoName));
        assertThat(SearchableSnapshotRepository.SNAPSHOT_SNAPSHOT_NAME_SETTING.get(settings), equalTo(snapshotName));
        assertThat(IndexModule.INDEX_STORE_TYPE_SETTING.get(settings), equalTo(SNAPSHOT_DIRECTORY_FACTORY_KEY));
        assertTrue(IndexMetaData.INDEX_BLOCKS_WRITE_SETTING.get(settings));
        assertTrue(SearchableSnapshotRepository.SNAPSHOT_SNAPSHOT_ID_SETTING.exists(settings));
        assertTrue(SearchableSnapshotRepository.SNAPSHOT_INDEX_ID_SETTING.exists(settings));

        assertRecovered(restoredIndexName, originalAllHits, originalBarHits);

        internalCluster().fullRestart();
        assertRecovered(restoredIndexName, originalAllHits, originalBarHits);

        internalCluster().ensureAtLeastNumDataNodes(2);

        final DiscoveryNode dataNode = randomFrom(StreamSupport.stream(client().admin().cluster().prepareState().get().getState().nodes()
            .getDataNodes().values().spliterator(), false).map(c -> c.value).toArray(DiscoveryNode[]::new));

        assertAcked(client().admin().indices().prepareUpdateSettings(restoredIndexName).setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(),
                    dataNode.getName())));

        assertFalse(client().admin().cluster().prepareHealth(restoredIndexName)
            .setWaitForNoRelocatingShards(true).setWaitForEvents(Priority.LANGUID).get().isTimedOut());

        assertRecovered(restoredIndexName, originalAllHits, originalBarHits);
    }

    private void assertRecovered(String indexName, TotalHits originalAllHits, TotalHits originalBarHits) {
        ensureGreen(indexName);

        final TotalHits newAllHits = client().prepareSearch(indexName).get().getHits().getTotalHits();
        final TotalHits newBarHits = client().prepareSearch(indexName)
            .setQuery(matchQuery("foo", "bar")).get().getHits().getTotalHits();

        logger.info("--> [{}] in total, of which [{}] match the query", newAllHits, newBarHits);

        assertThat(newAllHits, equalTo(originalAllHits));
        assertThat(newBarHits, equalTo(originalBarHits));

        final RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries(indexName).get();
        for (List<RecoveryState> recoveryStates : recoveryResponse.shardRecoveryStates().values()) {
            for (RecoveryState recoveryState : recoveryStates) {
                logger.info("Checking {}[{}]", recoveryState.getShardId(), recoveryState.getPrimary() ? "p" : "r");
                assertThat(recoveryState.getIndex().recoveredFileCount(),
                    lessThanOrEqualTo(1)); // we make a new commit so we write a new `segments_n` file
            }
        }
    }
}
