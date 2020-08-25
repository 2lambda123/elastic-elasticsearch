/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SearchableSnapshotsSystemIndicesIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestSystemIndexPlugin.class);
        return plugins;
    }

    public void testCannotMountSystemIndex() {
        final String systemIndexName = '.' + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        executeTest(systemIndexName, client());
    }

    public void testCannotMountSystemIndexWithDescriptor() {
        // TODO replace STACK_ORIGIN with searchable snapshot origin
        executeTest(TestSystemIndexPlugin.INDEX_NAME, new OriginSettingClient(client(), ClientHelper.STACK_ORIGIN));
    }

    private void executeTest(final String indexName, final Client client) {
        final boolean isHidden = randomBoolean();
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, isHidden).build())
        );

        final int nbDocs = scaledRandomIntBetween(0, 100);
        if (nbDocs > 0) {
            final BulkRequest bulkRequest = new BulkRequest();
            for (int i = 0; i < nbDocs; i++) {
                IndexRequest indexRequest = new IndexRequest(indexName);
                indexRequest.source("value", i);
                bulkRequest.add(indexRequest);
            }
            final BulkResponse bulkResponse = client.bulk(bulkRequest).actionGet();
            assertThat(bulkResponse.hasFailures(), is(false));
        }
        flushAndRefresh(indexName);
        assertHitCount(client.prepareSearch(indexName).get(), nbDocs);

        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createRepo(repositoryName);

        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final CreateSnapshotResponse snapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, snapshotName)
            .setIndices(indexName)
            .setWaitForCompletion(true)
            .get();

        final int numPrimaries = getNumShards(indexName).numPrimaries;
        assertThat(snapshotResponse.getSnapshotInfo().successfulShards(), equalTo(numPrimaries));
        assertThat(snapshotResponse.getSnapshotInfo().failedShards(), equalTo(0));

        if (randomBoolean()) {
            assertAcked(client.admin().indices().prepareClose(indexName));
        } else {
            assertAcked(client.admin().indices().prepareDelete(indexName));
        }

        final MountSearchableSnapshotRequest mountRequest = new MountSearchableSnapshotRequest(
            indexName,
            repositoryName,
            snapshotName,
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, randomBoolean()).build(),
            Strings.EMPTY_ARRAY,
            true
        );

        final InvalidIndexNameException exception = expectThrows(
            InvalidIndexNameException.class,
            () -> client.execute(MountSearchableSnapshotAction.INSTANCE, mountRequest).actionGet()
        );
        assertThat(exception.getIndex().getName(), equalTo(indexName));
        assertThat(exception.getMessage(), containsString("system indices cannot be mounted as searchable snapshots"));
    }

    public static class TestSystemIndexPlugin extends Plugin implements SystemIndexPlugin {

        static final String INDEX_NAME = ".test-system-index";

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(new SystemIndexDescriptor(INDEX_NAME, "System index for [" + getTestClass().getName() + ']'));
        }
    }
}
