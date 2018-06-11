/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockHttpTransport;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.xpack.ccr.action.CreateAndFollowIndexAction;
import org.elasticsearch.xpack.ccr.action.FollowIndexAction;
import org.elasticsearch.xpack.ccr.action.ShardChangesAction;
import org.elasticsearch.xpack.ccr.action.ShardFollowNodeTask;
import org.elasticsearch.xpack.ccr.action.ShardFollowTask;
import org.elasticsearch.xpack.ccr.action.UnfollowIndexAction;
import org.elasticsearch.xpack.core.XPackSettings;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Objects;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, transportClientRatio = 0)
public class ShardChangesIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal)  {
        Settings.Builder newSettings = Settings.builder();
        newSettings.put(super.nodeSettings(nodeOrdinal));
        newSettings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        newSettings.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
        newSettings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        newSettings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        newSettings.put(XPackSettings.LOGSTASH_ENABLED.getKey(), false);
        return newSettings.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        return Arrays.asList(TestSeedPlugin.class, TestZenDiscovery.TestPlugin.class, MockHttpTransport.TestPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCcr.class, CommonAnalysisPlugin.class);
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    // this emulates what the CCR persistent task will do for pulling
    public void testGetOperationsBasedOnGlobalSequenceId() throws Exception {
        client().admin().indices().prepareCreate("index")
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .get();

        client().prepareIndex("index", "doc", "1").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("index", "doc", "2").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("index", "doc", "3").setSource("{}", XContentType.JSON).get();

        ShardStats shardStats = client().admin().indices().prepareStats("index").get().getIndex("index").getShards()[0];
        long globalCheckPoint = shardStats.getSeqNoStats().getGlobalCheckpoint();
        assertThat(globalCheckPoint, equalTo(2L));

        ShardChangesAction.Request request = new ShardChangesAction.Request(shardStats.getShardRouting().shardId());
        request.setMinSeqNo(0L);
        request.setMaxSeqNo(globalCheckPoint);
        ShardChangesAction.Response response = client().execute(ShardChangesAction.INSTANCE, request).get();
        assertThat(response.getOperations().length, equalTo(3));
        Translog.Index operation = (Translog.Index) response.getOperations()[0];
        assertThat(operation.seqNo(), equalTo(0L));
        assertThat(operation.id(), equalTo("1"));

        operation = (Translog.Index) response.getOperations()[1];
        assertThat(operation.seqNo(), equalTo(1L));
        assertThat(operation.id(), equalTo("2"));

        operation = (Translog.Index) response.getOperations()[2];
        assertThat(operation.seqNo(), equalTo(2L));
        assertThat(operation.id(), equalTo("3"));

        client().prepareIndex("index", "doc", "3").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("index", "doc", "4").setSource("{}", XContentType.JSON).get();
        client().prepareIndex("index", "doc", "5").setSource("{}", XContentType.JSON).get();

        shardStats = client().admin().indices().prepareStats("index").get().getIndex("index").getShards()[0];
        globalCheckPoint = shardStats.getSeqNoStats().getGlobalCheckpoint();
        assertThat(globalCheckPoint, equalTo(5L));

        request = new ShardChangesAction.Request(shardStats.getShardRouting().shardId());
        request.setMinSeqNo(3L);
        request.setMaxSeqNo(globalCheckPoint);
        response = client().execute(ShardChangesAction.INSTANCE, request).get();
        assertThat(response.getOperations().length, equalTo(3));
        operation = (Translog.Index) response.getOperations()[0];
        assertThat(operation.seqNo(), equalTo(3L));
        assertThat(operation.id(), equalTo("3"));

        operation = (Translog.Index) response.getOperations()[1];
        assertThat(operation.seqNo(), equalTo(4L));
        assertThat(operation.id(), equalTo("4"));

        operation = (Translog.Index) response.getOperations()[2];
        assertThat(operation.seqNo(), equalTo(5L));
        assertThat(operation.id(), equalTo("5"));
    }

    public void testFollowIndex() throws Exception {
        final int numberOfPrimaryShards = randomIntBetween(1, 3);
        final String leaderIndexSettings = getIndexSettings(numberOfPrimaryShards,
            singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(client().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureYellow("index1");

        final FollowIndexAction.Request followRequest = new FollowIndexAction.Request();
        followRequest.setLeaderIndex("index1");
        followRequest.setFollowIndex("index2");

        final CreateAndFollowIndexAction.Request createAndFollowRequest = new CreateAndFollowIndexAction.Request();
        createAndFollowRequest.setFollowRequest(followRequest);
        client().execute(CreateAndFollowIndexAction.INSTANCE, createAndFollowRequest).get();

        final int firstBatchNumDocs = randomIntBetween(2, 64);
        logger.info("Indexing [{}] docs as first batch", firstBatchNumDocs);
        for (int i = 0; i < firstBatchNumDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i);
            client().prepareIndex("index1", "doc", Integer.toString(i)).setSource(source, XContentType.JSON).get();
        }

        final Map<ShardId, Long> firstBatchNumDocsPerShard = new HashMap<>();
        final ShardStats[] firstBatchShardStats = client().admin().indices().prepareStats("index1").get().getIndex("index1").getShards();
        for (final ShardStats shardStats : firstBatchShardStats) {
            if (shardStats.getShardRouting().primary()) {
                long value = shardStats.getStats().getIndexing().getTotal().getIndexCount() - 1;
                firstBatchNumDocsPerShard.put(shardStats.getShardRouting().shardId(), value);
            }
        }

        assertBusy(assertTask(numberOfPrimaryShards, firstBatchNumDocsPerShard));

        for (int i = 0; i < firstBatchNumDocs; i++) {
            assertBusy(assertExpectedDocumentRunnable(i));
        }

        unfollowIndex("index2");
        client().execute(FollowIndexAction.INSTANCE, followRequest).get();
        final int secondBatchNumDocs = randomIntBetween(2, 64);
        logger.info("Indexing [{}] docs as second batch", secondBatchNumDocs);
        for (int i = firstBatchNumDocs; i < firstBatchNumDocs + secondBatchNumDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i);
            client().prepareIndex("index1", "doc", Integer.toString(i)).setSource(source, XContentType.JSON).get();
        }

        final Map<ShardId, Long> secondBatchNumDocsPerShard = new HashMap<>();
        final ShardStats[] secondBatchShardStats = client().admin().indices().prepareStats("index1").get().getIndex("index1").getShards();
        for (final ShardStats shardStats : secondBatchShardStats) {
            if (shardStats.getShardRouting().primary()) {
                final long value = shardStats.getStats().getIndexing().getTotal().getIndexCount() - 1;
                secondBatchNumDocsPerShard.put(shardStats.getShardRouting().shardId(), value);
            }
        }

        assertBusy(assertTask(numberOfPrimaryShards, secondBatchNumDocsPerShard));

        for (int i = firstBatchNumDocs; i < firstBatchNumDocs + secondBatchNumDocs; i++) {
            assertBusy(assertExpectedDocumentRunnable(i));
        }
        unfollowIndex("index2");
    }

    public void testSyncMappings() throws Exception {
        final String leaderIndexSettings = getIndexSettings(2,
            singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(client().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));
        ensureYellow("index1");
        final FollowIndexAction.Request followRequest = new FollowIndexAction.Request();
        followRequest.setLeaderIndex("index1");
        followRequest.setFollowIndex("index2");

        final CreateAndFollowIndexAction.Request createAndFollowRequest = new CreateAndFollowIndexAction.Request();
        createAndFollowRequest.setFollowRequest(followRequest);
        client().execute(CreateAndFollowIndexAction.INSTANCE, createAndFollowRequest).get();

        final long firstBatchNumDocs = randomIntBetween(2, 64);
        for (long i = 0; i < firstBatchNumDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"f\":%d}", i);
            client().prepareIndex("index1", "doc", Long.toString(i)).setSource(source, XContentType.JSON).get();
        }

        assertBusy(() -> assertThat(client().prepareSearch("index2").get().getHits().totalHits, equalTo(firstBatchNumDocs)));
        MappingMetaData mappingMetaData = client().admin().indices().prepareGetMappings("index2").get().getMappings()
                .get("index2").get("doc");
        assertThat(XContentMapValues.extractValue("properties.f.type", mappingMetaData.sourceAsMap()), equalTo("integer"));
        assertThat(XContentMapValues.extractValue("properties.k", mappingMetaData.sourceAsMap()), nullValue());

        final int secondBatchNumDocs = randomIntBetween(2, 64);
        for (long i = firstBatchNumDocs; i < firstBatchNumDocs + secondBatchNumDocs; i++) {
            final String source = String.format(Locale.ROOT, "{\"k\":%d}", i);
            client().prepareIndex("index1", "doc", Long.toString(i)).setSource(source, XContentType.JSON).get();
        }

        assertBusy(() -> assertThat(client().prepareSearch("index2").get().getHits().totalHits,
                equalTo(firstBatchNumDocs + secondBatchNumDocs)));
        mappingMetaData = client().admin().indices().prepareGetMappings("index2").get().getMappings()
                .get("index2").get("doc");
        assertThat(XContentMapValues.extractValue("properties.f.type", mappingMetaData.sourceAsMap()), equalTo("integer"));
        assertThat(XContentMapValues.extractValue("properties.k.type", mappingMetaData.sourceAsMap()), equalTo("long"));
        unfollowIndex("index2");
    }

    public void testFollowIndexAndCloseNode() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(3);
        String leaderIndexSettings = getIndexSettings(3, singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(client().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));

        String followerIndexSettings = getIndexSettings(3, singletonMap(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), "true"));
        assertAcked(client().admin().indices().prepareCreate("index2").setSource(followerIndexSettings, XContentType.JSON));
        ensureGreen("index1", "index2");

        AtomicBoolean run = new AtomicBoolean(true);
        Thread thread = new Thread(() -> {
            int counter = 0;
            while (run.get()) {
                final String source = String.format(Locale.ROOT, "{\"f\":%d}", counter++);
                try {
                    client().prepareIndex("index1", "doc")
                        .setSource(source, XContentType.JSON)
                        .setTimeout(TimeValue.timeValueSeconds(1))
                        .get();
                } catch (Exception e) {
                    logger.error("Error while indexing into leader index", e);
                }
            }
        });
        thread.start();

        final FollowIndexAction.Request followRequest = new FollowIndexAction.Request();
        followRequest.setLeaderIndex("index1");
        followRequest.setFollowIndex("index2");
        followRequest.setBatchSize(randomIntBetween(32, 2048));
        followRequest.setConcurrentProcessors(randomIntBetween(2, 10));
        client().execute(FollowIndexAction.INSTANCE, followRequest).get();

        long maxNumDocsReplicated = Math.min(3000, randomLongBetween(followRequest.getBatchSize(), followRequest.getBatchSize() * 10));
        long minNumDocsReplicated = maxNumDocsReplicated / 3L;
        logger.info("waiting for at least [{}] documents to be indexed and then stop a random data node", minNumDocsReplicated);
        awaitBusy(() -> {
            SearchRequest request = new SearchRequest("index2");
            request.source(new SearchSourceBuilder().size(0));
            SearchResponse response = client().search(request).actionGet();
            if (response.getHits().getTotalHits() >= minNumDocsReplicated) {
                try {
                    internalCluster().stopRandomNonMasterNode();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return true;
            } else {
                return false;
            }
        }, 30, TimeUnit.SECONDS);

        logger.info("waiting for at least [{}] documents to be indexed", maxNumDocsReplicated);
        awaitBusy(() -> {
            SearchRequest request = new SearchRequest("index2");
            request.source(new SearchSourceBuilder().size(0));
            SearchResponse response = client().search(request).actionGet();
            return response.getHits().getTotalHits() >= maxNumDocsReplicated;
        }, 30, TimeUnit.SECONDS);
        run.set(false);
        thread.join();

        refresh("index1");
        SearchRequest request1 = new SearchRequest("index1");
        request1.source(new SearchSourceBuilder().size(0));
        SearchResponse response1 = client().search(request1).actionGet();
        assertBusy(() -> {
            refresh("index2");
            SearchRequest request2 = new SearchRequest("index2");
            request2.source(new SearchSourceBuilder().size(0));
            SearchResponse response2 = client().search(request2).actionGet();
            assertThat(response2.getHits().getTotalHits(), equalTo(response1.getHits().getTotalHits()));
        });
        unfollowIndex("index2");
    }

    public void testFollowIndexWithNestedField() throws Exception {
        final String leaderIndexSettings =
            getIndexSettingsWithNestedMapping(1, singletonMap(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true"));
        assertAcked(client().admin().indices().prepareCreate("index1").setSource(leaderIndexSettings, XContentType.JSON));

        final String followerIndexSettings =
            getIndexSettingsWithNestedMapping(1, singletonMap(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), "true"));
        assertAcked(client().admin().indices().prepareCreate("index2").setSource(followerIndexSettings, XContentType.JSON));

        ensureYellow("index1", "index2");

        final FollowIndexAction.Request followRequest = new FollowIndexAction.Request();
        followRequest.setLeaderIndex("index1");
        followRequest.setFollowIndex("index2");
        client().execute(FollowIndexAction.INSTANCE, followRequest).get();

        final int numDocs = randomIntBetween(2, 64);
        for (int i = 0; i < numDocs; i++) {
            try (XContentBuilder builder = jsonBuilder()) {
                builder.startObject();
                builder.field("field", "value");
                builder.startArray("objects");
                {
                    builder.startObject();
                    builder.field("field", i);
                    builder.endObject();
                }
                builder.endArray();
                builder.endObject();
                client().prepareIndex("index1", "doc", Integer.toString(i)).setSource(builder).get();
            }
        }

        for (int i = 0; i < numDocs; i++) {
            int value = i;
            assertBusy(() -> {
                final GetResponse getResponse = client().prepareGet("index2", "doc", Integer.toString(value)).get();
                assertTrue(getResponse.isExists());
                assertTrue((getResponse.getSource().containsKey("field")));
                assertThat(XContentMapValues.extractValue("objects.field", getResponse.getSource()),
                    equalTo(Collections.singletonList(value)));
            });
        }

        final UnfollowIndexAction.Request unfollowRequest = new UnfollowIndexAction.Request();
        unfollowRequest.setFollowIndex("index2");
        client().execute(UnfollowIndexAction.INSTANCE, unfollowRequest).get();

        assertBusy(() -> {
            final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            final PersistentTasksCustomMetaData tasks = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
            assertThat(tasks.tasks().size(), equalTo(0));
        });
    }

    public void testUnfollowNonExistingIndex() {
        UnfollowIndexAction.Request unfollowRequest = new UnfollowIndexAction.Request();
        unfollowRequest.setFollowIndex("non-existing-index");
        expectThrows(IllegalArgumentException.class, () -> client().execute(UnfollowIndexAction.INSTANCE, unfollowRequest).actionGet());
    }

    public void testFollowNonExistentIndex() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test-leader").get());
        assertAcked(client().admin().indices().prepareCreate("test-follower").get());
        final FollowIndexAction.Request followRequest = new FollowIndexAction.Request();
        // Leader index does not exist.
        followRequest.setLeaderIndex("non-existent-leader");
        followRequest.setFollowIndex("test-follower");
        expectThrows(IllegalArgumentException.class, () -> client().execute(FollowIndexAction.INSTANCE, followRequest).actionGet());
        // Follower index does not exist.
        followRequest.setLeaderIndex("test-leader");
        followRequest.setFollowIndex("non-existent-follower");
        expectThrows(IllegalArgumentException.class, () -> client().execute(FollowIndexAction.INSTANCE, followRequest).actionGet());
        // Both indices do not exist.
        followRequest.setLeaderIndex("non-existent-leader");
        followRequest.setFollowIndex("non-existent-follower");
        expectThrows(IllegalArgumentException.class, () -> client().execute(FollowIndexAction.INSTANCE, followRequest).actionGet());
    }

    private CheckedRunnable<Exception> assertTask(final int numberOfPrimaryShards, final Map<ShardId, Long> numDocsPerShard) {
        return () -> {
            final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            final PersistentTasksCustomMetaData taskMetadata = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

            ListTasksRequest listTasksRequest = new ListTasksRequest();
            listTasksRequest.setDetailed(true);
            listTasksRequest.setActions(ShardFollowTask.NAME + "[c]");
            ListTasksResponse listTasksResponse = client().admin().cluster().listTasks(listTasksRequest).actionGet();
            assertThat(listTasksResponse.getNodeFailures().size(), equalTo(0));
            assertThat(listTasksResponse.getTaskFailures().size(), equalTo(0));

            List<TaskInfo> taskInfos = listTasksResponse.getTasks();
            assertThat(taskInfos.size(), equalTo(numberOfPrimaryShards));
            Collection<PersistentTasksCustomMetaData.PersistentTask<?>> shardFollowTasks =
                    taskMetadata.findTasks(ShardFollowTask.NAME, Objects::nonNull);
            for (PersistentTasksCustomMetaData.PersistentTask<?> shardFollowTask : shardFollowTasks) {
                final ShardFollowTask shardFollowTaskParams = (ShardFollowTask) shardFollowTask.getParams();
                TaskInfo taskInfo = null;
                String expectedId = "id=" + shardFollowTask.getId();
                for (TaskInfo info : taskInfos) {
                    if (expectedId.equals(info.getDescription())) {
                        taskInfo = info;
                        break;
                    }
                }
                assertThat(taskInfo, notNullValue());
                ShardFollowNodeTask.Status status = (ShardFollowNodeTask.Status) taskInfo.getStatus();
                assertThat(status, notNullValue());
                assertThat(
                        status.getProcessedGlobalCheckpoint(),
                        equalTo(numDocsPerShard.get(shardFollowTaskParams.getLeaderShardId())));
            }
        };
    }

    private void unfollowIndex(String index) throws Exception {
        final UnfollowIndexAction.Request unfollowRequest = new UnfollowIndexAction.Request();
        unfollowRequest.setFollowIndex(index);
        client().execute(UnfollowIndexAction.INSTANCE, unfollowRequest).get();
        ListTasksResponse[] holder = new ListTasksResponse[1];
        try {
            assertBusy(() -> {
                final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
                final PersistentTasksCustomMetaData tasks = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
                assertThat(tasks.tasks().size(), equalTo(0));

                ListTasksRequest listTasksRequest = new ListTasksRequest();
                listTasksRequest.setDetailed(true);
                ListTasksResponse listTasksResponse = holder[0] = client().admin().cluster().listTasks(listTasksRequest).get();
                int numNodeTasks = 0;
                for (TaskInfo taskInfo : listTasksResponse.getTasks()) {
                    if (taskInfo.getAction().startsWith(ListTasksAction.NAME) == false) {
                        numNodeTasks++;
                    }
                }
                assertThat(numNodeTasks, equalTo(0));
            });
        } catch (AssertionError ae) {
            logger.error("List tasks response contains unexpected tasks: {}", holder[0]);
            throw ae;
        }
    }

    private CheckedRunnable<Exception> assertExpectedDocumentRunnable(final int value) {
        return () -> {
            final GetResponse getResponse = client().prepareGet("index2", "doc", Integer.toString(value)).get();
            assertTrue("Doc with id [" + value + "] is missing", getResponse.isExists());
            assertTrue((getResponse.getSource().containsKey("f")));
            assertThat(getResponse.getSource().get("f"), equalTo(value));
        };
    }

    private String getIndexSettings(final int numberOfPrimaryShards, final Map<String, String> additionalIndexSettings) throws IOException {
        final String settings;
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.startObject("settings");
                {
                    builder.field("index.number_of_shards", numberOfPrimaryShards);
                    builder.field("index.number_of_replicas", 1);
                    for (final Map.Entry<String, String> additionalSetting : additionalIndexSettings.entrySet()) {
                        builder.field(additionalSetting.getKey(), additionalSetting.getValue());
                    }
                }
                builder.endObject();
                builder.startObject("mappings");
                {
                    builder.startObject("doc");
                    {
                        builder.startObject("properties");
                        {
                            builder.startObject("f");
                            {
                                builder.field("type", "integer");
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            settings = BytesReference.bytes(builder).utf8ToString();
        }
        return settings;
    }

    private String getIndexSettingsWithNestedMapping(final int numberOfPrimaryShards,
                                                     final Map<String, String> additionalIndexSettings) throws IOException {
        final String settings;
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.startObject("settings");
                {
                    builder.field("index.number_of_shards", numberOfPrimaryShards);
                    for (final Map.Entry<String, String> additionalSetting : additionalIndexSettings.entrySet()) {
                        builder.field(additionalSetting.getKey(), additionalSetting.getValue());
                    }
                }
                builder.endObject();
                builder.startObject("mappings");
                {
                    builder.startObject("doc");
                    {
                        builder.startObject("properties");
                        {
                            builder.startObject("objects");
                            {
                                builder.field("type", "nested");
                                builder.startObject("properties");
                                {
                                    builder.startObject("field");
                                    {
                                        builder.field("type", "long");
                                    }
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                            builder.startObject("field");
                            {
                                builder.field("type", "keyword");
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            settings = BytesReference.bytes(builder).utf8ToString();
        }
        return settings;
    }
}
