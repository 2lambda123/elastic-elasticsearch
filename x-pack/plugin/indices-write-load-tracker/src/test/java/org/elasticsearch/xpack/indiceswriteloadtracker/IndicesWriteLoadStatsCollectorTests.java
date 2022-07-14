/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.indiceswriteloadtracker;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndicesWriteLoadStatsCollectorTests extends IndexShardTestCase {
    private LongSupplier relativeTimeClock = () -> 0L;

    @Override
    protected ThreadPool setUpThreadPool() {
        return new TestThreadPool(getClass().getName(), threadPoolSettings()) {
            @Override
            public long rawRelativeTimeInNanos() {
                return relativeTimeClock.getAsLong();
            }
        };
    }

    public void testRegularIndicesLoadIsNotTracked() throws Exception {
        final var fakeClock = new FakeClock(TimeValue.timeValueMillis(50));
        final var samplingFrequency = TimeValue.timeValueSeconds(1);
        try (var shardRef = createRegularIndexShard(fakeClock)) {
            final var shard = shardRef.shard();

            final var indicesWriteLoadStatsCollector = new IndicesWriteLoadStatsCollector(
                mock(ClusterService.class),
                fakeClock(samplingFrequency)
            ) {
                @Override
                String getParentDataStreamName(String indexName) {
                    assert false : "unexpected call";
                    return "";
                }
            };
            indicesWriteLoadStatsCollector.afterIndexShardStarted(shard);

            for (int i = 0; i < 10; i++) {
                fakeClock.setTickValue(samplingFrequency);
                indexDocs(shard, randomIntBetween(1, 10));
                indicesWriteLoadStatsCollector.collectWriteLoadStats();
                assertThat(shard.getTotalIndexingTimeInNanos(), is(greaterThan(0L)));
            }
            final var shardLoadHistogramSnapshots = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();
            assertThat(shardLoadHistogramSnapshots, is(empty()));
        }
    }

    public void testIndexingLoadTracking() throws Exception {
        final var fakeClock = new FakeClock(TimeValue.timeValueMillis(50));
        final var samplingFrequency = TimeValue.timeValueSeconds(1);
        try (var shardRef = createDataStreamShard(fakeClock)) {
            final var shard = shardRef.shard();

            final var clusterService = mock(ClusterService.class);
            final var clusterState = clusterStateWithShardDataStream(shard);
            when(clusterService.state()).thenReturn(clusterState);
            final var indicesWriteLoadStatsCollector = new IndicesWriteLoadStatsCollector(clusterService, fakeClock(samplingFrequency));
            indicesWriteLoadStatsCollector.afterIndexShardStarted(shard);

            int numberOfProcessors = randomIntBetween(2, 64);
            int maxCPUsUsed = 0;
            long previousTotalIndexingTimeSample = 0;
            for (int i = 0; i < 60; i++) {
                if (randomBoolean()) {
                    // Simulate some long indexing operations taking a few CPUS
                    fakeClock.setTickValue(samplingFrequency);
                    int numDocs = randomIntBetween(1, numberOfProcessors);
                    indexDocs(shard, numDocs);
                    maxCPUsUsed = Math.max(maxCPUsUsed, numDocs);
                } else {
                    // Otherwise, simulate a few quick indexing ops
                    fakeClock.setTickValue(TimeValue.timeValueMillis(randomIntBetween(1, 20)));
                    indexDocs(shard, randomIntBetween(1, 20));
                }

                assertThat(shard.getTotalIndexingTimeInNanos(), is(greaterThan(previousTotalIndexingTimeSample)));
                previousTotalIndexingTimeSample = shard.getTotalIndexingTimeInNanos();

                indicesWriteLoadStatsCollector.collectWriteLoadStats();
            }

            {
                final var shardLoadHistogramSnapshots = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

                assertThat(shardLoadHistogramSnapshots, hasSize(1));
                final var shardWriteLoadHistogramSnapshot = shardLoadHistogramSnapshots.get(0);
                final var indexingLoadHistogramSnapshot = shardWriteLoadHistogramSnapshot.indexLoadHistogramSnapshot();
                assertThat(indexingLoadHistogramSnapshot.max(), is(closeTo(maxCPUsUsed, 0.5)));
                assertThat(indexingLoadHistogramSnapshot.p90(), is(lessThanOrEqualTo(indexingLoadHistogramSnapshot.max())));
                assertThat(indexingLoadHistogramSnapshot.p50(), is(lessThanOrEqualTo(indexingLoadHistogramSnapshot.p90())));
            }

            {
                // We didn't have any readings after the previous reset
                final var shardLoadHistogramSnapshots = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

                assertThat(shardLoadHistogramSnapshots, hasSize(1));
                final var shardWriteLoadHistogramSnapshot = shardLoadHistogramSnapshots.get(0);
                final var indexingLoadHistogramSnapshot = shardWriteLoadHistogramSnapshot.indexLoadHistogramSnapshot();
                assertThat(indexingLoadHistogramSnapshot.max(), is(equalTo(0.0)));
                assertThat(indexingLoadHistogramSnapshot.p90(), is(equalTo(0.0)));
                assertThat(indexingLoadHistogramSnapshot.p50(), is(equalTo(0.0)));
            }
        }
    }

    public void testRefreshLoadTracking() throws Exception {
        final var fakeClock = new FakeClock(TimeValue.timeValueMillis(50));
        final var samplingFrequency = TimeValue.timeValueSeconds(1);
        try (var shardRef = createDataStreamShard(fakeClock, NoMergePolicy.INSTANCE)) {
            final var shard = shardRef.shard();

            final var clusterService = mock(ClusterService.class);
            final var clusterState = clusterStateWithShardDataStream(shard);
            when(clusterService.state()).thenReturn(clusterState);
            final var indicesWriteLoadStatsCollector = new IndicesWriteLoadStatsCollector(clusterService, fakeClock(samplingFrequency));
            indicesWriteLoadStatsCollector.afterIndexShardStarted(shard);

            long previousTotalRefreshTimeSample = 0;
            for (int i = 0; i < 60; i++) {
                fakeClock.setTickValue(TimeValue.timeValueMillis(randomIntBetween(1, 20)));
                indexDocs(shard, randomIntBetween(1, 20));

                fakeClock.setTickValue(samplingFrequency);
                shard.refresh("test");

                assertThat(shard.getTotalRefreshTimeInNanos(), is(greaterThan(previousTotalRefreshTimeSample)));
                previousTotalRefreshTimeSample = shard.getTotalRefreshTimeInNanos();

                indicesWriteLoadStatsCollector.collectWriteLoadStats();
            }

            final var shardLoadHistograms = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

            assertThat(shardLoadHistograms, hasSize(1));
            final var shardWriteLoadDistribution = shardLoadHistograms.get(0);

            assertThat(shardWriteLoadDistribution.indexLoadHistogramSnapshot().max(), is(greaterThan(0.0)));

            assertThat(shardWriteLoadDistribution.refreshLoadHistogramSnapshot().max(), is(closeTo(1.0, 0.5)));
            assertThat(
                shardWriteLoadDistribution.refreshLoadHistogramSnapshot().p90(),
                is(lessThanOrEqualTo(shardWriteLoadDistribution.refreshLoadHistogramSnapshot().max()))
            );
            assertThat(
                shardWriteLoadDistribution.refreshLoadHistogramSnapshot().p50(),
                is(lessThanOrEqualTo(shardWriteLoadDistribution.refreshLoadHistogramSnapshot().p90()))
            );
        }
    }

    public void testMergeLoadTracking() throws Exception {
        final var fakeClock = new FakeClock(TimeValue.timeValueMillis(50));
        final var samplingFrequency = TimeValue.timeValueSeconds(1);
        final var mergePolicy = new SwappableMergePolicy(NoMergePolicy.INSTANCE);
        try (var shardRef = createDataStreamShard(fakeClock, mergePolicy)) {
            final var shard = shardRef.shard();

            final var clusterState = clusterStateWithShardDataStream(shard);
            final var clusterService = mock(ClusterService.class);
            when(clusterService.state()).thenReturn(clusterState);
            final var indicesWriteLoadStatsCollector = new IndicesWriteLoadStatsCollector(clusterService, fakeClock(samplingFrequency));
            indicesWriteLoadStatsCollector.afterIndexShardStarted(shard);

            final int numTicks = 60;
            for (int i = 0; i < numTicks; i++) {
                fakeClock.setTickValue(TimeValue.timeValueMillis(randomIntBetween(1, 20)));
                indexDocs(shard, randomIntBetween(1, 20));
                shard.refresh("test");

                indicesWriteLoadStatsCollector.collectWriteLoadStats();
            }
            assertThat(shard.getTotalMergeTimeInMillis(), is(equalTo(0L)));

            mergePolicy.swapDelegate(new TieredMergePolicy());

            // Now force-merge every second and every force-merge should take 1 second
            for (int i = 0; i < numTicks; i++) {
                fakeClock.setTickValue(TimeValue.timeValueSeconds(1));
                shard.forceMerge(new ForceMergeRequest().maxNumSegments(1));

                indicesWriteLoadStatsCollector.collectWriteLoadStats();
                indexDocs(shard, randomIntBetween(1, 10));
            }

            assertThat(shard.getTotalMergeTimeInMillis(), is(equalTo(TimeValue.timeValueSeconds(numTicks).millis())));

            final var shardLoadHistograms = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

            assertThat(shardLoadHistograms, hasSize(1));
            final var shardWriteLoadDistribution = shardLoadHistograms.get(0);

            assertThat(shardWriteLoadDistribution.indexLoadHistogramSnapshot().max(), is(greaterThan(0.0)));

            assertThat(shardWriteLoadDistribution.mergeLoadHistogramSnapshot().max(), is(closeTo(1.0, 0.1)));
        }
    }

    public void testDeleteTimeIsTracked() throws Exception {
        final var fakeClock = new FakeClock(TimeValue.timeValueMillis(50));
        final var samplingFrequency = TimeValue.timeValueSeconds(1);
        try (var shardRef = createDataStreamShard(fakeClock)) {
            final var shard = shardRef.shard();

            final var clusterState = clusterStateWithShardDataStream(shard);
            final var clusterService = mock(ClusterService.class);
            when(clusterService.state()).thenReturn(clusterState);
            final var indicesWriteLoadStatsCollector = new IndicesWriteLoadStatsCollector(clusterService, fakeClock(samplingFrequency));
            indicesWriteLoadStatsCollector.afterIndexShardStarted(shard);

            for (int i = 0; i < 60; i++) {
                // We want to ensure that we are only measuring delete time
                fakeClock.setTickValue(TimeValue.ZERO);
                final int numDocs = randomIntBetween(1, 20);
                final var docIds = indexDocs(shard, numDocs);

                fakeClock.setTickValue(TimeValue.timeValueMillis(randomIntBetween(1, 20)));
                deleteDocs(shard, docIds);

                indicesWriteLoadStatsCollector.collectWriteLoadStats();
            }

            assertThat(shard.getTotalDeleteTimeInNanos(), is(greaterThan(0L)));

            final var shardLoadHistograms = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

            assertThat(shardLoadHistograms, hasSize(1));
            final var shardWriteLoadDistribution = shardLoadHistograms.get(0);

            assertThat(shardWriteLoadDistribution.indexLoadHistogramSnapshot().max(), is(greaterThan(0.0)));
        }
    }

    public void testShardLoadDistributionInfoIsClearedAfterDeletion() throws Exception {
        final var fakeClock = new FakeClock(TimeValue.timeValueMillis(50));
        final var samplingFrequency = TimeValue.timeValueSeconds(1);
        try (var shardRef = createDataStreamShard(fakeClock)) {
            final var shard = shardRef.shard();

            final var clusterState = clusterStateWithShardDataStream(shard);
            final var clusterService = mock(ClusterService.class);
            when(clusterService.state()).thenReturn(clusterState);
            final var indicesWriteLoadStatsCollector = new IndicesWriteLoadStatsCollector(clusterService, fakeClock(samplingFrequency));
            indicesWriteLoadStatsCollector.afterIndexShardStarted(shard);

            for (int i = 0; i < 60; i++) {
                fakeClock.setTickValue(TimeValue.timeValueMillis(randomIntBetween(1, 20)));
                indexDocs(shard, randomIntBetween(1, 20));

                indicesWriteLoadStatsCollector.collectWriteLoadStats();
            }

            final var shardLoadHistograms = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

            assertThat(shardLoadHistograms, hasSize(1));
            final var shardWriteLoadDistribution = shardLoadHistograms.get(0);
            assertThat(shardWriteLoadDistribution.shardId(), is(equalTo(shard.shardId())));

            indicesWriteLoadStatsCollector.afterIndexShardClosed(shard.shardId(), shard, shard.indexSettings().getSettings());

            assertThat(indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset(), is(empty()));
        }
    }

    public void testRolledOverDataStreamIndicesAreRemovedAfterCollection() throws Exception {
        final var fakeClock = new FakeClock(TimeValue.timeValueMillis(50));
        final var samplingFrequency = TimeValue.timeValueSeconds(1);

        try (
            var firstDataStreamShardRef = createDataStreamShard(fakeClock);
            var rolledOverDataStreamShardRef = createDataStreamShard(fakeClock)
        ) {

            final var firstDataStreamShard = firstDataStreamShardRef.shard();
            final var firstDataStreamIndex = firstDataStreamShard.shardId().getIndex();

            final var rolledOverShard = rolledOverDataStreamShardRef.shard();
            final var rolledOverIndex = rolledOverShard.shardId().getIndex();

            final AtomicReference<List<IndexShard>> shardsProvider = new AtomicReference<>(List.of(firstDataStreamShard));
            final var clusterService = mock(ClusterService.class);
            final var clusterState = ClusterState.builder(new ClusterName("cluster"))
                .metadata(
                    Metadata.builder()
                        .put(getDataStream(firstDataStreamIndex))
                        .put(
                            IndexMetadata.builder(firstDataStreamIndex.getName())
                                .settings(settings(Version.CURRENT))
                                .numberOfShards(1)
                                .numberOfReplicas(0)
                                .build(),
                            false
                        )
                        .build()
                )
                .build();
            when(clusterService.state()).thenReturn(clusterState);

            final var indicesWriteLoadStatsCollector = new IndicesWriteLoadStatsCollector(clusterService, fakeClock(samplingFrequency));
            indicesWriteLoadStatsCollector.afterIndexShardStarted(firstDataStreamShard);

            for (int i = 0; i < 60; i++) {
                fakeClock.setTickValue(TimeValue.timeValueMillis(randomIntBetween(1, 20)));
                indexDocs(firstDataStreamShard, randomIntBetween(1, 20));

                indicesWriteLoadStatsCollector.collectWriteLoadStats();
            }

            final var shardLoadHistogramSnapshots = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();

            assertThat(shardLoadHistogramSnapshots, hasSize(1));
            final var shardWriteLoadHistogramSnapshot = shardLoadHistogramSnapshots.get(0);
            assertThat(shardWriteLoadHistogramSnapshot.shardId(), is(equalTo(firstDataStreamShard.shardId())));

            // take some new samples before rolling over
            for (int i = 0; i < 10; i++) {
                fakeClock.setTickValue(TimeValue.timeValueMillis(randomIntBetween(1, 20)));
                indexDocs(firstDataStreamShard, randomIntBetween(1, 20));

                indicesWriteLoadStatsCollector.collectWriteLoadStats();
            }

            final var clusterStateWithRolledOverDataStream = ClusterState.builder(clusterState)
                .metadata(
                    Metadata.builder(clusterState.metadata())
                        .put(getDataStream(firstDataStreamIndex, rolledOverIndex))
                        .put(
                            IndexMetadata.builder(rolledOverIndex.getName())
                                .settings(settings(Version.CURRENT))
                                .numberOfShards(1)
                                .numberOfReplicas(0)
                                .build(),
                            false
                        )
                        .build()
                )
                .build();
            when(clusterService.state()).thenReturn(clusterStateWithRolledOverDataStream);
            indicesWriteLoadStatsCollector.afterIndexShardStarted(rolledOverShard);

            for (int i = 0; i < 10; i++) {
                fakeClock.setTickValue(TimeValue.timeValueMillis(randomIntBetween(1, 20)));
                indexDocs(rolledOverShard, randomIntBetween(1, 20));

                indicesWriteLoadStatsCollector.collectWriteLoadStats();
            }

            final var writeLoadHistogramAfterRollOver = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();
            assertThat(writeLoadHistogramAfterRollOver, hasSize(2));
            assertThat(
                writeLoadHistogramAfterRollOver.stream().anyMatch(s -> s.shardId().getIndex().equals(firstDataStreamIndex)),
                is(true)
            );
            assertThat(writeLoadHistogramAfterRollOver.stream().anyMatch(s -> s.shardId().getIndex().equals(rolledOverIndex)), is(true));

            final var writeLoadAfterRolloverCleanup = indicesWriteLoadStatsCollector.getWriteLoadHistogramSnapshotsAndReset();
            assertThat(writeLoadAfterRolloverCleanup, hasSize(1));
            final var shardWriteLoadHistogramAfterRollover = writeLoadAfterRolloverCleanup.get(0);
            assertThat(shardWriteLoadHistogramAfterRollover.shardId(), is(equalTo(rolledOverShard.shardId())));
        }
    }

    private ClusterState clusterStateWithShardDataStream(IndexShard shard) {
        final var index = shard.shardId().getIndex();
        return ClusterState.builder(new ClusterName("cluster"))
            .metadata(
                Metadata.builder()
                    .put(getDataStream(index))
                    .put(
                        IndexMetadata.builder(index.getName())
                            .settings(settings(Version.CURRENT))
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                            .build(),
                        false
                    )
                    .build()
            )
            .build();
    }

    private DataStream getDataStream(Index... indices) {
        return new DataStream(
            "datastream",
            Arrays.stream(indices).toList(),
            1,
            Collections.emptyMap(),
            false,
            false,
            false,
            false,
            IndexMode.STANDARD
        );
    }

    record ShardRef(IndexShard shard) implements AutoCloseable {
        @Override
        public void close() throws IOException {
            IOUtils.close(shard.store(), () -> shard.close("test", false));
        }
    }

    private ShardRef createDataStreamShard(LongSupplier relativeTimeClock) throws Exception {
        return createShard(relativeTimeClock, new TieredMergePolicy(), true);
    }

    private ShardRef createRegularIndexShard(LongSupplier relativeTimeClock) throws Exception {
        return createShard(relativeTimeClock, new TieredMergePolicy(), false);
    }

    private ShardRef createDataStreamShard(LongSupplier relativeTimeClock, MergePolicy mergePolicy) throws Exception {
        return createShard(relativeTimeClock, mergePolicy, true);
    }

    private ShardRef createShard(LongSupplier relativeTimeClock, MergePolicy mergePolicy, boolean createAsDataStream) throws Exception {
        final var shardId = new ShardId(new Index(randomAlphaOfLength(10), "_na_"), 0);
        final RecoverySource recoverySource = RecoverySource.EmptyStoreRecoverySource.INSTANCE;
        final ShardRouting shardRouting = TestShardRouting.newShardRouting(
            shardId,
            randomAlphaOfLength(10),
            true,
            ShardRoutingState.INITIALIZING,
            recoverySource
        );
        final var indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        final var indexMetadata = IndexMetadata.builder(shardRouting.getIndexName()).settings(indexSettings).primaryTerm(0, primaryTerm);

        if (createAsDataStream) {
            indexMetadata.putMapping("""
                {
                    "_doc": {
                        "properties": {
                            "@timestamp": {
                                "type": "date"
                            }
                        },
                        "_data_stream_timestamp": {
                            "enabled": true
                        }
                    }
                }
                """);
        } else {
            indexMetadata.putMapping("""
                {
                    "_doc": {
                        "properties": {
                            "@timestamp": {
                                "type": "date"
                            }
                        }
                    }
                }
                """);
        }
        final var shard = newShard(shardRouting, indexMetadata.build(), null, config -> {
            final EngineConfig configWithMergePolicy = new EngineConfig(
                config.getShardId(),
                config.getThreadPool(),
                config.getIndexSettings(),
                config.getWarmer(),
                config.getStore(),
                mergePolicy,
                config.getAnalyzer(),
                config.getSimilarity(),
                new CodecService(null),
                config.getEventListener(),
                config.getQueryCache(),
                config.getQueryCachingPolicy(),
                config.getTranslogConfig(),
                config.getFlushMergesAfter(),
                config.getExternalRefreshListener(),
                config.getInternalRefreshListener(),
                config.getIndexSort(),
                config.getCircuitBreakerService(),
                config.getGlobalCheckpointSupplier(),
                config.retentionLeasesSupplier(),
                config.getPrimaryTermSupplier(),
                IndexModule.DEFAULT_SNAPSHOT_COMMIT_SUPPLIER,
                config.getLeafSorter()
            );
            return new InternalEngine(configWithMergePolicy) {
                @Override
                public boolean ensureTranslogSynced(Stream<Translog.Location> locations) throws IOException {
                    return super.ensureTranslogSynced(locations);
                }
            };
        }, () -> {}, RetentionLeaseSyncer.EMPTY);
        recoverShardFromStore(shard);
        setUpRelativeTimeClock(relativeTimeClock);
        assertThat(shard.isDataStreamIndex(), is(createAsDataStream));
        return new ShardRef(shard);
    }

    private List<String> indexDocs(IndexShard shard, int numDocs) throws Exception {
        final List<String> docIds = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            final var id = UUIDs.randomBase64UUID();
            final Engine.IndexResult result = shard.applyIndexOperationOnPrimary(
                Versions.MATCH_ANY,
                VersionType.INTERNAL,
                new SourceToParse(id, new BytesArray("{\"@timestamp\": 123456}"), XContentType.JSON),
                UNASSIGNED_SEQ_NO,
                0,
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                false
            );
            assertThat(result.getResultType(), is(equalTo(Engine.Result.Type.SUCCESS)));
            docIds.add(id);
        }
        return docIds;
    }

    private void deleteDocs(IndexShard shard, List<String> docIds) throws Exception {
        for (String docId : docIds) {
            final var result = shard.applyDeleteOperationOnPrimary(
                1,
                docId,
                VersionType.INTERNAL,
                UNASSIGNED_SEQ_NO,
                UNASSIGNED_PRIMARY_TERM
            );
            assertThat(result.getResultType(), is(equalTo(Engine.Result.Type.SUCCESS)));
            assertThat(result.getTook(), is(greaterThan(0L)));
        }
    }

    /**
    * Creates a fake clock that advances the given {@param tickTime} in every
    * call to the clock
    */
    private FakeClock fakeClock(TimeValue tickTime) {
        return new FakeClock(tickTime);
    }

    private void setUpRelativeTimeClock(LongSupplier clock) {
        relativeTimeClock = clock;
    }

    static class FakeClock implements LongSupplier {
        private final AtomicLong clock = new AtomicLong();
        private TimeValue tickTime;

        FakeClock(TimeValue tickValue) {
            this.tickTime = tickValue;
        }

        @Override
        public long getAsLong() {
            return clock.getAndAdd(tickTime.nanos());
        }

        void setTickValue(TimeValue tickTime) {
            this.tickTime = tickTime;
        }
    }

    private static final class SwappableMergePolicy extends MergePolicy {
        private volatile MergePolicy delegate;

        SwappableMergePolicy(MergePolicy delegate) {
            this.delegate = delegate;
        }

        void swapDelegate(MergePolicy delegate) {
            this.delegate = delegate;
        }

        @Override
        public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
            throws IOException {
            return delegate.findMerges(mergeTrigger, segmentInfos, mergeContext);
        }

        @Override
        public MergeSpecification findForcedMerges(
            SegmentInfos segmentInfos,
            int maxSegmentCount,
            Map<SegmentCommitInfo, Boolean> segmentsToMerge,
            MergeContext mergeContext
        ) throws IOException {
            return delegate.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext);
        }

        @Override
        public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException {
            return delegate.findForcedDeletesMerges(segmentInfos, mergeContext);
        }

        @Override
        public MergeSpecification findFullFlushMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
            throws IOException {
            return delegate.findFullFlushMerges(mergeTrigger, segmentInfos, mergeContext);
        }

        @Override
        public boolean useCompoundFile(SegmentInfos infos, SegmentCommitInfo mergedInfo, MergeContext mergeContext) throws IOException {
            return delegate.useCompoundFile(infos, mergedInfo, mergeContext);
        }

        @Override
        public long size(SegmentCommitInfo info, MergePolicy.MergeContext mergeContext) throws IOException {
            return Long.MAX_VALUE;
        }

        @Override
        public double getNoCFSRatio() {
            return delegate.getNoCFSRatio();
        }

        @Override
        public void setNoCFSRatio(double noCFSRatio) {
            delegate.setNoCFSRatio(noCFSRatio);
        }

        @Override
        public double getMaxCFSSegmentSizeMB() {
            return delegate.getMaxCFSSegmentSizeMB();
        }

        @Override
        public void setMaxCFSSegmentSizeMB(double v) {
            delegate.setMaxCFSSegmentSizeMB(v);
        }

        @Override
        public boolean keepFullyDeletedSegment(IOSupplier<CodecReader> readerIOSupplier) throws IOException {
            return delegate.keepFullyDeletedSegment(readerIOSupplier);
        }

        @Override
        public int numDeletesToMerge(SegmentCommitInfo info, int delCount, IOSupplier<CodecReader> readerSupplier) throws IOException {
            return delegate.numDeletesToMerge(info, delCount, readerSupplier);
        }
    }
}
