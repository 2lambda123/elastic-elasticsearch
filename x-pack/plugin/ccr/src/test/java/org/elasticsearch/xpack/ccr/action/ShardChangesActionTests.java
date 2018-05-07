/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotStartedException;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ShardChangesActionTests extends ESSingleNodeTestCase {

    public void testGetOperationsBetween() throws Exception {
        final Settings settings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.translog.generation_threshold_size", new ByteSizeValue(randomIntBetween(8, 64), ByteSizeUnit.KB))
                .build();
        final IndexService indexService = createIndex("index", settings);
        IndexMetaData indexMetaData = indexService.getMetaData();

        final int numWrites = randomIntBetween(2, 8192);
        for (int i = 0; i < numWrites; i++) {
            client().prepareIndex("index", "doc", Integer.toString(i)).setSource("{}", XContentType.JSON).get();
        }

        // A number of times, get operations within a range that exists:
        int iters = randomIntBetween(8, 32);
        IndexShard indexShard = indexService.getShard(0);
        for (int iter = 0; iter < iters; iter++) {
            int min = randomIntBetween(0, numWrites - 1);
            int max = randomIntBetween(min, numWrites - 1);

            final Translog.Operation[] operations = ShardChangesAction.getOperationsBetween(indexShard, min, max, Long.MAX_VALUE);
            /*
             * We are not guaranteed that operations are returned to us in order they are in the translog (if our read crosses multiple
             * generations) so the best we can assert is that we see the expected operations.
             */
            final Set<Long> seenSeqNos = Arrays.stream(operations).map(Translog.Operation::seqNo).collect(Collectors.toSet());
            final Set<Long> expectedSeqNos = LongStream.range(min, max + 1).boxed().collect(Collectors.toSet());
            assertThat(seenSeqNos, equalTo(expectedSeqNos));
        }

        // get operations for a range no operations exists:
        Exception e = expectThrows(IllegalStateException.class,
                () -> ShardChangesAction.getOperationsBetween(indexShard, numWrites, numWrites + 1, Long.MAX_VALUE));
        assertThat(e.getMessage(), containsString("Not all operations between min_seq_no [" + numWrites + "] and max_seq_no [" +
                (numWrites + 1) +"] found, tracker checkpoint ["));

        // get operations for a range some operations do not exist:
        e = expectThrows(IllegalStateException.class,
                () -> ShardChangesAction.getOperationsBetween(indexShard, numWrites  - 10, numWrites + 10, Long.MAX_VALUE));
        assertThat(e.getMessage(), containsString("Not all operations between min_seq_no [" + (numWrites - 10) + "] and max_seq_no [" +
                (numWrites + 10) +"] found, tracker checkpoint ["));
    }

    public void testGetOperationsBetweenWhenShardNotStarted() throws Exception {
        IndexMetaData indexMetaData = IndexMetaData.builder("index")
                .settings(Settings.builder()
                        .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                        .build())
                .build();
        IndexShard indexShard = Mockito.mock(IndexShard.class);

        ShardRouting shardRouting = TestShardRouting.newShardRouting("index", 0, "_node_id", true, ShardRoutingState.INITIALIZING);
        Mockito.when(indexShard.routingEntry()).thenReturn(shardRouting);
        expectThrows(IndexShardNotStartedException.class, () -> ShardChangesAction.getOperationsBetween(indexShard, 0, 1, Long.MAX_VALUE));
    }

    public void testGetOperationsBetweenExceedByteLimit() throws Exception {
        final Settings settings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .build();
        final IndexService indexService = createIndex("index", settings);

        final long numWrites = 32;
        for (int i = 0; i < numWrites; i++) {
            client().prepareIndex("index", "doc", Integer.toString(i)).setSource("{}", XContentType.JSON).get();
        }

        final IndexShard indexShard = indexService.getShard(0);
        final Translog.Operation[] operations = ShardChangesAction.getOperationsBetween(indexShard, 0, numWrites - 1, 256);
        assertThat(operations.length, equalTo(12));
        assertThat(operations[0].seqNo(), equalTo(0L));
        assertThat(operations[1].seqNo(), equalTo(1L));
        assertThat(operations[2].seqNo(), equalTo(2L));
        assertThat(operations[3].seqNo(), equalTo(3L));
        assertThat(operations[4].seqNo(), equalTo(4L));
        assertThat(operations[5].seqNo(), equalTo(5L));
        assertThat(operations[6].seqNo(), equalTo(6L));
        assertThat(operations[7].seqNo(), equalTo(7L));
        assertThat(operations[8].seqNo(), equalTo(8L));
        assertThat(operations[9].seqNo(), equalTo(9L));
        assertThat(operations[10].seqNo(), equalTo(10L));
        assertThat(operations[11].seqNo(), equalTo(11L));
    }

}
