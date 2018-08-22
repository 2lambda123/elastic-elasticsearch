/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ShardFollowNodeTaskStatusTests extends AbstractSerializingTestCase<ShardFollowNodeTask.Status> {

    @Override
    protected ShardFollowNodeTask.Status doParseInstance(XContentParser parser) throws IOException {
        return ShardFollowNodeTask.Status.fromXContent(parser);
    }

    @Override
    protected ShardFollowNodeTask.Status createTestInstance() {
        // if you change this constructor, reflect the changes in the hand-written assertions below
        return new ShardFollowNodeTask.Status(
                randomInt(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomReadExceptions());
    }

    @Override
    protected void assertEqualInstances(final ShardFollowNodeTask.Status expectedInstance, final ShardFollowNodeTask.Status newInstance) {
        assertNotSame(expectedInstance, newInstance);
        assertThat(newInstance.getShardId(), equalTo(expectedInstance.getShardId()));
        assertThat(newInstance.leaderGlobalCheckpoint(), equalTo(expectedInstance.leaderGlobalCheckpoint()));
        assertThat(newInstance.leaderMaxSeqNo(), equalTo(expectedInstance.leaderMaxSeqNo()));
        assertThat(newInstance.followerGlobalCheckpoint(), equalTo(expectedInstance.followerGlobalCheckpoint()));
        assertThat(newInstance.lastRequestedSeqNo(), equalTo(expectedInstance.lastRequestedSeqNo()));
        assertThat(newInstance.numberOfConcurrentReads(), equalTo(expectedInstance.numberOfConcurrentReads()));
        assertThat(newInstance.numberOfConcurrentWrites(), equalTo(expectedInstance.numberOfConcurrentWrites()));
        assertThat(newInstance.numberOfQueuedWrites(), equalTo(expectedInstance.numberOfQueuedWrites()));
        assertThat(newInstance.indexMetadataVersion(), equalTo(expectedInstance.indexMetadataVersion()));
        assertThat(newInstance.totalFetchTimeMillis(), equalTo(expectedInstance.totalFetchTimeMillis()));
        assertThat(newInstance.numberOfSuccessfulFetches(), equalTo(expectedInstance.numberOfSuccessfulFetches()));
        assertThat(newInstance.numberOfFailedFetches(), equalTo(expectedInstance.numberOfFailedFetches()));
        assertThat(newInstance.operationsReceived(), equalTo(expectedInstance.operationsReceived()));
        assertThat(newInstance.totalTransferredBytes(), equalTo(expectedInstance.totalTransferredBytes()));
        assertThat(newInstance.totalIndexTimeMillis(), equalTo(expectedInstance.totalIndexTimeMillis()));
        assertThat(newInstance.numberOfSuccessfulBulkOperations(), equalTo(expectedInstance.numberOfSuccessfulBulkOperations()));
        assertThat(newInstance.numberOfFailedBulkOperations(), equalTo(expectedInstance.numberOfFailedBulkOperations()));
        assertThat(newInstance.numberOfOperationsIndexed(), equalTo(expectedInstance.numberOfOperationsIndexed()));
        assertThat(newInstance.fetchExceptions().size(), equalTo(expectedInstance.fetchExceptions().size()));
        assertThat(newInstance.fetchExceptions().keySet(), equalTo(expectedInstance.fetchExceptions().keySet()));
        for (final Map.Entry<Long, ElasticsearchException> entry : newInstance.fetchExceptions().entrySet()) {
            // x-content loses the exception
            final ElasticsearchException expected = expectedInstance.fetchExceptions().get(entry.getKey());
            assertThat(entry.getValue().getMessage(), containsString(expected.getMessage()));
            assertNotNull(entry.getValue().getCause());
            assertThat(
                    entry.getValue().getCause(),
                    anyOf(instanceOf(ElasticsearchException.class), instanceOf(IllegalStateException.class)));
            assertThat(entry.getValue().getCause().getMessage(), containsString(expected.getCause().getMessage()));
        }
    }

    @Override
    protected boolean assertToXContentEquivalence() {
        return false;
    }

    private NavigableMap<Long, ElasticsearchException> randomReadExceptions() {
        final int count = randomIntBetween(0, 16);
        final NavigableMap<Long, ElasticsearchException> readExceptions = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            readExceptions.put(randomNonNegativeLong(), new ElasticsearchException(new IllegalStateException("index [" + i + "]")));
        }
        return readExceptions;
    }

    @Override
    protected Writeable.Reader<ShardFollowNodeTask.Status> instanceReader() {
        return ShardFollowNodeTask.Status::new;
    }

}
