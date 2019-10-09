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
package org.elasticsearch.action.search;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Transport;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.instanceOf;

public class FetchSearchPhaseTests extends ESTestCase {

    public void testShortcutQueryAndFetchOptimization() {
        SearchPhaseController controller = new SearchPhaseController(
            (b) -> new InternalAggregation.ReduceContext(BigArrays.NON_RECYCLING_INSTANCE, null, b));
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        ArraySearchPhaseResults<SearchPhaseResult> results = controller.newSearchPhaseResults(mockSearchPhaseContext.getRequest(), 1);
        boolean hasHits = randomBoolean();
        final int numHits;
        if (hasHits) {
            QuerySearchResult queryResult = new QuerySearchResult();
            queryResult.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {new ScoreDoc(42, 1.0F)}), 1.0F), new DocValueFormat[0]);
            queryResult.size(1);
            FetchSearchResult fetchResult = new FetchSearchResult();
            fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit(42)}, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0F));
            QueryFetchSearchResult fetchSearchResult = new QueryFetchSearchResult(queryResult, fetchResult);
            fetchSearchResult.setShardIndex(0);
            results.consumeResult(fetchSearchResult);
            numHits = 1;
        } else {
            numHits = 0;
        }

        FetchSearchPhase phase = new FetchSearchPhase(results, controller, mockSearchPhaseContext,
            (searchResponse, scrollId) -> new SearchPhase("test") {
            @Override
            public void run() {
                mockSearchPhaseContext.sendSearchResponse(searchResponse, null);
            }
        });
        assertEquals("fetch", phase.getName());
        phase.run();
        mockSearchPhaseContext.assertNoFailure();
        SearchResponse searchResponse = mockSearchPhaseContext.searchResponse.get();
        assertNotNull(searchResponse);
        assertEquals(numHits, searchResponse.getHits().getTotalHits().value);
        if (numHits != 0) {
            assertEquals(42, searchResponse.getHits().getAt(0).docId());
        }
        assertTrue(mockSearchPhaseContext.releasedSearchContexts.isEmpty());

        MainSearchTaskStatus status = mockSearchPhaseContext.getTask().getStatus();
        assertNull(status.getCurrentPhase());
        assertEquals(1, status.getCompletedPhases().size());
        MainSearchTaskStatus.PhaseInfo phaseInfo = status.getCompletedPhases().get(0);
        assertEquals("fetch", phaseInfo.getName());
        assertEquals(0, phaseInfo.getExpectedOps());
        assertEquals(0, phaseInfo.getProcessedShards().size());
    }

    public void testFetchTwoDocuments() {
        TaskInfo taskInfo = new TaskInfo(new TaskId("node", 1), "type", "action", null, null, -1, -1, true, null,
            Collections.emptyMap());
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        SearchPhaseController controller = new SearchPhaseController(
            (b) -> new InternalAggregation.ReduceContext(BigArrays.NON_RECYCLING_INSTANCE, null, b));
        ArraySearchPhaseResults<SearchPhaseResult> results = controller.newSearchPhaseResults(mockSearchPhaseContext.getRequest(), 2);
        int resultSetSize = randomIntBetween(2, 10);
        QuerySearchResult queryResult = new QuerySearchResult(123, new SearchShardTarget("node1", new ShardId("test", "na", 0),
            null, OriginalIndices.NONE));
        queryResult.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] {new ScoreDoc(42, 1.0F)}), 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize); // the size of the result set
        queryResult.setShardIndex(0);
        results.consumeResult(queryResult);

        queryResult = new QuerySearchResult(321, new SearchShardTarget("node2", new ShardId("test", "na", 1), null, OriginalIndices.NONE));
        queryResult.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] {new ScoreDoc(84, 2.0F)}), 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize);
        queryResult.setShardIndex(1);
        results.consumeResult(queryResult);

        mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null) {
            @Override
            public void sendExecuteFetch(Transport.Connection connection, ShardFetchSearchRequest request, SearchTask task,
                                         SearchActionListener<FetchSearchResult> listener) {
                FetchSearchResult fetchResult = new FetchSearchResult();
                fetchResult.setTaskInfo(taskInfo);
                if (request.id() == 321) {
                    fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit(84)},
                        new TotalHits(1, TotalHits.Relation.EQUAL_TO), 2.0F));
                } else {
                    assertEquals(123, request.id());
                    fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit(42)},
                        new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0F));
                }
                listener.onResponse(fetchResult);
            }
        };
        FetchSearchPhase phase = new FetchSearchPhase(results, controller, mockSearchPhaseContext,
            (searchResponse, scrollId) -> new SearchPhase("test") {
                @Override
                public void run() {
                    mockSearchPhaseContext.sendSearchResponse(searchResponse, null);
                }
            });
        assertEquals("fetch", phase.getName());
        phase.run();
        mockSearchPhaseContext.assertNoFailure();
        SearchResponse searchResponse = mockSearchPhaseContext.searchResponse.get();
        assertNotNull(searchResponse);
        assertEquals(2, searchResponse.getHits().getTotalHits().value);
        assertEquals(84, searchResponse.getHits().getAt(0).docId());
        assertEquals(42, searchResponse.getHits().getAt(1).docId());
        assertEquals(0, searchResponse.getFailedShards());
        assertEquals(2, searchResponse.getSuccessfulShards());
        assertTrue(mockSearchPhaseContext.releasedSearchContexts.isEmpty());

        MainSearchTaskStatus status = mockSearchPhaseContext.getTask().getStatus();
        assertEquals(1, status.getCompletedPhases().size());
        MainSearchTaskStatus.PhaseInfo phaseInfo = status.getCompletedPhases().get(0);
        assertEquals("fetch", phaseInfo.getName());
        assertNull(phaseInfo.getFailure());
        assertEquals(2, phaseInfo.getExpectedOps());
        assertEquals(2, phaseInfo.getProcessedShards().size());
        for (MainSearchTaskStatus.ShardInfo shard : phaseInfo.getProcessedShards()) {
            assertNull(shard.getFailure());
            assertSame(taskInfo, shard.getTaskInfo());
            assertEquals("test", shard.getSearchShardTarget().getIndex());
            if (shard.getSearchShardTarget().getShardId().id() == 0) {
                assertEquals("node1", shard.getSearchShardTarget().getNodeId());
            } else {
                assertEquals(1, shard.getSearchShardTarget().getShardId().id());
                assertEquals("node2", shard.getSearchShardTarget().getNodeId());
            }
        }
    }

    public void testFailFetchOneDoc() {
        TaskInfo taskInfo = new TaskInfo(new TaskId("node", 1), "type", "action", null, null, -1, -1, true, null,
            Collections.emptyMap());
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        SearchPhaseController controller = new SearchPhaseController(
            (b) -> new InternalAggregation.ReduceContext(BigArrays.NON_RECYCLING_INSTANCE, null, b));
        ArraySearchPhaseResults<SearchPhaseResult> results =
            controller.newSearchPhaseResults(mockSearchPhaseContext.getRequest(), 2);
        int resultSetSize = randomIntBetween(2, 10);
        QuerySearchResult queryResult = new QuerySearchResult(123, new SearchShardTarget("node1", new ShardId("test", "na", 0),
            null, OriginalIndices.NONE));
        queryResult.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] {new ScoreDoc(42, 1.0F)}), 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize); // the size of the result set
        queryResult.setShardIndex(0);
        results.consumeResult(queryResult);

        queryResult = new QuerySearchResult(321, new SearchShardTarget("node2", new ShardId("test", "na", 1), null, OriginalIndices.NONE));
        queryResult.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] {new ScoreDoc(84, 2.0F)}), 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize);
        queryResult.setShardIndex(1);
        results.consumeResult(queryResult);

        mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null) {
            @Override
            public void sendExecuteFetch(Transport.Connection connection, ShardFetchSearchRequest request, SearchTask task,
                                         SearchActionListener<FetchSearchResult> listener) {
                if (request.id() == 321) {
                    FetchSearchResult fetchResult = new FetchSearchResult();
                    fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit(84)},
                        new TotalHits(1, TotalHits.Relation.EQUAL_TO), 2.0F));
                    fetchResult.setTaskInfo(taskInfo);
                    listener.onResponse(fetchResult);
                } else {
                    listener.onFailure(new MockDirectoryWrapper.FakeIOException());
                }
            }
        };
        FetchSearchPhase phase = new FetchSearchPhase(results, controller, mockSearchPhaseContext,
            (searchResponse, scrollId) -> new SearchPhase("test") {
                @Override
                public void run() {
                    mockSearchPhaseContext.sendSearchResponse(searchResponse, null);
                }
            });
        assertEquals("fetch", phase.getName());
        phase.run();
        mockSearchPhaseContext.assertNoFailure();
        SearchResponse searchResponse = mockSearchPhaseContext.searchResponse.get();
        assertNotNull(searchResponse);
        assertEquals(2, searchResponse.getHits().getTotalHits().value);
        assertEquals(84, searchResponse.getHits().getAt(0).docId());
        assertEquals(1, searchResponse.getFailedShards());
        assertEquals(1, searchResponse.getSuccessfulShards());
        assertEquals(1, searchResponse.getShardFailures().length);
        assertTrue(searchResponse.getShardFailures()[0].getCause() instanceof MockDirectoryWrapper.FakeIOException);
        assertEquals(1, mockSearchPhaseContext.releasedSearchContexts.size());
        assertTrue(mockSearchPhaseContext.releasedSearchContexts.contains(123L));

        MainSearchTaskStatus status = mockSearchPhaseContext.getTask().getStatus();
        assertEquals(1, status.getCompletedPhases().size());
        MainSearchTaskStatus.PhaseInfo phaseInfo = status.getCompletedPhases().get(0);
        assertEquals("fetch", phaseInfo.getName());
        assertNull(phaseInfo.getFailure());
        assertEquals(2, phaseInfo.getExpectedOps());
        assertEquals(2, phaseInfo.getProcessedShards().size());
        for (MainSearchTaskStatus.ShardInfo shard : phaseInfo.getProcessedShards()) {
            assertEquals("test", shard.getSearchShardTarget().getIndex());
            if (shard.getSearchShardTarget().getShardId().id() == 1) {
                assertEquals("node2", shard.getSearchShardTarget().getNodeId());
                assertNull(shard.getFailure());
                assertSame(taskInfo, shard.getTaskInfo());
            } else {
                assertEquals(0, shard.getSearchShardTarget().getShardId().id());
                assertEquals("node1", shard.getSearchShardTarget().getNodeId());
                assertThat(shard.getFailure(), instanceOf(MockDirectoryWrapper.FakeIOException.class));
                assertNull(shard.getTaskInfo());
            }
        }
    }

    public void testFetchDocsConcurrently() throws InterruptedException {
        TaskInfo taskInfo = new TaskInfo(new TaskId("node", 1), "type", "action", null, null, -1, -1, true, null,
            Collections.emptyMap());
        int resultSetSize = randomIntBetween(0, 100);
        // we use at least 2 hits otherwise this is subject to single shard optimization and we trip an assert...
        int numHits = randomIntBetween(2, 100); // also numshards --> 1 hit per shard
        SearchPhaseController controller = new SearchPhaseController(
            (b) -> new InternalAggregation.ReduceContext(BigArrays.NON_RECYCLING_INSTANCE, null, b));
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(numHits);
        ArraySearchPhaseResults<SearchPhaseResult> results = controller.newSearchPhaseResults(mockSearchPhaseContext.getRequest(), numHits);
        for (int i = 0; i < numHits; i++) {
            QuerySearchResult queryResult = new QuerySearchResult(i, new SearchShardTarget("node1", new ShardId("test", "na", i),
                null, OriginalIndices.NONE));
            queryResult.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {new ScoreDoc(i+1, i)}), i), new DocValueFormat[0]);
            queryResult.size(resultSetSize); // the size of the result set
            queryResult.setShardIndex(i);
            results.consumeResult(queryResult);
        }
        mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null) {
            @Override
            public void sendExecuteFetch(Transport.Connection connection, ShardFetchSearchRequest request, SearchTask task,
                                         SearchActionListener<FetchSearchResult> listener) {
                new Thread(() -> {
                    FetchSearchResult fetchResult = new FetchSearchResult();
                    fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit((int) (request.id()+1))},
                        new TotalHits(1, TotalHits.Relation.EQUAL_TO), 100F));
                    fetchResult.setTaskInfo(taskInfo);
                    listener.onResponse(fetchResult);
                }).start();
            }
        };
        CountDownLatch latch = new CountDownLatch(1);
        FetchSearchPhase phase = new FetchSearchPhase(results, controller, mockSearchPhaseContext,
            (searchResponse, scrollId) -> new SearchPhase("test") {
                @Override
                public void run() {
                    mockSearchPhaseContext.sendSearchResponse(searchResponse, null);
                    latch.countDown();
                }
            });
        assertEquals("fetch", phase.getName());
        phase.run();
        latch.await();
        mockSearchPhaseContext.assertNoFailure();
        SearchResponse searchResponse = mockSearchPhaseContext.searchResponse.get();
        assertNotNull(searchResponse);
        assertEquals(numHits, searchResponse.getHits().getTotalHits().value);
        assertEquals(Math.min(numHits, resultSetSize), searchResponse.getHits().getHits().length);
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (int i = 0; i < hits.length; i++) {
            assertNotNull(hits[i]);
            assertEquals("index: " + i, numHits-i, hits[i].docId());
            assertEquals("index: " + i, numHits-1-i, (int)hits[i].getScore());
        }
        assertEquals(0, searchResponse.getFailedShards());
        assertEquals(numHits, searchResponse.getSuccessfulShards());
        int sizeReleasedContexts = Math.max(0, numHits - resultSetSize); // all non fetched results will be freed
        assertEquals(mockSearchPhaseContext.releasedSearchContexts.toString(),
            sizeReleasedContexts, mockSearchPhaseContext.releasedSearchContexts.size());

        MainSearchTaskStatus status = mockSearchPhaseContext.getTask().getStatus();
        assertNull(status.getCurrentPhase());
        assertEquals(1, status.getCompletedPhases().size());
        MainSearchTaskStatus.PhaseInfo phaseInfo = status.getCompletedPhases().get(0);
        assertEquals("fetch", phaseInfo.getName());
        assertEquals(resultSetSize, phaseInfo.getExpectedOps());
        assertEquals(resultSetSize, phaseInfo.getProcessedShards().size());
        assertNull(phaseInfo.getFailure());
        for (MainSearchTaskStatus.ShardInfo shard : phaseInfo.getProcessedShards()) {
            assertNull(shard.getFailure());
            assertSame(taskInfo, shard.getTaskInfo());
            assertEquals("node1", shard.getSearchShardTarget().getNodeId());
            assertEquals("test", shard.getSearchShardTarget().getIndex());
        }
    }

    public void testExceptionFailsPhase() {
        TaskInfo taskInfo = new TaskInfo(new TaskId("node", 1), "type", "action", null, null, -1, -1, true, null,
            Collections.emptyMap());
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        SearchPhaseController controller = new SearchPhaseController(
            (b) -> new InternalAggregation.ReduceContext(BigArrays.NON_RECYCLING_INSTANCE, null, b));
        ArraySearchPhaseResults<SearchPhaseResult> results =
            controller.newSearchPhaseResults(mockSearchPhaseContext.getRequest(), 2);
        int resultSetSize = randomIntBetween(2, 10);
        QuerySearchResult queryResult = new QuerySearchResult(123, new SearchShardTarget("node1", new ShardId("test", "na", 0),
            null, OriginalIndices.NONE));
        queryResult.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] {new ScoreDoc(42, 1.0F)}), 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize); // the size of the result set
        queryResult.setShardIndex(0);
        results.consumeResult(queryResult);

        queryResult = new QuerySearchResult(321, new SearchShardTarget("node2", new ShardId("test", "na", 1), null, OriginalIndices.NONE));
        queryResult.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] {new ScoreDoc(84, 2.0F)}), 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize);
        queryResult.setShardIndex(1);
        results.consumeResult(queryResult);
        mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null) {
            @Override
            public void sendExecuteFetch(Transport.Connection connection, ShardFetchSearchRequest request, SearchTask task,
                                         SearchActionListener<FetchSearchResult> listener) {
                if (request.id() == 123) {
                    FetchSearchResult fetchResult = new FetchSearchResult();
                    fetchResult.setTaskInfo(taskInfo);
                    fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit(84)}, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 2.0F));
                    listener.onResponse(fetchResult);
                } else {
                    throw new RuntimeException("BOOM");
                }
            }
        };
        FetchSearchPhase phase = new FetchSearchPhase(results, controller, mockSearchPhaseContext,
            (searchResponse, scrollId) -> new SearchPhase("test") {
                @Override
                public void run() {
                    mockSearchPhaseContext.sendSearchResponse(searchResponse, null);
                }
            });
        assertEquals("fetch", phase.getName());
        phase.run();
        assertNotNull(mockSearchPhaseContext.phaseFailure.get());
        assertEquals(mockSearchPhaseContext.phaseFailure.get().getMessage(), "BOOM");
        assertNull(mockSearchPhaseContext.searchResponse.get());
        assertTrue(mockSearchPhaseContext.releasedSearchContexts.isEmpty());

        MainSearchTaskStatus status = mockSearchPhaseContext.getTask().getStatus();
        assertEquals(1, status.getCompletedPhases().size());
        MainSearchTaskStatus.PhaseInfo phaseInfo = status.getCompletedPhases().get(0);
        assertEquals("fetch", phaseInfo.getName());
        assertThat(phaseInfo.getFailure(), instanceOf(RuntimeException.class));
        assertEquals(2, phaseInfo.getExpectedOps());
        assertEquals(1, phaseInfo.getProcessedShards().size());
        MainSearchTaskStatus.ShardInfo shardInfo = phaseInfo.getProcessedShards().get(0);
        assertEquals("node1", shardInfo.getSearchShardTarget().getNodeId());
        assertEquals("test", shardInfo.getSearchShardTarget().getIndex());
        assertEquals(0, shardInfo.getSearchShardTarget().getShardId().getId());
        assertSame(taskInfo, shardInfo.getTaskInfo());
        assertNull(shardInfo.getFailure());
    }

    public void testCleanupIrrelevantContexts() { // contexts that are not fetched should be cleaned up
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        SearchPhaseController controller = new SearchPhaseController(
            (b) -> new InternalAggregation.ReduceContext(BigArrays.NON_RECYCLING_INSTANCE, null, b));
        ArraySearchPhaseResults<SearchPhaseResult> results =
            controller.newSearchPhaseResults(mockSearchPhaseContext.getRequest(), 2);
        int resultSetSize = 1;
        QuerySearchResult queryResult = new QuerySearchResult(123, new SearchShardTarget("node1", new ShardId("test", "na", 0),
            null, OriginalIndices.NONE));
        queryResult.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] {new ScoreDoc(42, 1.0F)}), 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize); // the size of the result set
        queryResult.setShardIndex(0);
        results.consumeResult(queryResult);

        queryResult = new QuerySearchResult(321, new SearchShardTarget("node2", new ShardId("test", "na", 1), null, OriginalIndices.NONE));
        queryResult.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] {new ScoreDoc(84, 2.0F)}), 2.0F), new DocValueFormat[0]);
        queryResult.size(resultSetSize);
        queryResult.setShardIndex(1);
        results.consumeResult(queryResult);

        mockSearchPhaseContext.searchTransport = new SearchTransportService(null, null) {
            @Override
            public void sendExecuteFetch(Transport.Connection connection, ShardFetchSearchRequest request, SearchTask task,
                                         SearchActionListener<FetchSearchResult> listener) {
                FetchSearchResult fetchResult = new FetchSearchResult();
                if (request.id() == 321) {
                    fetchResult.hits(new SearchHits(new SearchHit[] {new SearchHit(84)},
                        new TotalHits(1, TotalHits.Relation.EQUAL_TO), 2.0F));
                } else {
                    fail("requestID 123 should not be fetched but was");
                }
                listener.onResponse(fetchResult);
            }
        };
        FetchSearchPhase phase = new FetchSearchPhase(results, controller, mockSearchPhaseContext,
            (searchResponse, scrollId) -> new SearchPhase("test") {
                @Override
                public void run() {
                    mockSearchPhaseContext.sendSearchResponse(searchResponse, null);
                }
            });
        assertEquals("fetch", phase.getName());
        phase.run();
        mockSearchPhaseContext.assertNoFailure();
        SearchResponse searchResponse = mockSearchPhaseContext.searchResponse.get();
        assertNotNull(searchResponse);
        assertEquals(2, searchResponse.getHits().getTotalHits().value);
        assertEquals(1, searchResponse.getHits().getHits().length);
        assertEquals(84, searchResponse.getHits().getAt(0).docId());
        assertEquals(0, searchResponse.getFailedShards());
        assertEquals(2, searchResponse.getSuccessfulShards());
        assertEquals(1, mockSearchPhaseContext.releasedSearchContexts.size());
        assertTrue(mockSearchPhaseContext.releasedSearchContexts.contains(123L));
    }

    public void testQueryThenFetchNoDocsProgressReporting() {
        SearchPhaseController searchPhaseController = new SearchPhaseController(finalReduce -> null);
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(2);
        SearchPhaseResults<SearchPhaseResult> searchPhaseResults = new ArraySearchPhaseResults<>(2) {
            @Override
            SearchPhaseController.ReducedQueryPhase reduce() {
                return new SearchPhaseController.ReducedQueryPhase(new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0, 0,
                    false, false, null, null, null, SearchPhaseController.SortedTopDocs.EMPTY, null, 1, 10, 0, true);
            }
        };
        FetchSearchPhase fetchSearchPhase = new FetchSearchPhase(searchPhaseResults, searchPhaseController, mockSearchPhaseContext);
        fetchSearchPhase.run();

        MainSearchTaskStatus status = mockSearchPhaseContext.getTask().getStatus();
        assertNull(status.getCurrentPhase());
        assertEquals(2, status.getCompletedPhases().size());
        {
            MainSearchTaskStatus.PhaseInfo phaseInfo = status.getCompletedPhases().get(0);
            assertEquals("fetch", phaseInfo.getName());
            assertEquals(0, phaseInfo.getExpectedOps());
            assertEquals(0, phaseInfo.getProcessedShards().size());
        }
        {
            MainSearchTaskStatus.PhaseInfo phaseInfo = status.getCompletedPhases().get(1);
            assertEquals("expand", phaseInfo.getName());
            assertEquals(-1, phaseInfo.getExpectedOps());
            assertEquals(0, phaseInfo.getProcessedShards().size());
        }
    }
}
