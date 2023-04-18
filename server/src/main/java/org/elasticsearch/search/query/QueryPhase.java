/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.concurrent.EWMATrackingEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.lucene.queries.SearchAfterSortedDocQuery;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchContextSourcePrinter;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationPhase;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.query.InternalProfileCollectorManager;
import org.elasticsearch.search.rescore.RescorePhase;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.suggest.SuggestPhase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.elasticsearch.search.query.QueryCollectorManagerContext.createAggsCollectorManagerContext;
import static org.elasticsearch.search.query.QueryCollectorManagerContext.createEarlyTerminationCollectorManagerContext;
import static org.elasticsearch.search.query.QueryCollectorManagerContext.createFilteredCollectorManagerContext;
import static org.elasticsearch.search.query.QueryCollectorManagerContext.createMinScoreCollectorManagerContext;
import static org.elasticsearch.search.query.QueryCollectorManagerContext.createQueryCollectorManager;
import static org.elasticsearch.search.query.QueryCollectorManagerContext.createQueryCollectorManagerWithProfiler;
import static org.elasticsearch.search.query.TopDocsCollectorManagerContext.createTopDocsCollectorContext;

/**
 * Query phase of a search request, used to run the query and get back from each shard information about the matching documents
 * (document ids and score or sort criteria) so that matches can be reduced on the coordinating node
 */
public class QueryPhase {
    private static final Logger LOGGER = LogManager.getLogger(QueryPhase.class);

    public QueryPhase() {}

    public static void execute(SearchContext searchContext) throws QueryPhaseExecutionException {
        if (searchContext.hasOnlySuggest()) {
            SuggestPhase.execute(searchContext);
            searchContext.queryResult()
                .topDocs(
                    new TopDocsAndMaxScore(new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), Lucene.EMPTY_SCORE_DOCS), Float.NaN),
                    new DocValueFormat[0]
                );
            return;
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{}", new SearchContextSourcePrinter(searchContext));
        }

        // Pre-process aggregations as late as possible. In the case of a DFS_Q_T_F
        // request, preProcess is called on the DFS phase, this is why we pre-process them
        // here to make sure it happens during the QUERY phase
        AggregationPhase.preProcess(searchContext);
        executeInternal(searchContext);

        RescorePhase.execute(searchContext);
        SuggestPhase.execute(searchContext);
        AggregationPhase.execute(searchContext);

        if (searchContext.getProfilers() != null) {
            searchContext.queryResult().profileResults(searchContext.getProfilers().buildQueryPhaseResults());
        }
    }

    /**
     * In a package-private method so that it can be tested without having to
     * wire everything (mapperService, etc.)
     */
    static void executeInternal(SearchContext searchContext) throws QueryPhaseExecutionException {
        final ContextIndexSearcher searcher = searchContext.searcher();
        final IndexReader reader = searcher.getIndexReader();
        QuerySearchResult queryResult = searchContext.queryResult();
        queryResult.searchTimedOut(false);
        try {
            queryResult.from(searchContext.from());
            queryResult.size(searchContext.size());
            Query query = searchContext.rewrittenQuery();
            assert query == searcher.rewrite(query); // already rewritten

            final ScrollContext scrollContext = searchContext.scrollContext();
            if (scrollContext != null) {
                if (scrollContext.totalHits == null) {
                    // first round
                    assert scrollContext.lastEmittedDoc == null;
                    // there is not much that we can optimize here since we want to collect all
                    // documents in order to get the total number of hits

                } else {
                    final ScoreDoc after = scrollContext.lastEmittedDoc;
                    if (canEarlyTerminate(reader, searchContext.sort())) {
                        // now this gets interesting: since the search sort is a prefix of the index sort, we can directly
                        // skip to the desired doc
                        if (after != null) {
                            query = new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST)
                                .add(new SearchAfterSortedDocQuery(searchContext.sort().sort, (FieldDoc) after), BooleanClause.Occur.FILTER)
                                .build();
                        }
                    }
                }
            }

            final LinkedList<QueryCollectorManagerContext> collectors = new LinkedList<>();
            // whether the chain contains a collector that filters documents
            boolean hasFilterCollector = false;
            if (searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER) {
                // add terminate_after before the filter collectors
                // it will only be applied on documents accepted by these filter collectors
                collectors.add(createEarlyTerminationCollectorManagerContext(searchContext.terminateAfter()));
            }
            if (searchContext.parsedPostFilter() != null) {
                // add post filters before aggregations
                // it will only be applied to top hits
                collectors.add(createFilteredCollectorManagerContext(searcher, searchContext.parsedPostFilter().query()));
                // this collector can filter documents during the collection
                hasFilterCollector = true;
            }
            if (searchContext.getAggsCollectorManager() != null) {
                // plug in additional collectors, like aggregations
                collectors.add(createAggsCollectorManagerContext(searchContext.getAggsCollectorManager()));
            }
            if (searchContext.minimumScore() != null) {
                // apply the minimum score after multi collector so we filter aggs as well
                collectors.add(createMinScoreCollectorManagerContext(searchContext.minimumScore()));
                // this collector can filter documents during the collection
                hasFilterCollector = true;
            }

            boolean timeoutSet = scrollContext == null
                && searchContext.timeout() != null
                && searchContext.timeout().equals(SearchService.NO_TIMEOUT) == false;

            final Runnable timeoutRunnable;
            if (timeoutSet) {
                final long startTime = searchContext.getRelativeTimeInMillis();
                final long timeout = searchContext.timeout().millis();
                final long maxTime = startTime + timeout;
                timeoutRunnable = searcher.addQueryCancellation(() -> {
                    final long time = searchContext.getRelativeTimeInMillis();
                    if (time > maxTime) {
                        throw new TimeExceededException();
                    }
                });
            } else {
                timeoutRunnable = null;
            }

            searchWithCollectorManager(searchContext, searcher, query, collectors, hasFilterCollector, timeoutRunnable);
            ExecutorService executor = searchContext.indexShard().getThreadPool().executor(ThreadPool.Names.SEARCH);
            assert executor instanceof EWMATrackingEsThreadPoolExecutor
                || (executor instanceof EsThreadPoolExecutor == false /* in case thread pool is mocked out in tests */)
                : "SEARCH threadpool should have an executor that exposes EWMA metrics, but is of type " + executor.getClass();
            if (executor instanceof EWMATrackingEsThreadPoolExecutor rExecutor) {
                queryResult.nodeQueueSize(rExecutor.getCurrentQueueSize());
                queryResult.serviceTimeEWMA((long) rExecutor.getTaskExecutionEWMA());
            }
        } catch (Exception e) {
            throw new QueryPhaseExecutionException(searchContext.shardTarget(), "Failed to execute main query", e);
        }
    }

    private static void searchWithCollectorManager(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorManagerContext> collectors,
        boolean hasFilterCollector,
        Runnable timeoutRunnable
    ) throws IOException {
        // create the top docs collector last when the other collectors are known
        final TopDocsCollectorManagerContext topDocsFactory = createTopDocsCollectorContext(searchContext, hasFilterCollector);
        // add the top docs collector, the first collector context in the chain
        collectors.addFirst(topDocsFactory);

        CollectorManager<Collector, Void> manager;
        if (searchContext.getProfilers() != null) {
            final InternalProfileCollectorManager profileManager = createQueryCollectorManagerWithProfiler(collectors);
            searchContext.getProfilers().getCurrentQueryProfiler().setCollectorManager(profileManager);
            manager = profileManager;
        } else {
            manager = createQueryCollectorManager(collectors);
        }
        final List<Collector> collectedCollectors;
        if ((timeoutRunnable != null && searchContext.request().allowPartialSearchResults())
            || searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER) {
            // We need to handle the case when the execution is exception driven (timeouts and early termination).
            // In this case, we need to run the reduce phase ourselves, hence we collect
            // here the top level collectors.
            collectedCollectors = new ArrayList<>();
            final CollectorManager<Collector, Void> in = manager;
            manager = new CollectorManager<>() {
                @Override
                public Collector newCollector() throws IOException {
                    Collector collector = in.newCollector();
                    collectedCollectors.add(collector);
                    return collector;
                }

                @Override
                public Void reduce(Collection<Collector> collectors) throws IOException {
                    return in.reduce(collectors);
                }
            };
        } else {
            collectedCollectors = null;
        }
        QuerySearchResult queryResult = searchContext.queryResult();
        try {
            searcher.search(query, manager);
        } catch (EarlyTerminatingCollector.EarlyTerminationException e) {
            executeReductionAfterExceptionDrivenExecution(searcher, timeoutRunnable, manager, collectedCollectors);
            queryResult.terminatedEarly(true);
        } catch (TimeExceededException e) {
            assert timeoutRunnable != null : "TimeExceededException thrown even though timeout wasn't set";
            if (searchContext.request().allowPartialSearchResults() == false) {
                // Can't rethrow TimeExceededException because not serializable
                throw new QueryPhaseExecutionException(searchContext.shardTarget(), "Time exceeded");
            }
            executeReductionAfterExceptionDrivenExecution(searcher, timeoutRunnable, manager, collectedCollectors);
            queryResult.searchTimedOut(true);
        }
        if (searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER && queryResult.terminatedEarly() == null) {
            queryResult.terminatedEarly(false);
        }
        for (QueryCollectorManagerContext ctx : collectors) {
            ctx.postProcess(queryResult);
        }
    }

    private static void executeReductionAfterExceptionDrivenExecution(
        ContextIndexSearcher searcher,
        Runnable timeoutRunnable,
        CollectorManager<Collector, Void> manager,
        List<Collector> collectedCollectors
    ) throws IOException {
        // Search phase has finished, no longer need to check for timeout
        // otherwise reduction phase might get cancelled.
        searcher.removeQueryCancellation(timeoutRunnable);
        // Reduce our collectors to collect partial results
        manager.reduce(collectedCollectors);
    }

    /**
     * Returns whether collection within the provided <code>reader</code> can be early-terminated if it sorts
     * with <code>sortAndFormats</code>.
     **/
    private static boolean canEarlyTerminate(IndexReader reader, SortAndFormats sortAndFormats) {
        if (sortAndFormats == null || sortAndFormats.sort == null) {
            return false;
        }
        final Sort sort = sortAndFormats.sort;
        for (LeafReaderContext ctx : reader.leaves()) {
            Sort indexSort = ctx.reader().getMetaData().getSort();
            if (indexSort == null || Lucene.canEarlyTerminate(sort, indexSort) == false) {
                return false;
            }
        }
        return true;
    }

    public static class TimeExceededException extends RuntimeException {}
}
