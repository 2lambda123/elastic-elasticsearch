/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.lucene.MinimumScoreCollector;
import org.elasticsearch.common.lucene.search.FilteredCollector;
import org.elasticsearch.search.profile.query.InternalProfileCollectorManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.profile.query.CollectorResult.REASON_SEARCH_MIN_SCORE;
import static org.elasticsearch.search.profile.query.CollectorResult.REASON_SEARCH_MULTI;
import static org.elasticsearch.search.profile.query.CollectorResult.REASON_SEARCH_POST_FILTER;
import static org.elasticsearch.search.profile.query.CollectorResult.REASON_SEARCH_TERMINATE_AFTER_COUNT;

abstract class QueryCollectorManagerContext {
    private static final Collector EMPTY_COLLECTOR = new SimpleCollector() {
        @Override
        public void collect(int doc) {}

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    };

    private final String profilerName;

    QueryCollectorManagerContext(String profilerName) {
        this.profilerName = profilerName;
    }

    /**
     * Creates a collector manager that delegates documents to the provided <code>in</code> collector manager.
     * @param in The delegate collector manager
     */
    abstract CollectorManager<Collector, Void> createCollectorManager(CollectorManager<Collector, Void> in) throws IOException;

    /**
     * Wraps this collector manager with a profiler
     */
    InternalProfileCollectorManager createCollectorManagerWithProfile(InternalProfileCollectorManager in) throws IOException {
        return new InternalProfileCollectorManager(createCollectorManager(in), profilerName, in != null ? List.of(in) : List.of());
    }

    public static CollectorManager<Collector, Void> createQueryCollectorManager(List<QueryCollectorManagerContext> collectors)
        throws IOException {
        CollectorManager<Collector, Void> manager = null;
        for (QueryCollectorManagerContext ctx : collectors) {
            manager = ctx.createCollectorManager(manager);
        }
        return manager;
    }

    public static InternalProfileCollectorManager createQueryCollectorManagerWithProfiler(List<QueryCollectorManagerContext> collectors)
        throws IOException {
        InternalProfileCollectorManager manager = null;
        for (QueryCollectorManagerContext ctx : collectors) {
            manager = ctx.createCollectorManagerWithProfile(manager);
        }
        return manager;
    }

    /**
     * Post-process <code>result</code> after search execution.
     *
     * @param result The query search result to populate
     */
    void postProcess(QuerySearchResult result) throws IOException {}

    /**
     * Filters documents with a query score greater than <code>minScore</code>
     * @param minScore The minimum score filter
     */
    static QueryCollectorManagerContext createMinScoreCollectorManagerContext(float minScore) {
        return new QueryCollectorManagerContext(REASON_SEARCH_MIN_SCORE) {

            @Override
            CollectorManager<Collector, Void> createCollectorManager(CollectorManager<Collector, Void> in) throws IOException {
                return new SingleThreadCollectorManager(new MinimumScoreCollector(in.newCollector(), minScore));
            }
        };
    }

    /**
     * Filters documents based on the provided <code>query</code>
     */
    static QueryCollectorManagerContext createFilteredCollectorManagerContext(IndexSearcher searcher, Query query) {
        return new QueryCollectorManagerContext(REASON_SEARCH_POST_FILTER) {
            @Override
            CollectorManager<Collector, Void> createCollectorManager(CollectorManager<Collector, Void> in) throws IOException {
                final Weight filterWeight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
                return new SingleThreadCollectorManager(new FilteredCollector(in.newCollector(), filterWeight));
            }
        };
    }

    /**
     * Creates a multi collector manager from the provided sub-collector
     */
    static QueryCollectorManagerContext createAggsCollectorManagerContext(CollectorManager<Collector, Void> collectorManager) {
        assert collectorManager != null;
        return new QueryCollectorManagerContext(REASON_SEARCH_MULTI) {
            @Override
            CollectorManager<Collector, Void> createCollectorManager(CollectorManager<Collector, Void> in) throws IOException {
                assert in != null;
                return new SingleThreadCollectorManager(MultiCollector.wrap(in.newCollector(), collectorManager.newCollector()));
            }

            @Override
            InternalProfileCollectorManager createCollectorManagerWithProfile(InternalProfileCollectorManager in) throws IOException {
                final List<InternalProfileCollectorManager> subCollectors = new ArrayList<>();
                subCollectors.add(in);
                if (collectorManager instanceof InternalProfileCollectorManager == false) {
                    throw new IllegalArgumentException("non-profiling collector manger");
                }
                subCollectors.add((InternalProfileCollectorManager) collectorManager);
                return new InternalProfileCollectorManager(createCollectorManager(in), REASON_SEARCH_MULTI, subCollectors);
            }
        };
    }

    /**
     * Creates collector manager limiting the collection to the first <code>numHits</code> documents
     */
    static QueryCollectorManagerContext createEarlyTerminationCollectorManagerContext(int numHits) {
        return new QueryCollectorManagerContext(REASON_SEARCH_TERMINATE_AFTER_COUNT) {

            /**
             * Creates a {@link MultiCollector} to ensure that the {@link EarlyTerminatingCollector}
             * can terminate the collection independently of the provided <code>in</code> {@link Collector}.
             */
            @Override
            CollectorManager<Collector, Void> createCollectorManager(CollectorManager<Collector, Void> in) throws IOException {
                assert in != null;
                final Collector collector = new EarlyTerminatingCollector(EMPTY_COLLECTOR, numHits, true);
                return new SingleThreadCollectorManager(MultiCollector.wrap(collector, in.newCollector()));
            }
        };
    }
}
