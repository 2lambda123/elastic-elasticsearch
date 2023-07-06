/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilterScorable;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreCachingWrappingScorer;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Top-level collector used in the query phase to perform top hits collection as well as aggs collection.
 * Inspired by {@link org.apache.lucene.search.MultiCollector} but specialized for wrapping two collectors and filtering collected
 * documents as follows:
 * - through an optional <code>post_filter</code> that is applied to the top hits collection
 * - through an optional <code>min_score</code> threshold, which is applied to both the top hits as well as aggs.
 * Supports also terminating the collection after a certain number of documents have been collected (<code>terminate_after</code>).
 *
 * When top docs as well as aggs are collected (because both collectors were provided), skipping low scoring hits via
 * {@link Scorable#setMinCompetitiveScore(float)} is not supported for either of the collectors.
 */
final class QueryPhaseCollector implements Collector {
    private final Collector aggsCollector;
    private final Collector topDocsCollector;
    private final TerminateAfterChecker terminateAfterChecker;
    private final Weight postFilterWeight;
    private final Float minScore;
    private final boolean cacheScores;
    private boolean terminatedAfter = false;

    QueryPhaseCollector(Collector topDocsCollector, Weight postFilterWeight, int terminateAfter, Collector aggsCollector, Float minScore) {
        this(topDocsCollector, postFilterWeight, resolveTerminateAfterChecker(terminateAfter), aggsCollector, minScore);
    }

    QueryPhaseCollector(
        Collector topDocsCollector,
        Weight postFilterWeight,
        TerminateAfterChecker terminateAfterChecker,
        Collector aggsCollector,
        Float minScore
    ) {
        this.topDocsCollector = Objects.requireNonNull(topDocsCollector);
        this.postFilterWeight = postFilterWeight;
        this.terminateAfterChecker = terminateAfterChecker;
        this.aggsCollector = aggsCollector;
        this.minScore = minScore;
        this.cacheScores = aggsCollector != null && topDocsCollector.scoreMode().needsScores() && aggsCollector.scoreMode().needsScores();
    }

    Collector getTopDocsCollector() {
        return topDocsCollector;
    }

    Collector getAggsCollector() {
        return aggsCollector;
    }

    @Override
    public void setWeight(Weight weight) {
        if (postFilterWeight == null && minScore == null) {
            // propagate the weight when we do no additional filtering over the docs that are collected
            // when post_filter or min_score are provided, the collection cannot be shortcut via Weight#count
            topDocsCollector.setWeight(weight);
        }
        if (aggsCollector != null && minScore == null) {
            // min_score is applied to aggs collection as well as top docs collection, though BucketCollectorWrapper does not override it.
            aggsCollector.setWeight(weight);
        }
    }

    @Override
    public ScoreMode scoreMode() {
        ScoreMode scoreMode;
        if (aggsCollector == null) {
            scoreMode = topDocsCollector.scoreMode();
        } else {
            assert aggsCollector.scoreMode() != ScoreMode.TOP_SCORES : "aggs never rely on setMinCompetitiveScore";
            if (topDocsCollector.scoreMode() == aggsCollector.scoreMode()) {
                scoreMode = topDocsCollector.scoreMode();
            } else if (topDocsCollector.scoreMode().needsScores() || aggsCollector.scoreMode().needsScores()) {
                scoreMode = ScoreMode.COMPLETE;
            } else {
                scoreMode = ScoreMode.COMPLETE_NO_SCORES;
            }
            // TODO for aggs that return TOP_DOCS, score mode becomes exhaustive unless top docs collector agrees on the score mode
        }
        if (minScore != null) {
            // TODO if we had TOP_DOCS, shouldn't we return TOP_DOCS_WITH_SCORES instead of COMPLETE?
            scoreMode = scoreMode == ScoreMode.TOP_SCORES ? ScoreMode.TOP_SCORES : ScoreMode.COMPLETE;
        }
        return scoreMode;
    }

    /**
     * @return whether the collection was terminated based on the provided <code>terminate_after</code> value
     */
    boolean isTerminatedAfter() {
        return terminatedAfter;
    }

    private boolean shouldCollectTopDocs(int doc, Scorable scorer, Bits postFilterBits) throws IOException {
        if (isDocWithinMinScore(scorer)) {
            if (doesDocMatchPostFilter(doc, postFilterBits)) {
                // terminate_after is purposely applied after post_filter, and terminates aggs collection based on number of filtered
                // top hits that have been collected. Strange feature, but that has been behaviour for a long time.
                applyTerminateAfter();
                return true;
            }
        }
        return false;
    }

    private boolean isDocWithinMinScore(Scorable scorer) throws IOException {
        return minScore == null || scorer.score() >= minScore;
    }

    private boolean doesDocMatchPostFilter(int doc, Bits postFilterBits) {
        return postFilterBits == null || postFilterBits.get(doc);
    }

    private void applyTerminateAfter() {
        if (terminateAfterChecker.isThresholdReached()) {
            terminatedAfter = true;
            throw new CollectionTerminatedException();
        }
    }

    private Bits getPostFilterBits(LeafReaderContext context) throws IOException {
        if (postFilterWeight == null) {
            return null;
        }
        final ScorerSupplier filterScorerSupplier = postFilterWeight.scorerSupplier(context);
        return Lucene.asSequentialAccessBits(context.reader().maxDoc(), filterScorerSupplier);
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        applyTerminateAfter();
        Bits postFilterBits = getPostFilterBits(context);

        if (aggsCollector == null) {
            LeafCollector topDocsLeafCollector;
            try {
                topDocsLeafCollector = topDocsCollector.getLeafCollector(context);
            } catch (@SuppressWarnings("unused") CollectionTerminatedException e) {
                // TODO we keep on collecting although we have nothing to collect (there is no top docs nor aggs leaf collector).
                // The reason is only to set the early terminated flag to the QueryResult like some tests expect. This needs fixing.
                if (terminateAfterChecker == NO_OP_TERMINATE_AFTER_CHECKER) {
                    throw e;
                }
                topDocsLeafCollector = null;
            }
            return new TopDocsLeafCollector(postFilterBits, topDocsLeafCollector);
        }

        LeafCollector topDocsLeafCollector;
        try {
            topDocsLeafCollector = topDocsCollector.getLeafCollector(context);
        } catch (@SuppressWarnings("unused") CollectionTerminatedException e) {
            // top docs collector does not need this segment, but the aggs collector does.
            topDocsLeafCollector = null;
        }

        LeafCollector aggsLeafCollector;
        try {
            aggsLeafCollector = aggsCollector.getLeafCollector(context);
        } catch (@SuppressWarnings("unused") CollectionTerminatedException e) {
            // aggs collector does not need this segment, but the top docs collector may.
            if (topDocsLeafCollector == null) {
                // TODO we keep on collecting although we have nothing to collect (there is no top docs nor aggs leaf collector).
                // The reason is only to set the early terminated flag to the QueryResult. We should fix this.
                if (terminateAfterChecker == NO_OP_TERMINATE_AFTER_CHECKER) {
                    throw e;
                }
            }
            aggsLeafCollector = null;
        }
        // say that the aggs collector early terminates while the top docs collector does not, we still want to wrap in the same way
        // to enforce that setMinCompetitiveScore is a no-op. Otherwise we may allow the top docs collector to skip non competitive
        // hits despite the score mode of the Collector did not allow it.
        return new CompositeLeafCollector(postFilterBits, topDocsLeafCollector, aggsLeafCollector);
    }

    private class TopDocsLeafCollector implements LeafCollector {
        private final Bits postFilterBits;
        private LeafCollector topDocsLeafCollector;
        private Scorable scorer;

        TopDocsLeafCollector(Bits postFilterBits, LeafCollector topDocsLeafCollector) {
            this.postFilterBits = postFilterBits;
            this.topDocsLeafCollector = topDocsLeafCollector;
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            if (cacheScores) {
                scorer = ScoreCachingWrappingScorer.wrap(scorer);
            }
            if (terminateAfterChecker != NO_OP_TERMINATE_AFTER_CHECKER) {
                scorer = new FilterScorable(scorer) {
                    @Override
                    public void setMinCompetitiveScore(float minScore) {
                        // Ignore calls to setMinCompetitiveScore when terminate_after is used, otherwise early termination
                        // of total hits tracking makes it impossible to terminate after.
                        // TODO the reason is only to set the early terminated flag to the QueryResult. We should fix this.
                    }
                };
            }
            if (topDocsLeafCollector != null) {
                topDocsLeafCollector.setScorer(scorer);
            }
            this.scorer = scorer;
        }

        @Override
        public DocIdSetIterator competitiveIterator() throws IOException {
            if (topDocsLeafCollector != null) {
                return topDocsLeafCollector.competitiveIterator();
            }
            return null;
        }

        @Override
        public void collect(int doc) throws IOException {
            if (shouldCollectTopDocs(doc, scorer, postFilterBits)) {
                terminateAfterChecker.incrementNumCollected();
                if (topDocsLeafCollector != null) {
                    try {
                        topDocsLeafCollector.collect(doc);
                    } catch (@SuppressWarnings("unused") CollectionTerminatedException e) {
                        topDocsLeafCollector = null;
                        // TODO we keep on collecting although we have nothing to collect (there is no top docs nor aggs leaf
                        // collector).
                        // The reason is only to set the early terminated flag to the QueryResult. We should fix this.
                        if (terminateAfterChecker == NO_OP_TERMINATE_AFTER_CHECKER) {
                            throw e;
                        }
                    }
                }
            }
        }
    }

    private class CompositeLeafCollector implements LeafCollector {
        private final Bits postFilterBits;
        private LeafCollector topDocsLeafCollector;
        private LeafCollector aggsLeafCollector;
        private Scorable scorer;

        CompositeLeafCollector(Bits postFilterBits, LeafCollector topDocsLeafCollector, LeafCollector aggsLeafCollector) {
            this.postFilterBits = postFilterBits;
            this.topDocsLeafCollector = topDocsLeafCollector;
            this.aggsLeafCollector = aggsLeafCollector;
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            if (cacheScores) {
                scorer = ScoreCachingWrappingScorer.wrap(scorer);
            }
            scorer = new FilterScorable(scorer) {
                @Override
                public void setMinCompetitiveScore(float minScore) {
                    // Ignore calls to setMinCompetitiveScore so that if the top docs collector
                    // wants to skip low-scoring hits, the aggs collector still sees all hits.
                    // this is important also for terminate_after in case used when total hits tracking is early terminated.
                }
            };
            if (topDocsLeafCollector != null) {
                topDocsLeafCollector.setScorer(scorer);
            }
            if (aggsLeafCollector != null) {
                aggsLeafCollector.setScorer(scorer);
            }
            this.scorer = scorer;
        }

        @Override
        public void collect(int doc) throws IOException {
            if (shouldCollectTopDocs(doc, scorer, postFilterBits)) {
                terminateAfterChecker.incrementNumCollected();
                if (topDocsLeafCollector != null) {
                    try {
                        topDocsLeafCollector.collect(doc);
                    } catch (@SuppressWarnings("unused") CollectionTerminatedException e) {
                        topDocsLeafCollector = null;
                        // top docs collector does not need this segment, but the aggs collector may.
                        if (aggsLeafCollector == null) {
                            // TODO we keep on collecting although we have nothing to collect (there is no top docs nor aggs leaf
                            // collector).
                            // The reason is only to set the early terminated flag to the QueryResult. We should fix this.
                            if (terminateAfterChecker == NO_OP_TERMINATE_AFTER_CHECKER) {
                                throw e;
                            }
                        }
                    }
                }
            }
            // min_score is applied to aggs as well as top hits
            if (isDocWithinMinScore(scorer)) {
                if (aggsLeafCollector != null) {
                    try {
                        aggsLeafCollector.collect(doc);
                    } catch (@SuppressWarnings("unused") CollectionTerminatedException e) {
                        aggsLeafCollector = null;
                        // aggs collector does not need this segment, but the top docs collector may.
                        if (topDocsLeafCollector == null) {
                            // TODO we keep on collecting although we have nothing to collect (there is no top docs nor aggs leaf
                            // collector).
                            // The reason is only to set the early terminated flag to the QueryResult. We should fix this.
                            if (terminateAfterChecker == NO_OP_TERMINATE_AFTER_CHECKER) {
                                throw e;
                            }
                        }
                    }
                }
            }
        }

        @Override
        public DocIdSetIterator competitiveIterator() throws IOException {
            // TODO we expose the competitive iterator only when one of the two sub-leaf collectors has early terminated,
            // it could be a good idea to expose a disjunction of the two when both are not null
            if (topDocsLeafCollector == null) {
                return aggsLeafCollector.competitiveIterator();
            }
            if (aggsLeafCollector == null) {
                return topDocsLeafCollector.competitiveIterator();
            }
            return null;
        }
    }

    static <T, A> QueryPhaseCollectorManager<T, A> createManager(
        CollectorManager<? extends Collector, T> topDocsCollectorManager,
        Weight postFilterWeight,
        int terminateAfter,
        CollectorManager<? extends Collector, A> aggsCollectorManager,
        Float minScore
    ) {
        return new QueryPhaseCollectorManager<>(
            topDocsCollectorManager,
            postFilterWeight,
            resolveTerminateAfterChecker(terminateAfter),
            aggsCollectorManager,
            minScore
        );
    }

    private static TerminateAfterChecker resolveTerminateAfterChecker(int terminateAfter) {
        if (terminateAfter < 0) {
            throw new IllegalArgumentException("terminateAfter must be greater than or equal to 0");
        }
        return terminateAfter == 0 ? NO_OP_TERMINATE_AFTER_CHECKER : new GlobalTerminateAfterChecker(terminateAfter);
    }

    abstract static class TerminateAfterChecker {
        abstract boolean isThresholdReached();

        abstract void incrementNumCollected();
    }

    private static final class GlobalTerminateAfterChecker extends TerminateAfterChecker {
        private final int terminateAfter;
        private final AtomicInteger numCollected = new AtomicInteger();

        GlobalTerminateAfterChecker(int terminateAfter) {
            assert terminateAfter > 0;
            this.terminateAfter = terminateAfter;
        }

        boolean isThresholdReached() {
            return numCollected.getAcquire() >= terminateAfter;
        }

        void incrementNumCollected() {
            numCollected.incrementAndGet();
        }
    }

    private static final TerminateAfterChecker NO_OP_TERMINATE_AFTER_CHECKER = new TerminateAfterChecker() {
        @Override
        boolean isThresholdReached() {
            return false;
        }

        @Override
        void incrementNumCollected() {}
    };
}
