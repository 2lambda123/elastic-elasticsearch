/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.Strings.format;

public class VectorSimilarityQuery extends Query {
    private final float similarity;
    private final float docScore;
    private final Query innerKnnQuery;

    public VectorSimilarityQuery(Query innerKnnQuery, float similarity, float docScore) {
        this.similarity = similarity;
        this.docScore = docScore;
        this.innerKnnQuery = innerKnnQuery;
    }

    // For testing
    Query getInnerKnnQuery() {
        return innerKnnQuery;
    }

    float getSimilarity() {
        return similarity;
    }

    float getDocScore() {
        return docScore;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query rewrittenInnerQuery = innerKnnQuery.rewrite(reader);
        if (rewrittenInnerQuery instanceof MatchNoDocsQuery) {
            return new MatchNoDocsQuery();
        }
        if (rewrittenInnerQuery == innerKnnQuery) {
            return this;
        }
        return new VectorSimilarityQuery(rewrittenInnerQuery, similarity, docScore);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        final Weight innerWeight;
        if (scoreMode.isExhaustive()) {
            innerWeight = innerKnnQuery.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        } else {
            innerWeight = innerKnnQuery.createWeight(searcher, ScoreMode.TOP_SCORES, 1.0f);
        }
        return new MinScoreWeight(innerWeight, docScore, similarity, this, boost);
    }

    @Override
    public String toString(String field) {
        return "VectorSimilarityQuery["
            + "similarity="
            + similarity
            + ", docScore="
            + docScore
            + ", innerKnnQuery="
            + innerKnnQuery.toString(field)
            + ']';
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        VectorSimilarityQuery other = (VectorSimilarityQuery) obj;
        return Objects.equals(innerKnnQuery, other.innerKnnQuery) && docScore == other.docScore && similarity == other.similarity;
    }

    @Override
    public int hashCode() {
        return Objects.hash(innerKnnQuery, docScore, similarity);
    }

    private static class MinScoreWeight extends FilterWeight {

        private final float similarity, docScore, boost;

        private MinScoreWeight(Weight innerWeight, float docScore, float similarity, Query parent, float boost) {
            super(parent, innerWeight);
            this.docScore = docScore;
            this.similarity = similarity;
            this.boost = boost;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            Explanation explanation = in.explain(context, doc);
            if (explanation.isMatch()) {
                float score = explanation.getValue().floatValue();
                if (score >= docScore) {
                    return Explanation.match(explanation.getValue().floatValue() * boost, "vector similarity within limit", explanation);
                } else {
                    return Explanation.noMatch(
                        format(
                            "vector found, but score [%f] is less than matching minimum score [%f] from similarity [%f]",
                            explanation.getValue().floatValue(),
                            docScore,
                            similarity
                        ),
                        explanation
                    );
                }
            }
            return explanation;
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            Scorer innerScorer = in.scorer(context);
            if (innerScorer == null) {
                return null;
            }
            return new MinScoreScorer(this, innerScorer, docScore, boost);
        }
    }

    private static class MinScoreScorer extends Scorer {
        private final float minScore;
        private final float boost;
        private final Scorer in;

        protected MinScoreScorer(Weight weight, Scorer in, float minScore, float boost) {
            super(weight);
            this.minScore = minScore;
            this.boost = boost;
            this.in = in;
        }

        @Override
        public DocIdSetIterator iterator() {
            return new DocIdSetIterator() {
                final DocIdSetIterator innerIterator = in.iterator();

                @Override
                public int docID() {
                    return docIdNoShadow();
                }

                @Override
                public int nextDoc() throws IOException {
                    int doc;
                    do {
                        doc = innerIterator.nextDoc();
                    } while (doc < NO_MORE_DOCS && scoreNoBoost() < minScore);
                    return doc;
                }

                @Override
                public int advance(int target) throws IOException {
                    int doc = innerIterator.advance(target);
                    if (doc == NO_MORE_DOCS) {
                        return doc;
                    }
                    while (doc < NO_MORE_DOCS && scoreNoBoost() < minScore) {
                        doc = innerIterator.nextDoc();
                    }
                    return doc;
                }

                @Override
                public long cost() {
                    return innerIterator.cost();
                }
            };
        }

        private float scoreNoBoost() throws IOException {
            return in.score();
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
            float innerMaxScore = in.getMaxScore(upTo);
            if (innerMaxScore > minScore) {
                return innerMaxScore * boost;
            }
            return 0;
        }

        @Override
        public float score() throws IOException {
            return in.score() * boost;
        }

        private int docIdNoShadow() {
            return in.docID();
        }

        @Override
        public int docID() {
            return docIdNoShadow();
        }
    }
}
