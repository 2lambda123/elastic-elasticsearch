/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * Find documents with text fields that exactly match the input
 */
public class TextFieldExactQuery extends Query {

    private final String value;
    private final Query conjunction;
    private final IndexFieldData<?> fieldData;

    public TextFieldExactQuery(MappedFieldType fieldType, IndexFieldData<?> fieldData, String input) {
        String field = fieldType.name();
        this.value = input;
        this.fieldData = fieldData;
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        TokenStream ts = fieldType.getTextSearchInfo().searchAnalyzer().tokenStream(field, input);
        CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
        try {
            ts.reset();
            while (ts.incrementToken()) {
                bq.add(new TermQuery(new Term(field, termAtt.toString())), BooleanClause.Occur.MUST);
            }
            ts.end();
            ts.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.conjunction = bq.build();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        Weight conjWeight = this.conjunction.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1);
        return new ConstantScoreWeight(this, 1) {
            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                Scorer conjScorer = conjWeight.scorer(context);
                if (conjScorer == null) {
                    return null;
                }
                DocIdSetIterator approx = conjScorer.iterator();
                LeafFieldData fd = fieldData.load(context);
                DocValuesScriptFieldFactory dv = fd.getScriptFieldFactory(fieldData.getFieldName());
                TwoPhaseIterator twoPhase = new TwoPhaseIterator(approx) {
                    @Override
                    public boolean matches() throws IOException {
                        dv.setNextDocId(approximation.docID());
                        for (Object o : dv.toScriptDocValues()) {
                            if (Objects.equals(o, value)) {
                                return true;
                            }
                        }
                        return false;
                    }

                    @Override
                    public float matchCost() {
                        return 9000;
                    }
                };
                return new ConstantScoreScorer(this, score(), scoreMode, twoPhase);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;   // don't cache queries that could do a table scan
            }
        };
    }

    @Override
    public String toString(String field) {
        return field + ":exact(" + value + ")";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        conjunction.visit(visitor);
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        TextFieldExactQuery other = (TextFieldExactQuery) obj;
        return Objects.equals(this.value, other.value) && Objects.equals(this.fieldData, other.fieldData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.value, this.fieldData);
    }
}
