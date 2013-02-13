/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.facet.terms.doubles;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import gnu.trove.iterator.TDoubleIntIterator;
import gnu.trove.map.hash.TDoubleIntHashMap;
import gnu.trove.set.hash.TDoubleHashSet;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

/**
 *
 */
public class TermsDoubleFacetCollector extends AbstractFacetCollector {

    private final IndexNumericFieldData indexFieldData;

    private final TermsFacet.ComparatorType comparatorType;

    private final int size;

    private final int numberOfShards;

    private DoubleValues values;

    private final StaticAggregatorValueProc aggregator;

    private final SearchScript script;

    public TermsDoubleFacetCollector(String facetName, IndexNumericFieldData indexFieldData, int size, TermsFacet.ComparatorType comparatorType, boolean allTerms, SearchContext context,
                                     ImmutableSet<BytesRef> excluded, SearchScript script) {
        super(facetName);
        this.indexFieldData = indexFieldData;
        this.size = size;
        this.comparatorType = comparatorType;
        this.numberOfShards = context.numberOfShards();
        this.script = script;

        if (this.script == null && excluded.isEmpty()) {
            aggregator = new StaticAggregatorValueProc(CacheRecycler.popDoubleIntMap());
        } else {
            aggregator = new AggregatorValueProc(CacheRecycler.popDoubleIntMap(), excluded, this.script);
        }

        // TODO: we need to support this with the new field data....
//        if (allTerms) {
//            try {
//                for (AtomicReaderContext readerContext : context.searcher().getTopReaderContext().leaves()) {
//                    DoubleFieldData fieldData = (DoubleFieldData) fieldDataCache.cache(fieldDataType, readerContext.reader(), indexFieldName);
//                    fieldData.forEachValue(aggregator);
//                }
//            } catch (Exception e) {
//                throw new FacetPhaseExecutionException(facetName, "failed to load all terms", e);
//            }
//        }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        if (script != null) {
            script.setScorer(scorer);
        }
    }

    @Override
    protected void doSetNextReader(AtomicReaderContext context) throws IOException {
        values = indexFieldData.load(context).getDoubleValues();
        if (script != null) {
            script.setNextReader(context);
        }
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        values.forEachValueInDoc(doc, aggregator);
    }

    @Override
    public Facet facet() {
        TDoubleIntHashMap facets = aggregator.facets();
        if (facets.isEmpty()) {
            CacheRecycler.pushDoubleIntMap(facets);
            return new InternalDoubleTermsFacet(facetName, comparatorType, size, ImmutableList.<InternalDoubleTermsFacet.DoubleEntry>of(), aggregator.missing(), aggregator.total());
        } else {
            if (size < EntryPriorityQueue.LIMIT) {
                EntryPriorityQueue ordered = new EntryPriorityQueue(size, comparatorType.comparator());
                for (TDoubleIntIterator it = facets.iterator(); it.hasNext(); ) {
                    it.advance();
                    ordered.insertWithOverflow(new InternalDoubleTermsFacet.DoubleEntry(it.key(), it.value()));
                }
                InternalDoubleTermsFacet.DoubleEntry[] list = new InternalDoubleTermsFacet.DoubleEntry[ordered.size()];
                for (int i = ordered.size() - 1; i >= 0; i--) {
                    list[i] = (InternalDoubleTermsFacet.DoubleEntry) ordered.pop();
                }
                CacheRecycler.pushDoubleIntMap(facets);
                return new InternalDoubleTermsFacet(facetName, comparatorType, size, Arrays.asList(list), aggregator.missing(), aggregator.total());
            } else {
                BoundedTreeSet<InternalDoubleTermsFacet.DoubleEntry> ordered = new BoundedTreeSet<InternalDoubleTermsFacet.DoubleEntry>(comparatorType.comparator(), size);
                for (TDoubleIntIterator it = facets.iterator(); it.hasNext(); ) {
                    it.advance();
                    ordered.add(new InternalDoubleTermsFacet.DoubleEntry(it.key(), it.value()));
                }
                CacheRecycler.pushDoubleIntMap(facets);
                return new InternalDoubleTermsFacet(facetName, comparatorType, size, ordered, aggregator.missing(), aggregator.total());
            }
        }
    }

    public static class AggregatorValueProc extends StaticAggregatorValueProc {

        private final SearchScript script;

        private final TDoubleHashSet excluded;

        public AggregatorValueProc(TDoubleIntHashMap facets, Set<BytesRef> excluded, SearchScript script) {
            super(facets);
            this.script = script;
            if (excluded == null || excluded.isEmpty()) {
                this.excluded = null;
            } else {
                this.excluded = new TDoubleHashSet(excluded.size());
                for (BytesRef s : excluded) {
                    this.excluded.add(Double.parseDouble(s.utf8ToString()));
                }
            }
        }

        @Override
        public void onValue(int docId, double value) {
            if (excluded != null && excluded.contains(value)) {
                return;
            }
            if (script != null) {
                script.setNextDocId(docId);
                script.setNextVar("term", value);
                Object scriptValue = script.run();
                if (scriptValue == null) {
                    return;
                }
                if (scriptValue instanceof Boolean) {
                    if (!((Boolean) scriptValue)) {
                        return;
                    }
                } else {
                    value = ((Number) scriptValue).doubleValue();
                }
            }
            super.onValue(docId, value);
        }
    }

    public static class StaticAggregatorValueProc implements DoubleValues.ValueInDocProc {

        private final TDoubleIntHashMap facets;

        private int missing;
        private int total;

        public StaticAggregatorValueProc(TDoubleIntHashMap facets) {
            this.facets = facets;
        }

        @Override
        public void onValue(int docId, double value) {
            facets.adjustOrPutValue(value, 1, 1);
            total++;
        }

        @Override
        public void onMissing(int docId) {
            missing++;
        }

        public final TDoubleIntHashMap facets() {
            return facets;
        }

        public final int missing() {
            return this.missing;
        }

        public int total() {
            return this.total;
        }
    }
}
