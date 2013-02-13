/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.docset.DocIdSets;

import java.io.IOException;

/**
 * The assumption is that the underlying filter might not apply the accepted docs, so this filter helps to wrap
 * the actual filter and apply the actual accepted docs.
 */
// TODO: we can try and be smart, and only apply if if a filter is cached (down the "chain") since that's the only place that acceptDocs are not applied in ES
public class ApplyAcceptedDocsFilter extends Filter {

    private final Filter filter;

    public ApplyAcceptedDocsFilter(Filter filter) {
        this.filter = filter;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        DocIdSet docIdSet = filter.getDocIdSet(context, acceptDocs);
        if (DocIdSets.isEmpty(docIdSet)) {
            return null;
        }
        if (acceptDocs == null) {
            return docIdSet;
        }
        if (acceptDocs == context.reader().getLiveDocs()) {
            // optimized wrapper for not deleted cases
            return new NotDeletedDocIdSet(docIdSet, acceptDocs);
        }
        return BitsFilteredDocIdSet.wrap(docIdSet, acceptDocs);
    }

    public Filter filter() {
        return this.filter;
    }

    @Override
    public String toString() {
        return filter.toString();
    }

    static class NotDeletedDocIdSet extends DocIdSet {

        private final DocIdSet innerSet;
        private final Bits liveDocs;

        NotDeletedDocIdSet(DocIdSet innerSet, Bits liveDocs) {
            this.innerSet = innerSet;
            this.liveDocs = liveDocs;
        }

        @Override
        public boolean isCacheable() {
            return innerSet.isCacheable();
        }

        @Override
        public Bits bits() throws IOException {
            Bits bits = innerSet.bits();
            if (bits == null) {
                return null;
            }
            return new NotDeleteBits(bits, liveDocs);
        }

        @Override
        public DocIdSetIterator iterator() throws IOException {
            if (!DocIdSets.isFastIterator(innerSet) && liveDocs instanceof FixedBitSet) {
                // might as well iterate over the live docs..., since the iterator is not fast enough
                // but we can only do that if we have Bits..., in short, we reverse the order...
                Bits bits = innerSet.bits();
                if (bits != null) {
                    return new NotDeletedDocIdSetIterator(((FixedBitSet) liveDocs).iterator(), bits);
                }
            }
            DocIdSetIterator iterator = innerSet.iterator();
            if (iterator == null) {
                return null;
            }
            return new NotDeletedDocIdSetIterator(iterator, liveDocs);
        }
    }

    static class NotDeleteBits implements Bits {

        private final Bits bits;
        private final Bits liveDocs;

        NotDeleteBits(Bits bits, Bits liveDocs) {
            this.bits = bits;
            this.liveDocs = liveDocs;
        }

        @Override
        public boolean get(int index) {
            return liveDocs.get(index) && bits.get(index);
        }

        @Override
        public int length() {
            return bits.length();
        }
    }

    static class NotDeletedDocIdSetIterator extends FilteredDocIdSetIterator {

        private final Bits match;

        NotDeletedDocIdSetIterator(DocIdSetIterator innerIter, Bits match) {
            super(innerIter);
            this.match = match;
        }

        @Override
        protected boolean match(int doc) {
            return match.get(doc);
        }
    }
}
