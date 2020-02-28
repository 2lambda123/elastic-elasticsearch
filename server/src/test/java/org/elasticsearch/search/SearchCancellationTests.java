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
package org.elasticsearch.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.Directory;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.lucene.util.TestUtil.nextInt;
import static org.hamcrest.Matchers.equalTo;

public class SearchCancellationTests extends ESTestCase {

    private static final String FIELD_NAME = "foo";

    private static Directory dir;
    private static IndexReader reader;

    @BeforeClass
    public static void setup() throws IOException {
        dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        // we need at least 2 segments - so no merges should be allowed
        w.w.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
        w.setDoRandomForceMerge(false);
        int numDocs = nextInt(random(), 2, 20);
        indexRandomDocuments(w, numDocs, 0);
        w.flush();
        indexRandomDocuments(w, nextInt(random(), 1, 20), numDocs);
        reader = w.getReader();
        w.close();
    }

    private static void indexRandomDocuments(RandomIndexWriter w, int numDocs, int repeatChar) throws IOException {
        for (int i = 1; i <= numDocs; ++i) {
            Document doc = new Document();
            doc.add(new StringField(FIELD_NAME, "a".repeat(i + repeatChar), Field.Store.NO));
            w.addDocument(doc);
        }
    }

    @AfterClass
    public static void cleanup() throws IOException {
        IOUtils.close(reader, dir);
        dir = null;
        reader = null;
    }

    public void testCancellableCollector() throws IOException {
        TotalHitCountCollector collector = new TotalHitCountCollector();
        AtomicBoolean cancelled = new AtomicBoolean();
        ContextIndexSearcher searcher = new ContextIndexSearcher(reader,
            IndexSearcher.getDefaultSimilarity(), IndexSearcher.getDefaultQueryCache(), IndexSearcher.getDefaultQueryCachingPolicy());
        searcher.setCancellable(new ContextIndexSearcher.Cancellable() {
            @Override
            public boolean isEnabled() {
                return true;
            }

            @Override
            public void checkCancelled() {
                if (cancelled.get()) {
                    throw new TaskCancelledException("cancelled");
                }
            }

            @Override
            public void checkDirReaderCancelled() {
            }

            @Override
            public void unsetCheckTimeout() {
            }
        });
        searcher.search(new MatchAllDocsQuery(), collector);
        assertThat(collector.getTotalHits(), equalTo(reader.numDocs()));
        cancelled.set(true);
        expectThrows(TaskCancelledException.class,
            () -> searcher.search(new MatchAllDocsQuery(), collector));
    }

    public void testCancellableDirReader() throws IOException {
        TotalHitCountCollector collector = new TotalHitCountCollector();
        AtomicBoolean cancelled = new AtomicBoolean();
        ContextIndexSearcher searcher = new ContextIndexSearcher(reader,
                IndexSearcher.getDefaultSimilarity(), IndexSearcher.getDefaultQueryCache(), IndexSearcher.getDefaultQueryCachingPolicy());
        searcher.setCancellable(new ContextIndexSearcher.Cancellable() {
            @Override
            public boolean isEnabled() {
                return true;
            }

            @Override
            public void checkCancelled() {
            }

            @Override
            public void checkDirReaderCancelled() {
                if (cancelled.get()) {
                    throw new TaskCancelledException("cancelled");
                }
            }

            @Override
            public void unsetCheckTimeout() {
            }
        });
        searcher.search(new PrefixQuery(new Term(FIELD_NAME, "a")), collector);
        assertThat(collector.getTotalHits(), equalTo(reader.numDocs()));
        cancelled.set(true);
        expectThrows(TaskCancelledException.class, () ->
                searcher.search(new PrefixQuery(new Term(FIELD_NAME, "a")), collector));
    }
}
