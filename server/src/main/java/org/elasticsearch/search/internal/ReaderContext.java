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

package org.elasticsearch.search.internal;

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.rescore.RescoreContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Holds a reference to a point in time {@link Engine.Searcher} that will be used to construct {@link SearchContext}.
 * This class also implements {@link org.elasticsearch.common.util.concurrent.RefCounted} since in some situations like
 * in {@link org.elasticsearch.search.SearchService} a SearchContext can be closed concurrently due to independent events
 * ie. when an index gets removed. To prevent accessing closed IndexReader / IndexSearcher instances the SearchContext
 * can be guarded by a reference count and fail if it's been closed by an external event.
 *
 * For reference why we use RefCounted here see https://github.com/elastic/elasticsearch/pull/20095.
 */
public class ReaderContext extends AbstractRefCounted implements Releasable {
    private final long id;
    private final IndexShard indexShard;
    private final Engine.Searcher engineSearcher;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private volatile long keepAlive;
    private volatile long lastAccessTime = -1L;

    // BWC
    private final ShardSearchRequest request;
    private final ScrollContext scrollContext;
    private AggregatedDfs aggregatedDfs;
    private List<RescoreContext> rescore;

    private final List<Releasable> onCloses = new CopyOnWriteArrayList<>();

    public ReaderContext(long id, IndexShard indexShard, Engine.Searcher engineSearcher, ShardSearchRequest request) {
        super("reader_context");
        this.id = id;
        this.indexShard = indexShard;
        this.engineSearcher = engineSearcher;
        this.request = request;
        if (request.scroll() != null) {
            this.scrollContext = new ScrollContext();
        } else {
            this.scrollContext = null;
        }
    }

    @Override
    public final void close() {
        if (closed.compareAndSet(false, true)) {
            decRef();
        } else {
            assert false : "ReaderContext was closed already";
        }
    }

    @Override
    protected void closeInternal() {
        Releasables.close(Releasables.wrap(onCloses), engineSearcher);
    }

    public void addOnClose(Releasable releasable) {
        onCloses.add(releasable);
    }

    public long id() {
        return id;
    }

    public IndexShard indexShard() {
        return indexShard;
    }

    public Engine.Searcher engineSearcher() {
        return engineSearcher;
    }

    public String source() {
        return engineSearcher.source();
    }

    public ShardSearchRequest request() {
        return request;
    }

    public ScrollContext scrollContext() {
        return scrollContext;
    }

    public AggregatedDfs aggregatedDfs() {
        return aggregatedDfs;
    }

    public void aggregatedDfs(AggregatedDfs aggregatedDfs) {
        this.aggregatedDfs = aggregatedDfs;
    }

    public void accessed(long accessTime) {
        this.lastAccessTime = accessTime;
    }

    public long lastAccessTime() {
        return this.lastAccessTime;
    }

    public long keepAlive() {
        return this.keepAlive;
    }

    public void keepAlive(long keepAlive) {
        this.keepAlive = keepAlive;
    }

    public List<RescoreContext> rescore() {
        if (rescore == null) {
            return Collections.emptyList();
        } else {
            return Collections.unmodifiableList(rescore);
        }
    }

    public void addRescore(RescoreContext rescore) {
        if (this.rescore == null) {
            this.rescore = new ArrayList<>();
        }
        this.rescore.add(rescore);
    }
}
