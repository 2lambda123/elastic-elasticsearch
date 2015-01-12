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

package org.elasticsearch.test.store;

import com.google.common.base.Charsets;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.StoreRateLimiting;
import org.apache.lucene.util.AbstractRandomizedTest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.IndexShardException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.distributor.Distributor;
import org.elasticsearch.index.store.fs.FsDirectoryService;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Random;

public class MockFSDirectoryService extends FsDirectoryService {

    private static final EnumSet<IndexShardState> validCheckIndexStates = EnumSet.of(
            IndexShardState.STARTED, IndexShardState.RELOCATED , IndexShardState.POST_RECOVERY
    );

    private final MockDirectoryHelper helper;
    private FsDirectoryService delegateService;
    public static final String CHECK_INDEX_ON_CLOSE = "index.store.mock.check_index_on_close";
    private final boolean checkIndexOnClose;

    @Inject
    public MockFSDirectoryService(final ShardId shardId, @IndexSettings Settings indexSettings, IndexStore indexStore, final IndicesService service) {
        super(shardId, indexSettings, indexStore);
        final long seed = indexSettings.getAsLong(ElasticsearchIntegrationTest.SETTING_INDEX_SEED, 0l);
        Random random = new Random(seed);
        helper = new MockDirectoryHelper(shardId, indexSettings, logger, random, seed);
        checkIndexOnClose = indexSettings.getAsBoolean(CHECK_INDEX_ON_CLOSE, true);

        delegateService = helper.randomDirectorService(indexStore);
        if (checkIndexOnClose) {
            final IndicesLifecycle.Listener listener = new IndicesLifecycle.Listener() {

                boolean canRun = false;

                @Override
                public void beforeIndexShardClosed(ShardId sid, @Nullable IndexShard indexShard,
                                                   @IndexSettings Settings indexSettings) {
                    if (indexShard != null && shardId.equals(sid)) {
                        logger.info("Shard state before potentially flushing is {}", indexShard.state());
                        if (validCheckIndexStates.contains(indexShard.state())) {
                            canRun = true;
                            // When the the internal engine closes we do a rollback, which removes uncommitted segments
                            // By doing a commit flush we perform a Lucene commit, but don't clear the translog,
                            // so that even in tests where don't flush we can check the integrity of the Lucene index
                            indexShard.engine().flush(Engine.FlushType.COMMIT, false, true); // Keep translog for tests that rely on replaying it
                            logger.info("flush finished in beforeIndexShardClosed");
                        }
                    }
                }

                @Override
                public void afterIndexShardClosed(ShardId sid, @Nullable IndexShard indexShard,
                                                  @IndexSettings Settings indexSettings) {
                    if (shardId.equals(sid) && indexShard != null && canRun) {
                        assert indexShard.state() == IndexShardState.CLOSED : "Current state must be closed";
                        checkIndex(indexShard.store(), sid);
                    }
                    service.indicesLifecycle().removeListener(this);
                }
            };
            service.indicesLifecycle().addListener(listener);
        }
    }

    @Override
    public Directory[] build() throws IOException {
        return delegateService.build();
    }
    
    @Override
    protected synchronized Directory newFSDirectory(Path location, LockFactory lockFactory) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void checkIndex(Store store, ShardId shardId) throws IndexShardException {
        if (store.tryIncRef()) {
            logger.info("start check index");
            try {
                Directory dir = store.directory();
                if (!Lucene.indexExists(dir)) {
                    return;
                }
                if (IndexWriter.isLocked(dir)) {
                    AbstractRandomizedTest.checkIndexFailed = true;
                    throw new IllegalStateException("IndexWriter is still open on shard " + shardId);
                }
                try (CheckIndex checkIndex = new CheckIndex(dir)) {
                    BytesStreamOutput os = new BytesStreamOutput();
                    PrintStream out = new PrintStream(os, false, Charsets.UTF_8.name());
                    checkIndex.setInfoStream(out);
                    out.flush();
                    CheckIndex.Status status = checkIndex.checkIndex();
                    if (!status.clean) {
                        AbstractRandomizedTest.checkIndexFailed = true;
                        logger.warn("check index [failure]\n{}", new String(os.bytes().toBytes(), Charsets.UTF_8));
                        throw new IndexShardException(shardId, "index check failure");
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("check index [success]\n{}", new String(os.bytes().toBytes(), Charsets.UTF_8));
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("failed to check index", e);
            } finally {
                logger.info("end check index");
                store.decRef();
            }
        }
    }

    @Override
    public void onPause(long nanos) {
        delegateService.onPause(nanos);
    }

    @Override
    public StoreRateLimiting rateLimiting() {
        return delegateService.rateLimiting();
    }

    @Override
    public long throttleTimeInNanos() {
        return delegateService.throttleTimeInNanos();
    }

    @Override
    public Directory newFromDistributor(Distributor distributor) throws IOException {
        return helper.wrap(super.newFromDistributor(distributor));
    }
}
