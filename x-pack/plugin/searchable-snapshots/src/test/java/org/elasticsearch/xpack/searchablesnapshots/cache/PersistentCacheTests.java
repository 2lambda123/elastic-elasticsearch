/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.mockfile.FilterFileChannel;
import org.apache.lucene.mockfile.FilterFileSystemProvider;
import org.apache.lucene.mockfile.FilterPath;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.PathUtilsForTesting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.store.cache.CacheFile;
import org.elasticsearch.index.store.cache.CacheKey;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.BUILT_IN_ROLES;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_ROLE;
import static org.elasticsearch.index.store.cache.TestUtils.assertCacheFileEquals;
import static org.elasticsearch.node.NodeRoleSettings.NODE_ROLES_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.cache.PersistentCache.createCacheIndexWriter;
import static org.elasticsearch.xpack.searchablesnapshots.cache.PersistentCache.resolveCacheIndexFolder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class PersistentCacheTests extends AbstractSearchableSnapshotsTestCase {

    public void testCacheIndexWriter() throws Exception {
        final NodeEnvironment.NodePath nodePath = randomFrom(nodeEnvironment.nodePaths());

        int docId = 0;
        final Map<String, Integer> liveDocs = new HashMap<>();
        final Set<String> deletedDocs = new HashSet<>();

        for (int iter = 0; iter < 20; iter++) {

            final Path snapshotCacheIndexDir = resolveCacheIndexFolder(nodePath);
            assertThat(Files.exists(snapshotCacheIndexDir), equalTo(iter > 0));

            // load existing documents from persistent cache index before each iteration
            final Map<String, Document> documents = PersistentCache.loadDocuments(nodeEnvironment);
            assertThat(documents.size(), equalTo(liveDocs.size()));

            try (PersistentCache.CacheIndexWriter writer = createCacheIndexWriter(nodePath)) {
                assertThat(writer.nodePath(), sameInstance(nodePath));

                // verify that existing documents are loaded
                for (Map.Entry<String, Integer> liveDoc : liveDocs.entrySet()) {
                    final Document document = documents.get(liveDoc.getKey());
                    assertThat("Document should be loaded", document, notNullValue());
                    final String iteration = document.get("update_iteration");
                    assertThat(iteration, equalTo(String.valueOf(liveDoc.getValue())));
                    writer.updateCacheFile(liveDoc.getKey(), document);
                }

                // verify that deleted documents are not loaded
                for (String deletedDoc : deletedDocs) {
                    final Document document = documents.get(deletedDoc);
                    assertThat("Document should not be loaded", document, nullValue());
                }

                // random updates of existing documents
                final Map<String, Integer> updatedDocs = new HashMap<>();
                for (String cacheId : randomSubsetOf(liveDocs.keySet())) {
                    final Document document = new Document();
                    document.add(new StringField("cache_id", cacheId, Field.Store.YES));
                    document.add(new StringField("update_iteration", String.valueOf(iter), Field.Store.YES));
                    writer.updateCacheFile(cacheId, document);

                    updatedDocs.put(cacheId, iter);
                }

                // create new random documents
                final Map<String, Integer> newDocs = new HashMap<>();
                for (int i = 0; i < between(1, 10); i++) {
                    final String cacheId = String.valueOf(docId++);
                    final Document document = new Document();
                    document.add(new StringField("cache_id", cacheId, Field.Store.YES));
                    document.add(new StringField("update_iteration", String.valueOf(iter), Field.Store.YES));
                    writer.updateCacheFile(cacheId, document);

                    newDocs.put(cacheId, iter);
                }

                // deletes random documents
                final Map<String, Integer> removedDocs = new HashMap<>();
                for (String cacheId : randomSubsetOf(Sets.union(liveDocs.keySet(), newDocs.keySet()))) {
                    writer.deleteCacheFile(cacheId);

                    removedDocs.put(cacheId, iter);
                }

                boolean commit = false;
                if (frequently()) {
                    writer.commit();
                    commit = true;
                }

                if (commit) {
                    liveDocs.putAll(updatedDocs);
                    liveDocs.putAll(newDocs);
                    for (String cacheId : removedDocs.keySet()) {
                        liveDocs.remove(cacheId);
                        deletedDocs.add(cacheId);
                    }
                }
            }
        }
    }

    public void testRepopulateCache() throws Exception {
        final CacheService cacheService = defaultCacheService();
        cacheService.setCacheSyncInterval(TimeValue.ZERO);
        cacheService.start();

        final List<CacheFile> cacheFiles = randomCacheFiles(cacheService);
        cacheService.synchronizeCache();

        final List<CacheFile> removedCacheFiles = randomSubsetOf(cacheFiles);
        for (CacheFile removedCacheFile : removedCacheFiles) {
            if (randomBoolean()) {
                // evict cache file from the cache
                cacheService.removeFromCache(removedCacheFile.getCacheKey());
            } else {
                IOUtils.rm(removedCacheFile.getFile());
            }
            cacheFiles.remove(removedCacheFile);
        }
        cacheService.stop();

        final CacheService newCacheService = defaultCacheService();
        newCacheService.start();
        for (CacheFile cacheFile : cacheFiles) {
            CacheFile newCacheFile = newCacheService.get(cacheFile.getCacheKey(), cacheFile.getLength(), cacheFile.getFile().getParent());
            assertThat(newCacheFile, notNullValue());
            assertThat(newCacheFile, not(sameInstance(cacheFile)));
            assertCacheFileEquals(newCacheFile, cacheFile);
        }
        newCacheService.stop();
    }

    public void testCleanUp() throws Exception {
        final List<Path> cacheFiles;
        try (CacheService cacheService = defaultCacheService()) {
            cacheService.start();
            cacheFiles = randomCacheFiles(cacheService).stream().map(CacheFile::getFile).collect(Collectors.toList());
            if (randomBoolean()) {
                cacheService.synchronizeCache();
            }
        }

        final Settings nodeSettings = Settings.builder()
            .put(NODE_ROLES_SETTING.getKey(), randomValueOtherThan(DATA_ROLE, () -> randomFrom(BUILT_IN_ROLES)).roleName())
            .build();

        assertTrue(cacheFiles.stream().allMatch(Files::exists));
        PersistentCache.cleanUp(nodeSettings, nodeEnvironment);
        assertTrue(cacheFiles.stream().noneMatch(Files::exists));
    }

    public void testFSyncDoesNotAddDocumentsBackInPersistentCacheWhenShardIsEvicted() throws Exception {
        IOUtils.close(nodeEnvironment); // this test uses a specific filesystem to block fsync

        final FSyncBlockingFileSystemProvider fileSystem = setupFSyncBlockingFileSystemProvider();
        try {
            nodeEnvironment = newNodeEnvironment(
                Settings.builder()
                    .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
                    .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
                    .build()
            );

            try (
                PersistentCache persistentCache = new PersistentCache(nodeEnvironment);
                CacheService cacheService = new CacheService(Settings.EMPTY, clusterService, threadPool, persistentCache)
            ) {
                cacheService.setCacheSyncInterval(TimeValue.ZERO);
                cacheService.start();

                logger.debug("creating cache files");
                final CacheFile randomCacheFile = randomFrom(randomCacheFiles(cacheService));

                final boolean fsyncFailure = randomBoolean();
                logger.debug("blocking fsync for cache file [{}] with failure [{}]", randomCacheFile.getFile(), fsyncFailure);
                fileSystem.blockFSyncForPath(randomCacheFile.getFile(), fsyncFailure);

                logger.debug("starting synchronization of cache files");
                final Thread fsyncThread = new Thread(cacheService::synchronizeCache);
                fsyncThread.start();

                logger.debug("waiting for synchronization of cache files to be blocked");
                fileSystem.waitForBlock();

                final CacheKey cacheKey = randomCacheFile.getCacheKey();
                logger.debug("starting eviction of shard [{}]", cacheKey);
                cacheService.markShardAsEvictedInCache(cacheKey.getSnapshotUUID(), cacheKey.getSnapshotIndexName(), cacheKey.getShardId());

                logger.debug("waiting for shard eviction to be processed");
                cacheService.waitForCacheFilesEvictionIfNeeded(
                    cacheKey.getSnapshotUUID(),
                    cacheKey.getSnapshotIndexName(),
                    cacheKey.getShardId()
                );

                logger.debug("unblocking synchronization of cache files");
                fileSystem.unblock();
                fsyncThread.join();

                if (fsyncFailure) {
                    assertTrue(
                        "Fsync previously failed, the cache file should be marked as 'need fsync' again",
                        cacheService.isCacheFileToSync(randomCacheFile)
                    );
                    cacheService.synchronizeCache();
                }

                assertThat(
                    "Persistent cache should not contain any cached data for the evicted shard",
                    persistentCache.getCacheSize(cacheKey.getShardId(), new SnapshotId("_ignored_", cacheKey.getSnapshotUUID())),
                    equalTo(0L)
                );
            }
        } finally {
            fileSystem.tearDown();
        }
    }

    private static FSyncBlockingFileSystemProvider setupFSyncBlockingFileSystemProvider() {
        final FileSystem defaultFileSystem = PathUtils.getDefaultFileSystem();
        final FSyncBlockingFileSystemProvider provider = new FSyncBlockingFileSystemProvider(defaultFileSystem, createTempDir());
        PathUtilsForTesting.installMock(provider.getFileSystem(null));
        return provider;
    }

    /**
     * {@link FilterFileSystemProvider} that can block fsync for a specified {@link Path}.
     */
    public static class FSyncBlockingFileSystemProvider extends FilterFileSystemProvider {

        private final AtomicReference<Path> pathToBlock = new AtomicReference<>();
        private final AtomicBoolean failFSync = new AtomicBoolean();
        private final CountDownLatch blockingLatch = new CountDownLatch(1);
        private final CountDownLatch releasingLatch = new CountDownLatch(1);

        private final FileSystem delegateInstance;
        private final Path rootDir;

        public FSyncBlockingFileSystemProvider(FileSystem delegate, Path rootDir) {
            super("fsyncblocking://", delegate);
            this.rootDir = new FilterPath(rootDir, this.fileSystem);
            this.delegateInstance = delegate;
        }

        public Path resolve(String other) {
            return rootDir.resolve(other);
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            return new FilterFileChannel(super.newFileChannel(path, options, attrs)) {

                @Override
                public void force(boolean metaData) throws IOException {
                    final Path blockedPath = pathToBlock.get();
                    if (blockedPath == null || blockedPath.equals(path.toAbsolutePath()) == false) {
                        super.force(metaData);
                        return;
                    }
                    try {
                        blockingLatch.countDown();
                        releasingLatch.await();
                        if (failFSync.get()) {
                            throw new IOException("Simulated");
                        } else {
                            super.force(metaData);
                        }
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                }
            };
        }

        public void blockFSyncForPath(Path path, boolean failure) {
            pathToBlock.set(path.toAbsolutePath());
            failFSync.set(failure);
        }

        public void waitForBlock() {
            try {
                blockingLatch.await();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }

        public void unblock() {
            releasingLatch.countDown();
        }

        public void tearDown() {
            PathUtilsForTesting.installMock(delegateInstance);
        }
    }
}
