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
package org.elasticsearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A basic read-only engine that allows switching a shard to be true read-only temporarily or permanently.
 * Note: this engine can be opened side-by-side with a read-write engine but will not reflect any changes made to the read-write
 * engine.
 *
 * @see #ReadOnlyEngine(EngineConfig, SeqNoStats, TranslogStats, boolean, Function)
 */
public class ReadOnlyEngine extends Engine {

    private final SegmentInfos lastCommittedSegmentInfos;
    private final SeqNoStats seqNoStats;
    private final TranslogStats translogStats;
    private final SearcherManager searcherManager;
    private final IndexCommit indexCommit;
    private final Lock indexWriterLock;
    private final DocsStats docsStats;
    private final RamAccountingSearcherFactory searcherFactory;

    /**
     * Creates a new ReadOnlyEngine. This ctor can also be used to open a read-only engine on top of an already opened
     * read-write engine. It allows to optionally obtain the writer locks for the shard which would time-out if another
     * engine is still open.
     *
     * @param config the engine configuration
     * @param seqNoStats sequence number statistics for this engine or null if not provided
     * @param translogStats translog stats for this engine or null if not provided
     * @param obtainLock if <code>true</code> this engine will try to obtain the {@link IndexWriter#WRITE_LOCK_NAME} lock. Otherwise
     *                   the lock won't be obtained
     * @param readerWrapperFunction allows to wrap the index-reader for this engine.
     */
    public ReadOnlyEngine(EngineConfig config, SeqNoStats seqNoStats, TranslogStats translogStats, boolean obtainLock,
                   Function<DirectoryReader, DirectoryReader> readerWrapperFunction) {
        super(config);
        this.searcherFactory = new RamAccountingSearcherFactory(engineConfig.getCircuitBreakerService());
        try {
            Store store = config.getStore();
            store.incRef();
            DirectoryReader reader = null;
            Directory directory = store.directory();
            Lock indexWriterLock = null;
            boolean success = false;
            try {
                // we obtain the IW lock even though we never modify the index.
                // yet this makes sure nobody else does. including some testing tools that try to be messy
                indexWriterLock = obtainLock ? directory.obtainLock(IndexWriter.WRITE_LOCK_NAME) : null;
                this.lastCommittedSegmentInfos = Lucene.readSegmentInfos(directory);
                this.translogStats = translogStats == null ? new TranslogStats(0, 0, 0, 0, 0) : translogStats;
                if (seqNoStats == null) {
                    seqNoStats = buildSeqNoStats(config, lastCommittedSegmentInfos);
                    ensureMaxSeqNoEqualsToGlobalCheckpoint(seqNoStats);
                }
                this.seqNoStats = seqNoStats;
                this.indexCommit = Lucene.getIndexCommit(lastCommittedSegmentInfos, directory);
                reader = open(indexCommit);
                reader = wrapReader(reader, readerWrapperFunction);
                searcherManager = new SearcherManager(reader, searcherFactory);
                this.docsStats = docsStats(lastCommittedSegmentInfos);
                this.indexWriterLock = indexWriterLock;
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.close(reader, indexWriterLock, store::decRef);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e); // this is stupid
        }
    }

    protected void ensureMaxSeqNoEqualsToGlobalCheckpoint(final SeqNoStats seqNoStats) {
        // Before 8.0 the global checkpoint is not known and up to date when the engine is created after
        // peer recovery, so we only check the max seq no / global checkpoint coherency when the global
        // checkpoint is different from the unassigned sequence number value.
        // In addition to that we only execute the check if the index the engine belongs to has been
        // created after the refactoring of the Close Index API and its TransportVerifyShardBeforeCloseAction
        // that guarantee that all operations have been flushed to Lucene.
        final Version indexVersionCreated = engineConfig.getIndexSettings().getIndexVersionCreated();
        if (indexVersionCreated.onOrAfter(Version.V_7_2_0) ||
            (seqNoStats.getGlobalCheckpoint() != SequenceNumbers.UNASSIGNED_SEQ_NO && indexVersionCreated.onOrAfter(Version.V_6_7_0))) {
            if (seqNoStats.getMaxSeqNo() != seqNoStats.getGlobalCheckpoint()) {
                throw new IllegalStateException("Maximum sequence number [" + seqNoStats.getMaxSeqNo()
                    + "] from last commit does not match global checkpoint [" + seqNoStats.getGlobalCheckpoint() + "]");
            }
        }
        assert assertMaxSeqNoEqualsToGlobalCheckpoint(seqNoStats.getMaxSeqNo(), seqNoStats.getMaxSeqNo());
    }

    protected boolean assertMaxSeqNoEqualsToGlobalCheckpoint(final long maxSeqNo, final long globalCheckpoint) {
        assert maxSeqNo == globalCheckpoint : "max seq. no. [" + maxSeqNo + "] does not match [" + globalCheckpoint + "]";
        return true;
    }

    @Override
    public void verifyEngineBeforeIndexClosing() throws IllegalStateException {
        // the value of the global checkpoint is verified when the read-only engine is opened,
        // and it is not expected to change during the lifecycle of the engine. We could also
        // check this value before closing the read-only engine but if something went wrong
        // and the global checkpoint is not in-sync with the max. sequence number anymore,
        // checking the value here again would prevent the read-only engine to be closed and
        // reopened as an internal engine, which would be the path to fix the issue.
    }

    protected final DirectoryReader wrapReader(DirectoryReader reader,
                                                    Function<DirectoryReader, DirectoryReader> readerWrapperFunction) throws IOException {
        reader = ElasticsearchDirectoryReader.wrap(reader, engineConfig.getShardId());
        if (engineConfig.getIndexSettings().isSoftDeleteEnabled()) {
            reader = new SoftDeletesDirectoryReaderWrapper(reader, Lucene.SOFT_DELETES_FIELD);
        }
        return readerWrapperFunction.apply(reader);
    }

    protected DirectoryReader open(IndexCommit commit) throws IOException {
        return DirectoryReader.open(commit);
    }

    private DocsStats docsStats(final SegmentInfos lastCommittedSegmentInfos) {
        long numDocs = 0;
        long numDeletedDocs = 0;
        long sizeInBytes = 0;
        if (lastCommittedSegmentInfos != null) {
            for (SegmentCommitInfo segmentCommitInfo : lastCommittedSegmentInfos) {
                numDocs += segmentCommitInfo.info.maxDoc() - segmentCommitInfo.getDelCount() - segmentCommitInfo.getSoftDelCount();
                numDeletedDocs += segmentCommitInfo.getDelCount() + segmentCommitInfo.getSoftDelCount();
                try {
                    sizeInBytes += segmentCommitInfo.sizeInBytes();
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to get size for [" + segmentCommitInfo.info.name + "]", e);
                }
            }
        }
        return new DocsStats(numDocs, numDeletedDocs, sizeInBytes);
    }

    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            try {
                IOUtils.close(searcherManager, indexWriterLock, store::decRef);
            } catch (Exception ex) {
                logger.warn("failed to close searcher", ex);
            } finally {
                closedLatch.countDown();
            }
        }
    }

    private static SeqNoStats buildSeqNoStats(EngineConfig config, SegmentInfos infos) {
        final SequenceNumbers.CommitInfo seqNoStats =
            SequenceNumbers.loadSeqNoInfoFromLuceneCommit(infos.userData.entrySet());
        long maxSeqNo = seqNoStats.maxSeqNo;
        long localCheckpoint = seqNoStats.localCheckpoint;
        return new SeqNoStats(maxSeqNo, localCheckpoint, config.getGlobalCheckpointSupplier().getAsLong());
    }

    @Override
    public GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException {
        return getFromSearcher(get, searcherFactory, SearcherScope.EXTERNAL);
    }

    @Override
    protected ReferenceManager<IndexSearcher> getReferenceManager(SearcherScope scope) {
        return searcherManager;
    }

    @Override
    protected SegmentInfos getLastCommittedSegmentInfos() {
        return lastCommittedSegmentInfos;
    }

    @Override
    public String getHistoryUUID() {
        return lastCommittedSegmentInfos.userData.get(Engine.HISTORY_UUID_KEY);
    }

    @Override
    public long getWritingBytes() {
        return 0;
    }

    @Override
    public long getIndexThrottleTimeInMillis() {
        return 0;
    }

    @Override
    public boolean isThrottled() {
        return false;
    }

    @Override
    public IndexResult index(Index index) {
        assert false : "this should not be called";
        throw new UnsupportedOperationException("indexing is not supported on a read-only engine");
    }

    @Override
    public DeleteResult delete(Delete delete) {
        assert false : "this should not be called";
        throw new UnsupportedOperationException("deletes are not supported on a read-only engine");
    }

    @Override
    public NoOpResult noOp(NoOp noOp) {
        assert false : "this should not be called";
        throw new UnsupportedOperationException("no-ops are not supported on a read-only engine");
    }

    @Override
    public boolean isTranslogSyncNeeded() {
        return false;
    }

    @Override
    public boolean ensureTranslogSynced(Stream<Translog.Location> locations) {
        return false;
    }

    @Override
    public void syncTranslog() {
    }

    @Override
    public Closeable acquireRetentionLock() {
        return () -> {};
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(String source, MapperService mapperService, long fromSeqNo, long toSeqNo,
                                                boolean requiredFullRange) throws IOException {
        if (engineConfig.getIndexSettings().isSoftDeleteEnabled() == false) {
            throw new IllegalStateException("accessing changes snapshot requires soft-deletes enabled");
        }
        return readHistoryOperations(source, mapperService, fromSeqNo);
    }

    @Override
    public Translog.Snapshot readHistoryOperations(String source, MapperService mapperService, long startingSeqNo) throws IOException {
        return newEmptySnapshot();
    }

    @Override
    public int estimateNumberOfHistoryOperations(String source, MapperService mapperService, long startingSeqNo) throws IOException {
        return 0;
    }

    @Override
    public boolean hasCompleteOperationHistory(String source, MapperService mapperService, long startingSeqNo) throws IOException {
        // we can do operation-based recovery if we don't have to replay any operation.
        return startingSeqNo > seqNoStats.getMaxSeqNo();
    }

    @Override
    public long getMinRetainedSeqNo() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TranslogStats getTranslogStats() {
        return translogStats;
    }

    @Override
    public Translog.Location getTranslogLastWriteLocation() {
        return new Translog.Location(0,0,0);
    }

    @Override
    public long getLocalCheckpoint() {
        return seqNoStats.getLocalCheckpoint();
    }

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        return new SeqNoStats(seqNoStats.getMaxSeqNo(), seqNoStats.getLocalCheckpoint(), globalCheckpoint);
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return seqNoStats.getGlobalCheckpoint();
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return 0;
    }

    @Override
    public List<Segment> segments(boolean verbose) {
        return Arrays.asList(getSegmentInfo(lastCommittedSegmentInfos, verbose));
    }

    @Override
    public void refresh(String source) {
        // we could allow refreshes if we want down the road the searcher manager will then reflect changes to a rw-engine
        // opened side-by-side
    }

    @Override
    public boolean maybeRefresh(String source) throws EngineException {
        return false;
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {
    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    public SyncedFlushResult syncFlush(String syncId, CommitId expectedCommitId) {
        // we can't do synced flushes this would require an indexWriter which we don't have
        throw new UnsupportedOperationException("syncedFlush is not supported on a read-only engine");
    }

    @Override
    public CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException {
        return new CommitId(lastCommittedSegmentInfos.getId());
    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes,
                           boolean upgrade, boolean upgradeOnlyAncientSegments) {
    }

    @Override
    public IndexCommitRef acquireLastIndexCommit(boolean flushFirst) {
        store.incRef();
        return new IndexCommitRef(indexCommit, store::decRef);
    }

    @Override
    public IndexCommitRef acquireSafeIndexCommit() {
        return acquireLastIndexCommit(false);
    }

    @Override
    public void activateThrottling() {
    }

    @Override
    public void deactivateThrottling() {
    }

    @Override
    public void trimUnreferencedTranslogFiles() {
    }

    @Override
    public boolean shouldRollTranslogGeneration() {
        return false;
    }

    @Override
    public void rollTranslogGeneration() {
    }

    @Override
    public int restoreLocalHistoryFromTranslog(TranslogRecoveryRunner translogRecoveryRunner) {
        return 0;
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) {
        return 0;
    }

    @Override
    public Engine recoverFromTranslog(final TranslogRecoveryRunner translogRecoveryRunner, final long recoverUpToSeqNo) {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            try (Translog.Snapshot snapshot = newEmptySnapshot()) {
                translogRecoveryRunner.run(this, snapshot);
            } catch (final Exception e) {
                throw new EngineException(shardId, "failed to recover from empty translog snapshot", e);
            }
        }
        return this;
    }

    @Override
    public void skipTranslogRecovery() {
    }

    @Override
    public void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) {
    }

    @Override
    public void maybePruneDeletes() {
    }

    @Override
    public DocsStats docStats() {
        return docsStats;
    }

    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {

    }

    protected void processReaders(IndexReader reader, IndexReader previousReader) {
        searcherFactory.processReaders(reader, previousReader);
    }

    @Override
    public boolean refreshNeeded() {
        return false;
    }

    private Translog.Snapshot newEmptySnapshot() {
        return new Translog.Snapshot() {
            @Override
            public void close() {
            }

            @Override
            public int totalOperations() {
                return 0;
            }

            @Override
            public Translog.Operation next() {
                return null;
            }
        };
    }

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return seqNoStats.getMaxSeqNo();
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {
        assert maxSeqNoOfUpdatesOnPrimary <= getMaxSeqNoOfUpdatesOrDeletes() :
            maxSeqNoOfUpdatesOnPrimary + ">" + getMaxSeqNoOfUpdatesOrDeletes();
    }
}
