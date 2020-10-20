/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store.cache;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class CacheFile {

    @FunctionalInterface
    public interface EvictionListener {
        void onEviction(CacheFile evictedCacheFile);
    }

    private static final StandardOpenOption[] OPEN_OPTIONS = new StandardOpenOption[] {
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE,
        StandardOpenOption.SPARSE };

    /**
     * Reference counter that counts the number of eviction listeners referencing to this cache file. Once this instance has been evicted
     * and all listeners notified it makes sure to delete the physical file backing this cache.
     */
    private final AbstractRefCounted refCounter = new AbstractRefCounted("CacheFile") {
        @Override
        protected void closeInternal() {
            assert assertNoPendingListeners();
            try {
                Files.deleteIfExists(file);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    };

    private final SparseFileTracker tracker;
    private final String description;
    private final Path file;

    private final Set<EvictionListener> listeners = new HashSet<>();

    /**
     * A reference counted holder for the current channel to the physical file backing this cache file instance.
     */
    private static final class FileChannelReference extends AbstractRefCounted {

        private final FileChannel fileChannel;

        FileChannelReference(Path file) throws IOException {
            super("FileChannel");
            this.fileChannel = FileChannel.open(file, OPEN_OPTIONS);
        }

        @Override
        protected void closeInternal() {
            try {
                fileChannel.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    // If true this file has been evicted from the cache and should not be used any more
    private final AtomicBoolean evicted = new AtomicBoolean(false);

    @Nullable
    private volatile FileChannelReference channelRef;

    public CacheFile(String description, long length, Path file) {
        this.tracker = new SparseFileTracker(file.toString(), length);
        this.description = Objects.requireNonNull(description);
        this.file = Objects.requireNonNull(file);
        assert invariant();
    }

    public long getLength() {
        return tracker.getLength();
    }

    public Path getFile() {
        return file;
    }

    @Nullable
    public FileChannel getChannel() {
        final FileChannelReference reference = channelRef;
        return reference == null ? null : reference.fileChannel;
    }

    public boolean acquire(final EvictionListener listener) throws IOException {
        assert listener != null;

        ensureOpen();
        boolean success = false;
        if (refCounter.tryIncRef()) {
            try {
                synchronized (listeners) {
                    ensureOpen();
                    final boolean added = listeners.add(listener);
                    assert added : "listener already exists " + listener;
                    if (listeners.size() == 1) {
                        assert channelRef == null;
                        channelRef = new FileChannelReference(file);
                    }
                }
                success = true;
            } finally {
                if (success == false) {
                    refCounter.decRef();
                }
            }
        }
        assert invariant();
        return success;
    }

    public boolean release(final EvictionListener listener) {
        assert listener != null;

        boolean success = false;
        try {
            synchronized (listeners) {
                final boolean removed = listeners.remove(Objects.requireNonNull(listener));
                assert removed : "listener does not exist " + listener;
                if (removed == false) {
                    throw new IllegalStateException("Cannot remove an unknown listener");
                }
                if (listeners.isEmpty()) {
                    // nobody is using this file so we close the channel
                    channelRef.decRef();
                    channelRef = null;
                }
            }
            success = true;
        } finally {
            if (success) {
                refCounter.decRef();
            }
        }
        assert invariant();
        return success;
    }

    private boolean assertNoPendingListeners() {
        synchronized (listeners) {
            assert listeners.isEmpty();
            assert channelRef == null;
        }
        return true;
    }

    /**
     * Evicts this file from the cache. Once this method has been called, subsequent use of this class with throw exceptions.
     */
    public void startEviction() {
        if (evicted.compareAndSet(false, true)) {
            final Set<EvictionListener> evictionListeners;
            synchronized (listeners) {
                evictionListeners = new HashSet<>(listeners);
                refCounter.decRef();
            }
            evictionListeners.forEach(listener -> listener.onEviction(this));
        }
        assert invariant();
    }

    private boolean invariant() {
        synchronized (listeners) {
            if (listeners.isEmpty()) {
                assert channelRef == null;
                assert evicted.get() == false || refCounter.refCount() != 0 || Files.notExists(file);
            } else {
                assert channelRef != null;
                assert refCounter.refCount() > 0;
                assert channelRef.refCount() > 0;
                assert Files.exists(file);
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "CacheFile{"
            + "desc='"
            + description
            + "', file="
            + file
            + ", length="
            + tracker.getLength()
            + ", channel="
            + (channelRef != null ? "yes" : "no")
            + ", listeners="
            + listeners.size()
            + ", evicted="
            + evicted
            + ", tracker="
            + tracker
            + '}';
    }

    private void ensureOpen() {
        if (evicted.get()) {
            throw new AlreadyClosedException("Cache file is evicted");
        }
    }

    @FunctionalInterface
    interface RangeAvailableHandler {
        int onRangeAvailable(FileChannel channel) throws IOException;
    }

    @FunctionalInterface
    interface RangeMissingHandler {
        void fillCacheRange(FileChannel channel, long from, long to, Consumer<Long> progressUpdater) throws IOException;
    }

    /**
     * Populates any missing ranges within {@code rangeToWrite} using the {@link RangeMissingHandler}, and notifies the
     * {@link RangeAvailableHandler} when {@code rangeToRead} is available to read from the file. If {@code rangeToRead} is already
     * available then the {@link RangeAvailableHandler} is called synchronously by this method; if not then the given {@link Executor}
     * processes the missing ranges and notifies the {@link RangeAvailableHandler}.
     *
     * @return a future which returns the result of the {@link RangeAvailableHandler} once it has completed.
     */
    CompletableFuture<Integer> populateAndRead(
        final Tuple<Long, Long> rangeToWrite,
        final Tuple<Long, Long> rangeToRead,
        final RangeAvailableHandler reader,
        final RangeMissingHandler writer,
        final Executor executor
    ) {
        ensureOpen();
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        try {
            final FileChannelReference reference = getFileChannelReference(future);
            final List<SparseFileTracker.Gap> gaps = tracker.waitForRange(rangeToWrite, rangeToRead, ActionListener.wrap(success -> {
                final int read = reader.onRangeAvailable(reference.fileChannel);
                assert read == rangeToRead.v2() - rangeToRead.v1() : "partial read ["
                    + read
                    + "] does not match the range to read ["
                    + rangeToRead.v2()
                    + '-'
                    + rangeToRead.v1()
                    + ']';
                future.complete(read);
            }, future::completeExceptionally));

            if (gaps.isEmpty() == false) {
                executor.execute(new AbstractRunnable() {

                    @Override
                    protected void doRun() {
                        for (SparseFileTracker.Gap gap : gaps) {
                            ensureOpen();
                            try {
                                if (reference.tryIncRef() == false) {
                                    throw new AlreadyClosedException("Cache file channel has been released and closed");
                                }
                                try {
                                    ensureOpen();
                                    writer.fillCacheRange(reference.fileChannel, gap.start(), gap.end(), gap::onProgress);
                                } finally {
                                    reference.decRef();
                                }
                                gap.onCompletion();
                            } catch (Exception e) {
                                gap.onFailure(e);
                            }
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        gaps.forEach(gap -> gap.onFailure(e));
                    }
                });
            }
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    /**
     * Notifies the {@link RangeAvailableHandler} when {@code rangeToRead} is available to read from the file. If {@code rangeToRead} is
     * already available then the {@link RangeAvailableHandler} is called synchronously by this method; if not, but it is pending, then the
     * {@link RangeAvailableHandler} is notified when the pending ranges have completed. If it contains gaps that are not currently pending
     * then no listeners are registered and this method returns {@code null}.
     *
     * @return a future which returns the result of the {@link RangeAvailableHandler} once it has completed, or {@code null} if the
     *         target range is neither available nor pending.
     */
    @Nullable
    CompletableFuture<Integer> readIfAvailableOrPending(final Tuple<Long, Long> rangeToRead, final RangeAvailableHandler reader) {
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        try {
            final FileChannelReference reference = getFileChannelReference(future);
            if (tracker.waitForRangeIfPending(rangeToRead, ActionListener.wrap(success -> {
                final int read = reader.onRangeAvailable(reference.fileChannel);
                assert read == rangeToRead.v2() - rangeToRead.v1() : "partial read ["
                    + read
                    + "] does not match the range to read ["
                    + rangeToRead.v2()
                    + '-'
                    + rangeToRead.v1()
                    + ']';
                future.complete(read);
            }, future::completeExceptionally))) {
                return future;
            } else {
                // complete the future to release the channel reference
                future.complete(0);
                return null;
            }
        } catch (Exception e) {
            future.completeExceptionally(e);
            return future;
        }
    }

    /**
     * Get the reference to the currently open file channel for this cache file for a read operation
     *
     * @param future completable future to complete after the read operation finished
     * @return file channel reference
     */
    private FileChannelReference getFileChannelReference(CompletableFuture<Integer> future) {
        final FileChannelReference reference;
        synchronized (listeners) {
            ensureOpen();
            reference = channelRef;
            assert reference.refCount() > 0 : "impossible to run into a fully released channel reference under the listeners mutex";
            reference.incRef();
            future.handle((res, t) -> {
                reference.decRef();
                return null;
            });
        }
        return reference;
    }

    public Tuple<Long, Long> getAbsentRangeWithin(long start, long end) {
        ensureOpen();
        return tracker.getAbsentRangeWithin(start, end);
    }
}
