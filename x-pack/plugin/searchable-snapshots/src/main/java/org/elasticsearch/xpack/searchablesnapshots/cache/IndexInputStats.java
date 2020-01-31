/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheDirectory.CacheBufferedIndexInput;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongConsumer;

/**
 * {@link IndexInputStats} records stats for a given {@link CacheBufferedIndexInput}.
 */
public class IndexInputStats {

    /* A threshold beyond which an index input seeking is counted as "large" */
    static final ByteSizeValue SEEKING_THRESHOLD = new ByteSizeValue(8, ByteSizeUnit.MB);

    private final long fileLength;

    private final LongAdder opened = new LongAdder();
    private final LongAdder inner = new LongAdder();
    private final LongAdder closed = new LongAdder();

    private final Counter forwardSmallSeeks = new Counter();
    private final Counter backwardSmallSeeks = new Counter();

    private final Counter forwardLargeSeeks = new Counter();
    private final Counter backwardLargeSeeks = new Counter();

    private final Counter contiguousReads = new Counter();
    private final Counter nonContiguousReads = new Counter();

    private final Counter directBytesRead = new Counter();

    private final Counter cachedBytesRead = new Counter();
    private final Counter cachedBytesWritten = new Counter();

    public IndexInputStats(long fileLength) {
        this.fileLength = fileLength;
    }

    public void incrementOpenCount() {
        opened.increment();
    }

    public void incrementInnerOpenCount() {
        inner.increment();
    }

    public void incrementCloseCount() {
        closed.increment();
    }

    public void addCachedBytesRead(int bytesRead) {
        cachedBytesRead.add(bytesRead);
    }

    public void addCachedBytesWritten(int bytesWritten) {
        cachedBytesWritten.add(bytesWritten);
    }

    public void addDirectBytesRead(int bytesRead) {
        directBytesRead.add(bytesRead);
    }

    public void incrementBytesRead(long previousPosition, long currentPosition, int bytesRead) {
        LongConsumer incBytesRead = (previousPosition == currentPosition) ? contiguousReads::add : nonContiguousReads::add;
        incBytesRead.accept(bytesRead);
    }

    public void incrementSeeks(long currentPosition, long newPosition) {
        final long delta = newPosition - currentPosition;
        if (delta == 0L) {
            return;
        }
        final boolean isLarge = isLargeSeek(delta);
        if (delta > 0) {
            if (isLarge) {
                forwardLargeSeeks.add(delta);
            } else {
                forwardSmallSeeks.add(delta);
            }
        } else {
            if (isLarge) {
                backwardLargeSeeks.add(delta);
            } else {
                backwardSmallSeeks.add(delta);
            }
        }
    }

    long getFileLength() {
        return fileLength;
    }

    LongAdder getOpened() {
        return opened;
    }

    LongAdder getInnerOpened() {
        return inner;
    }

    LongAdder getClosed() {
        return closed;
    }

    Counter getForwardSmallSeeks() {
        return forwardSmallSeeks;
    }

    Counter getBackwardSmallSeeks() {
        return backwardSmallSeeks;
    }

    Counter getForwardLargeSeeks() {
        return forwardLargeSeeks;
    }

    Counter getBackwardLargeSeeks() {
        return backwardLargeSeeks;
    }

    Counter getContiguousReads() {
        return contiguousReads;
    }

    Counter getNonContiguousReads() {
        return nonContiguousReads;
    }

    Counter getDirectBytesRead() {
        return directBytesRead;
    }

    Counter getCachedBytesRead() {
        return cachedBytesRead;
    }

    Counter getCachedBytesWritten() {
        return cachedBytesWritten;
    }

    @SuppressForbidden(reason = "Handles Long.MIN_VALUE before using Math.abs()")
    boolean isLargeSeek(long delta) {
        return delta != Long.MIN_VALUE && Math.abs(delta) > SEEKING_THRESHOLD.getBytes();
    }

    static class Counter {

        private final LongAdder count = new LongAdder();
        private final LongAdder total = new LongAdder();
        private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);

        void add(final long value) {
            count.increment();
            total.add(value);
            min.updateAndGet(prev -> Math.min(prev, value));
            max.updateAndGet(prev -> Math.max(prev, value));
        }

        long count() {
            return count.sum();
        }

        long total() {
            return total.sum();
        }

        long min() {
            final long value = min.get();
            if (value == Long.MAX_VALUE) {
                return 0L;
            }
            return value;
        }

        long max() {
            final long value = max.get();
            if (value == Long.MIN_VALUE) {
                return 0L;
            }
            return value;
        }
    }
}
