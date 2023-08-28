/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Superclass of table to assign {@code int} ids to various key types,
 * vending the ids in order they are added.
 */
public abstract class Ordinator {
    protected final PageCacheRecycler recycler;
    protected final CircuitBreaker breaker;
    protected final IdSpace idSpace;

    protected int capacity;
    protected int mask;
    protected int nextGrowSize;
    protected int currentSize;
    protected int growCount;

    protected Ordinator(
        PageCacheRecycler recycler,
        CircuitBreaker breaker,
        IdSpace idSpace,
        int initialCapacity,
        float smallCoreFillFactor
    ) {
        this.recycler = recycler;
        this.breaker = breaker;
        this.idSpace = idSpace;

        this.capacity = initialCapacity;
        this.mask = capacity - 1;
        this.nextGrowSize = (int) (capacity * smallCoreFillFactor);

        assert initialCapacity == Integer.highestOneBit(initialCapacity) : "intial capacity is a power of two";
    }

    /**
     * How many entries are in the {@link Ordinator64}.
     */
    public final int currentSize() {
        return currentSize;
    }

    /**
     * Performance information hopefully useful for debugging.
     */
    public abstract Status status();

    /**
     * Build an iterator to walk all values and ids.
     */
    public abstract Itr iterator();

    /**
     * Sequence of {@code int}s assigned to ids. These can be shared between
     * {@link Ordinator}s.
     */
    public static class IdSpace {
        private int id;

        /**
         * Allocate the next id.
         */
        public int next() {
            return id++;
        }
    }

    /**
     * Performance information about the {@link Ordinator} hopefully useful for debugging.
     */
    public abstract static class Status implements NamedWriteable, ToXContentObject {
        private final int growCount;
        private final int capacity;
        private final int size;
        private final int nextGrowSize;

        protected Status(int growCount, int capacity, int size, int nextGrowSize) {
            this.growCount = growCount;
            this.capacity = capacity;
            this.size = size;
            this.nextGrowSize = nextGrowSize;
        }

        protected Status(StreamInput in) throws IOException {
            this(in.readVInt(), in.readVInt(), in.readVInt(), in.readVInt());
        }

        /**
         * The number of times this {@link Ordinator} has grown.
         */
        public int growCount() {
            return growCount;
        }

        /**
         * The size of the {@link Ordinator}.
         */
        public int capacity() {
            return capacity;
        }

        /**
         * Number of entries added to the {@link Ordinator}.
         */
        public int size() {
            return size;
        }

        /**
         * When {@link #size} grows to this number the hash will grow again.
         */
        public int nextGrowSize() {
            return nextGrowSize;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(growCount);
            out.writeVInt(capacity);
            out.writeVInt(size);
            out.writeVInt(nextGrowSize);
        }

        @Override
        public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("grow_count", growCount);
            builder.field("capacity", capacity);
            builder.field("size", size);
            builder.field("next_grow_size", nextGrowSize);
            builder.field("core", getWriteableName());
            toXContentFragment(builder, params);
            return builder.endObject();
        }

        protected abstract void toXContentFragment(XContentBuilder builder, Params params) throws IOException;
    }

    static class SmallCoreStatus extends Status {
        SmallCoreStatus(int growCount, int capacity, int size, int nextGrowSize) {
            super(growCount, capacity, size, nextGrowSize);
        }

        SmallCoreStatus(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getWriteableName() {
            return "small";
        }

        @Override
        protected void toXContentFragment(XContentBuilder builder, Params params) throws IOException {}
    }

    static class BigCoreStatus extends Status {
        /**
         * The number of times and {@link Ordinator64#add} operation needed to probe additional
         * entries. If all is right with the world this should be {@code 0}, meaning
         * every entry found an empty slot within {@code SIMD_WIDTH} slots from its
         * natural positions. Such hashes will never have to probe on read. More
         * generally, a {@code find} operation should take on average
         * {@code insertProbes / size} probes.
         */
        private final int insertProbes;

        /**
         * The number of {@link PageCacheRecycler#PAGE_SIZE_IN_BYTES} pages allocated for keys.
         */
        public final int keyPages;

        /**
         * The number of {@link PageCacheRecycler#PAGE_SIZE_IN_BYTES} pages allocated for ids.
         */
        public final int idPages;

        BigCoreStatus(int growCount, int capacity, int size, int nextGrowSize, int insertProbes, int keyPages, int idPages) {
            super(growCount, capacity, size, nextGrowSize);
            this.insertProbes = insertProbes;
            this.keyPages = keyPages;
            this.idPages = idPages;
        }

        BigCoreStatus(StreamInput in) throws IOException {
            super(in);
            insertProbes = in.readVInt();
            keyPages = in.readVInt();
            idPages = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(insertProbes);
            out.writeVInt(keyPages);
            out.writeVInt(idPages);
        }

        /**
         * The number of {@link PageCacheRecycler#PAGE_SIZE_IN_BYTES} pages allocated for keys.
         */
        public int keyPages() {
            return keyPages;
        }

        /**
         * The number of {@link PageCacheRecycler#PAGE_SIZE_IN_BYTES} pages allocated for ids.
         */
        public int idPages() {
            return idPages;
        }

        @Override
        public String getWriteableName() {
            return "big";
        }

        @Override
        protected void toXContentFragment(XContentBuilder builder, Params params) throws IOException {
            builder.field("insert_probes", insertProbes);
            builder.field("key_pages", keyPages);
            builder.field("id_pages", idPages);
        }
    }

    /**
     * Shared superstructure for hash cores. Basically just page tracking
     * and {@link Releasable}.
     */
    abstract class Core implements Releasable {
        final List<Releasable> toClose = new ArrayList<>();

        byte[] grabPage() {
            breaker.addEstimateBytesAndMaybeBreak(PageCacheRecycler.PAGE_SIZE_IN_BYTES, "ordinator");
            toClose.add(() -> breaker.addWithoutBreaking(-PageCacheRecycler.PAGE_SIZE_IN_BYTES));
            Recycler.V<byte[]> page = recycler.bytePage(false);
            toClose.add(page);
            return page.v();
        }

        /**
         * Build the status for this core.
         */
        protected abstract Status status();

        /**
         * Build an iterator for all values in the core.
         */
        protected abstract Itr iterator();

        @Override
        public void close() {
            Releasables.close(toClose);
        }
    }

    /**
     * Iterates the entries in the {@link Ordinator}.
     */
    public abstract class Itr {
        protected int slot = -1;

        /**
         * Advance to the next entry in the {@link Ordinator}, returning {@code false}
         * if there aren't any more entries..
         */
        public abstract boolean next();

        /**
         * The id the iterator is current pointing to.
         */
        public abstract int id();
    }
}
