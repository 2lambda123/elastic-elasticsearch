/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;

import org.apache.lucene.util.hppc.BitMixer;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Assigns {@code int} ids to {@code long}s, vending the ids in order they are added.
 * <p>
 *     At it's core there are two hash table implementations, a "small core" and
 *     a "big core". The "small core" is a simple
 *     <a href="https://en.wikipedia.org/wiki/Open_addressing">open addressed</a>
 *     hash table with a fixed 60% load factor and a table of 2048. It quite quick
 *     because it has a fixed size and never grows.
 * </p>
 * <p>
 *     When the "small core" has more entries than it's load factor the "small core"
 *     is replaced with a "big core". The "big core" functions quite similarly to
 *     a <a href="https://faultlore.com/blah/hashbrown-tldr/">Swisstable</a>, Google's
 *     fancy SIMD hash table. In this table there's a contiguous array of "control"
 *     bytes that are either {@code 0b1111_1111} for empty entries or
 *     {@code 0b0aaa_aaaa} for populated entries, where {@code aaa_aaaa} are the top
 *     7 bytes of the hash. To find an entry by key you hash it, grab the top 7 bytes
 *     or it, and perform a SIMD read of the control array starting at the expected
 *     slot. We use the widest SIMD instruction the CPU supports, meaning 64 or 32
 *     bytes. If any of those match we check the actual key. So instead of scanning
 *     one slot at a time "small core", we effectively scan a whole bunch at once.
 *     This allows us to run a much higher load factor (85%) without any performance
 *     penalty so the extra byte feels super worth it.
 * </p>
 * <p>
 *     When a "big core" fills it's table to the fill factor, we build a new "big core"
 *     and read all values in the old "big core" into the new one.
 * </p>
 */
@SuppressWarnings("preview")
public class Ordinator64 extends Ordinator implements Releasable {
    private static final VectorSpecies<Byte> BS = ByteVector.SPECIES_PREFERRED;

    private static final int PAGE_SHIFT = 14;

    private static final int PAGE_MASK = PageCacheRecycler.PAGE_SIZE_IN_BYTES - 1;

    private static final int KEY_SIZE = Long.BYTES;

    private static final int ID_SIZE = Integer.BYTES;

    static final int INITIAL_CAPACITY = PageCacheRecycler.PAGE_SIZE_IN_BYTES / KEY_SIZE;

    static {
        if (PageCacheRecycler.PAGE_SIZE_IN_BYTES >> PAGE_SHIFT != 1) {
            throw new AssertionError("bad constants");
        }
        if (Integer.highestOneBit(KEY_SIZE) != KEY_SIZE) {
            throw new AssertionError("not a power of two");
        }
        if (Integer.highestOneBit(ID_SIZE) != ID_SIZE) {
            throw new AssertionError("not a power of two");
        }
        if (Integer.highestOneBit(INITIAL_CAPACITY) != INITIAL_CAPACITY) {
            throw new AssertionError("not a power of two");
        }
        if (ID_SIZE > KEY_SIZE) {
            throw new AssertionError("key too small");
        }
    }

    private static final VarHandle longHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
    private static final VarHandle intHandle = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());

    private SmallCore smallCore;
    private BigCore bigCore;

    public Ordinator64(PageCacheRecycler recycler, CircuitBreaker breaker, IdSpace idSpace) {
        super(recycler, breaker, idSpace, INITIAL_CAPACITY, Ordinator64.SmallCore.FILL_FACTOR);
        this.smallCore = new SmallCore();
    }

    /**
     * Find an {@code id} by a {@code key}.
     */
    public int find(long key) {
        int hash = hash(key);
        if (smallCore != null) {
            return smallCore.find(key, hash);
        } else {
            return bigCore.find(key, hash, control(hash));
        }
    }

    /**
     * Add many {@code key}s at once, putting their {@code id}s into an array.
     * If any {@code key} was already present it's previous assigned {@code id}
     * will be added to the array. If it wasn't present it'll be assigned a new
     * {@code id}.
     * <p>
     *     This method tends to be faster than {@link #add(long)}.
     * </p>
     */
    public void add(long[] keys, int[] ids, int length) {
        int i = 0;
        for (; i < length; i++) {
            if (bigCore != null) {
                for (; i < length; i++) {
                    long k = keys[i];
                    ids[i] = bigCore.add(k, hash(k));
                }
                return;
            }

            ids[i] = add(keys[i]);
        }
    }

    /**
     * Add many {@code key}s at once, putting their {@code id}s into a builder.
     * If any {@code key} was already present it's previous assigned {@code id}
     * will be added to the builder. If it wasn't present it'll be assigned a new
     * {@code id}.
     * <p>
     *     This method tends to be faster than {@link #add(long)}.
     * </p>
     */
    public void add(long[] keys, LongBlock.Builder ids, int length) {
        int i = 0;
        for (; i < length; i++) {
            if (bigCore != null) {
                for (; i < length; i++) {
                    long k = keys[i];
                    ids.appendLong(bigCore.add(k, hash(k)));
                }
                return;
            }

            ids.appendLong(add(keys[i]));
        }
    }

    /**
     * Add a {@code key}, returning its {@code id}s. If it was already present
     * it's previous assigned {@code id} will be returned. If it wasn't present
     * it'll be assigned a new {@code id}.
     */
    public int add(long key) {
        int hash = hash(key);
        if (smallCore != null) {
            if (currentSize < nextGrowSize) {
                return smallCore.add(key, hash);
            }
            smallCore.transitionToBigCore();
        }
        return bigCore.add(key, hash);
    }

    @Override
    public Status status() {
        return smallCore != null ? smallCore.status() : bigCore.status();
    }

    public abstract class Itr extends Ordinator.Itr {
        /**
         * The key the iterator is current pointing to.
         */
        public abstract long key();
    }

    @Override
    public Itr iterator() {
        return smallCore != null ? smallCore.iterator() : bigCore.iterator();
    }

    /**
     * Build the control byte for a populated entry out of the hash.
     * The control bytes for a populated entry has the high bit clear
     * and the remaining 7 bits contain the top 7 bits of the hash.
     * So it looks like {@code 0b0xxx_xxxx}.
     */
    private byte control(int hash) {
        return (byte) (hash >>> (Integer.SIZE - 7));
    }

    @Override
    public void close() {
        Releasables.close(smallCore, bigCore);
    }

    private int growTracking() {
        // Juggle constants for the new page size
        growCount++;
        // TODO what about MAX_INT?
        int oldCapacity = capacity;
        capacity <<= 1;
        mask = capacity - 1;
        nextGrowSize = (int) (capacity * BigCore.FILL_FACTOR);
        return oldCapacity;
    }

    /**
     * Open addressed hash table the probes by triangle numbers. Empty
     * {@code id}s are encoded as {@code -1}. This hash table can't
     * grow, and is instead replaced by a {@link BigCore}.
     * <p>
     *     This uses two pages from the {@link PageCacheRecycler}, one
     *     for the {@code keys} and one for the {@code ids}.
     * </p>
     */
    class SmallCore extends Core {
        static final float FILL_FACTOR = 0.6F;

        private final byte[] keyPage;
        private final byte[] idPage;

        private SmallCore() {
            boolean success = false;
            try {
                keyPage = grabPage();
                idPage = grabPage();
                Arrays.fill(idPage, (byte) 0xff);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        int find(long key, int hash) {
            int slotIncrement = 0;
            int slot = slot(hash);
            while (true) {
                int id = id(slot);
                if (id < 0) {
                    // Empty
                    return -1;
                }
                if (key(slot) == key) {
                    return id;
                }

                slotIncrement++;
                slot = slot(slot + slotIncrement);
            }
        }

        int add(long key, int hash) {
            int slotIncrement = 0;
            int slot = slot(hash);
            while (true) {
                int keyOffset = keyOffset(slot);
                int idOffset = idOffset(slot);
                long slotKey = (long) longHandle.get(keyPage, keyOffset);
                int slotId = (int) intHandle.get(idPage, idOffset);
                if (slotId >= 0) {
                    if (slotKey == key) {
                        return slotId;
                    }
                } else {
                    currentSize++;
                    longHandle.set(keyPage, keyOffset, key);
                    int id = idSpace.next();
                    intHandle.set(idPage, idOffset, id);
                    return id;
                }

                slotIncrement++;
                slot = slot(slot + slotIncrement);
            }
        }

        void transitionToBigCore() {
            int oldCapacity = growTracking();

            try {
                bigCore = new BigCore();
                rehash(oldCapacity);
            } finally {
                close();
                smallCore = null;
            }
        }

        @Override
        protected Status status() {
            return new SmallCoreStatus(growCount, capacity, currentSize, nextGrowSize);
        }

        @Override
        protected Itr iterator() {
            return new Itr() {
                @Override
                public boolean next() {
                    do {
                        slot++;
                    } while (slot < capacity && SmallCore.this.id(slot) < 0);
                    return slot < capacity;
                }

                @Override
                public int id() {
                    return SmallCore.this.id(slot);
                }

                @Override
                public long key() {
                    return SmallCore.this.key(slot);
                }
            };
        }

        private void rehash(int oldCapacity) {
            for (int slot = 0; slot < oldCapacity; slot++) {
                int id = (int) intHandle.get(idPage, idOffset(slot));
                if (id < 0) {
                    continue;
                }
                long key = (long) longHandle.get(keyPage, keyOffset(slot));
                int hash = hash(key);
                bigCore.insert(key, hash, control(hash), id);
            }
        }

        private long key(int slot) {
            return (long) longHandle.get(keyPage, keyOffset(slot));
        }

        private int id(int slot) {
            return (int) intHandle.get(idPage, idOffset(slot));
        }
    }

    /**
     * A Swisstable inspired hashtable. This differs from the normal swisstable
     * in because it's adapted to Elasticsearch's {@link PageCacheRecycler}.
     * The keys and ids are stored many {@link PageCacheRecycler#PAGE_SIZE_IN_BYTES}
     * arrays, with the keys separated from the values. This is mostly so that we
     * can be sure the array and offset into the array can be calculated by right
     * shifts.
     */
    class BigCore extends Core {
        static final float FILL_FACTOR = 0.85F;

        private static final byte CONTROL_EMPTY = (byte) 0b1111_1111;

        /**
         * The "control" bytes from the Swisstable algorithm. This'll contain
         * {@link #CONTROL_EMPTY} for empty entries and {@code 0b0aaa_aaaa} for
         * filled entries, where {@code aaa_aaaa} are the top seven bits of the
         * hash. These are tests by SIMD instructions as a quick first pass to
         * check many entries at once.
         * <p>
         *     This array has to be contiguous or we loose too much speed so it
         *     isn't managed by the {@link PageCacheRecycler}, instead we
         *     allocate it directly.
         * </p>
         * <p>
         *     This array contains {@code capacity + SIMD_LANES} entries with the
         *     first {@code SIMD_LANES} bytes cloned to the end of the array so
         *     the simd probes for possible matches never had to worry about
         *     "wrapping" around the array.
         */
        private final byte[] controlData;

        /**
         * Pages of {@code keys}, vended by the {@link PageCacheRecycler}. It's
         * important that the size of keys be a power of two, so we can quickly
         * select the appropriate page and keys never span multiple pages.
         */
        private final byte[][] keyPages;

        /**
         * Pages of {@code ids}, vended by the {@link PageCacheRecycler}. Ids
         * are {@code int}s so it's very quick to select the appropriate page
         * for each slot.
         */
        private final byte[][] idPages;

        /**
         * The number of times and {@link #add} operation needed to probe additional
         * entries. If all is right with the world this should be {@code 0}, meaning
         * every entry found an empty slot within {@code SIMD_WIDTH} slots from its
         * natural positions. Such hashes will never have to probe on read. More
         * generally, a {@code find} operation should take on average
         * {@code insertProbes / size} probes.
         */
        private int insertProbes;

        BigCore() {
            int controlLength = capacity + BS.length();
            breaker.addEstimateBytesAndMaybeBreak(controlLength, "ordinator");
            toClose.add(() -> breaker.addWithoutBreaking(-controlLength));
            controlData = new byte[controlLength];
            Arrays.fill(controlData, (byte) 0xFF);

            boolean success = false;
            try {
                int keyPagesNeeded = (capacity * KEY_SIZE - 1) >> PAGE_SHIFT;
                keyPagesNeeded++;
                keyPages = new byte[keyPagesNeeded][];
                for (int i = 0; i < keyPagesNeeded; i++) {
                    keyPages[i] = grabPage();
                }
                assert keyPages[keyOffset(mask) >> PAGE_SHIFT] != null;

                int idPagesNeeded = (capacity * ID_SIZE - 1) >> PAGE_SHIFT;
                idPagesNeeded++;
                idPages = new byte[idPagesNeeded][];
                for (int i = 0; i < idPagesNeeded; i++) {
                    idPages[i] = grabPage();
                }
                assert idPages[idOffset(mask) >> PAGE_SHIFT] != null;
                success = true;
            } finally {
                if (false == success) {
                    close();
                }
            }
        }

        /**
         * Probe chunks for the value.
         * <p>
         *     Each probe is:
         * </p>
         * <ol>
         *     <li>Build a bit mask of all matching control values.</li>
         *     <li>If any match, check if the actual values. If any of those match, return them.</li>
         *     <li>No values matched, so check the control values for EMPTY flags. If there are any then the value isn't in the hash.</li>
         *     <li>There aren't any EMPTY flags, meaning this chunk is full. So we should continue probing.</li>
         * </ol>
         * <p>
         *     We probe via triangle numbers, adding 1, then 2, then 3, then 4, etc. That'll
         *     help protect us from chunky hashes. And it's simple math. And it'll hit all the
         *     buckets (<a href="https://fgiesen.wordpress.com/2015/02/22/triangular-numbers-mod-2n/">proof</a>).
         *     The probe loop doesn't stop if it never finds an EMPTY flag. But it'll always
         *     find one because we keep a load factor lower than 100%.
         * </p>
         */
        private int find(long key, int hash, byte control) {
            int slotIncrement = 0;
            int slot = slot(hash);
            while (true) {
                VectorMask<Byte> candidateMatches = controlMatches(slot, control);
                // TODO the double checking could be vectorized for some key types. Longs, probably.

                int first;
                while ((first = candidateMatches.firstTrue()) < candidateMatches.length()) {
                    int checkSlot = slot(slot + first);

                    if (key(checkSlot) == key) {
                        return id(checkSlot);
                    }
                    // Clear the first set bit and try again
                    candidateMatches = candidateMatches.indexInRange(-1 - first, candidateMatches.length());
                }

                if (controlMatches(slot, CONTROL_EMPTY).anyTrue()) {
                    return -1;
                }

                slotIncrement += BS.length();
                slot = slot(slot + slotIncrement);
            }
        }

        int add(long key, int hash) {
            byte control = control(hash);
            int found = find(key, hash, control);
            if (found >= 0) {
                return found;
            }

            currentSize++;
            if (currentSize >= nextGrowSize) {
                assert currentSize == nextGrowSize;
                grow();
            }

            int id = idSpace.next();
            bigCore.insert(key, hash, control, id);
            return id;
        }

        /**
         * Insert the key into the first empty slot that allows it. Used by {@link #add}
         * after we verify that the key isn't in the index. And used by {@link #rehash}
         * because we know all keys are unique.
         */
        void insert(long key, int hash, byte control, int id) {
            int slotIncrement = 0;
            int slot = slot(hash);
            while (true) {
                VectorMask<Byte> empty = controlMatches(slot, CONTROL_EMPTY);
                if (empty.anyTrue()) {
                    slot = slot(slot + empty.firstTrue());
                    int keyOffset = keyOffset(slot);
                    int idOffset = idOffset(slot);

                    longHandle.set(keyPages[keyOffset >> PAGE_SHIFT], keyOffset & PAGE_MASK, key);
                    intHandle.set(idPages[idOffset >> PAGE_SHIFT], idOffset & PAGE_MASK, id);
                    controlData[slot] = control;
                    /*
                     * Mirror the first BS.length bytes to the end of the array. All
                     * other positions are just written twice.
                     */
                    controlData[((slot - BS.length()) & mask) + BS.length()] = control;
                    return;
                }

                slotIncrement += BS.length();
                slot = slot(slot + slotIncrement);
                insertProbes++;
            }
        }

        @Override
        protected Status status() {
            return new BigCoreStatus(growCount, capacity, currentSize, nextGrowSize, insertProbes, keyPages.length, idPages.length);
        }

        @Override
        protected Itr iterator() {
            return new Itr() {
                @Override
                public boolean next() {
                    do {
                        slot++;
                    } while (slot < capacity && controlData[slot] == CONTROL_EMPTY);
                    return slot < capacity;
                }

                @Override
                public int id() {
                    return BigCore.this.id(slot);
                }

                @Override
                public long key() {
                    return BigCore.this.key(slot);
                }
            };
        }

        private void grow() {
            int oldCapacity = growTracking();
            try {
                bigCore = new BigCore();
                rehash(oldCapacity);
            } finally {
                close();
            }
        }

        private void rehash(int oldCapacity) {
            int slot = 0;
            while (slot < oldCapacity) {
                VectorMask<Byte> empty = controlMatches(slot, CONTROL_EMPTY);
                // TODO iterate like in find - it's faster.
                for (int i = 0; i < empty.length(); i++) {
                    if (empty.laneIsSet(i)) {
                        slot++;
                        continue;
                    }
                    long key = key(slot);
                    int hash = hash(key);
                    int id = id(slot);
                    bigCore.insert(key, hash, control(hash), id);
                    slot++;
                }
            }
        }

        /**
         * Checks the control byte at {@code slot} and the next few bytes ahead
         * of {@code slot} for the control bits. The extra probed bytes is as
         * many as will fit in your widest simd instruction. So, 32 or 64 will
         * be common.
         */
        private VectorMask<Byte> controlMatches(int slot, byte control) {
            return ByteVector.fromArray(BS, controlData, slot).eq(control);
        }

        private long key(int slot) {
            int keyOffset = keyOffset(slot);
            return (long) longHandle.get(keyPages[keyOffset >> PAGE_SHIFT], keyOffset & PAGE_MASK);
        }

        private int id(int slot) {
            int idOffset = idOffset(slot);
            return (int) intHandle.get(idPages[idOffset >> PAGE_SHIFT], idOffset & PAGE_MASK);
        }
    }

    int keyOffset(int slot) {
        return slot * KEY_SIZE;
    }

    int idOffset(int slot) {
        return slot * ID_SIZE;
    }

    int hash(long v) {
        return BitMixer.mix(v);
    }

    int slot(int hash) {
        return hash & mask;
    }
}
