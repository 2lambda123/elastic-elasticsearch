/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.blobstore.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.bytes.PagedBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.util.ByteArray;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link FilterInputStream} that copies over all the bytes read from the original input stream to a given {@link ByteArray}. The
 * number of bytes copied cannot exceed the size of the {@link ByteArray}.
 */
public class CopyOnReadInputStream extends FilterInputStream {

    private final ActionListener<ReleasableBytesReference> listener;
    private final AtomicBoolean closed;
    private final ByteArray bytes;

    private IOException failure;
    private long count;
    private long mark;

    public CopyOnReadInputStream(InputStream in, ByteArray byteArray, ActionListener<ReleasableBytesReference> listener) {
        super(in);
        this.listener = Objects.requireNonNull(listener);
        this.bytes = Objects.requireNonNull(byteArray);
        this.closed = new AtomicBoolean(false);
    }

    private <T> T handleFailure(CheckedSupplier<T, IOException> supplier) throws IOException {
        try {
            return supplier.get();
        } catch (IOException e) {
            assert failure == null;
            failure = e;
            throw e;
        }
    }

    public int read() throws IOException {
        final int result = handleFailure(super::read);
        if (result != -1) {
            if (count < bytes.size()) {
                bytes.set(count, (byte) result);
            }
            count++;
        }
        return result;
    }

    public int read(byte[] b, int off, int len) throws IOException {
        final int result = handleFailure(() -> super.read(b, off, len));
        if (result != -1) {
            if (count < bytes.size()) {
                bytes.set(count, b, off, Math.toIntExact(Math.min(bytes.size() - count, result)));
            }
            count += result;
        }
        return result;
    }

    @Override
    public long skip(long n) throws IOException {
        final long skip = handleFailure(() -> super.skip(n));
        // TODO BUG read the skipped bytes if they should go in the byte array
        if (skip > 0L) {
            count += skip;
        }
        return skip;
    }

    @Override
    public synchronized void mark(int readlimit) {
        super.mark(readlimit);
        mark = count;
    }

    @Override
    public synchronized void reset() throws IOException {
        handleFailure(() -> {
            super.reset();
            return null;
        });
        count = mark;
    }

    @Override
    public final void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            boolean success = false;
            try {
                if (failure == null) {
                    PagedBytesReference reference = new PagedBytesReference(bytes, Math.toIntExact(Math.min(count, bytes.size())));
                    // TODO notify listener as soon as bytes are available; likewise notify on failure if not; no need to fail if we
                    // got the bytes we wanted
                    listener.onResponse(new ReleasableBytesReference(reference, bytes));
                    success = true;
                } else {
                    listener.onFailure(failure);
                }
            } finally {
                if (success == false) {
                    bytes.close();
                }
                in.close();
            }
        }
    }
}
