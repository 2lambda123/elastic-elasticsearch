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

package org.elasticsearch.index.translog.fs;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStreams;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class FsChannelSnapshot implements Translog.Snapshot {

    private final long id;

    private final int totalOperations;

    private final RafReference raf;

    private final FileChannel channel;

    private final long length;

    private Translog.Operation lastOperationRead = null;

    private int position = 0;

    private ByteBuffer cacheBuffer;

    private AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Create a snapshot of translog file channel. The length parameter should be consistent with totalOperations and point
     * at the end of the last operation in this snapshot.
     */
    public FsChannelSnapshot(long id, RafReference raf, long length, int totalOperations) throws FileNotFoundException {
        this.id = id;
        this.raf = raf;
        this.channel = raf.raf().getChannel();
        this.length = length;
        this.totalOperations = totalOperations;
    }

    @Override
    public long translogId() {
        return this.id;
    }

    @Override
    public long position() {
        return this.position;
    }

    @Override
    public long length() {
        return this.length;
    }

    @Override
    public int estimatedTotalOperations() {
        return this.totalOperations;
    }

    @Override
    public long lengthInBytes() {
        return length - position;
    }

    @Override
    public boolean hasNext() {
        try {
            if (position >= length) {
                return false;
            }
            if (cacheBuffer == null) {
                cacheBuffer = ByteBuffer.allocate(1024);
            }
            cacheBuffer.limit(4);
            int bytesRead = Channels.readFromFileChannel(channel, position, cacheBuffer);
            if (bytesRead < 0) {
                // the snapshot is acquired under a write lock. we should never read beyond the EOF
                throw new EOFException("read past EOF. pos [" + position + "] length: [" + cacheBuffer.limit() + "] end: [" + channel.size() + "]");
            }
            assert bytesRead == 4;
            cacheBuffer.flip();
            int opSize = cacheBuffer.getInt();
            position += 4;
            if ((position + opSize) > length) {
                // the snapshot is acquired under a write lock. we should never read beyond the EOF
                position -= 4;
                throw new EOFException("opSize of [" + opSize + "] pointed beyond EOF. position [" + position + "] length [" + length + "]");
            }
            if (cacheBuffer.capacity() < opSize) {
                cacheBuffer = ByteBuffer.allocate(opSize);
            }
            cacheBuffer.clear();
            cacheBuffer.limit(opSize);
            bytesRead = Channels.readFromFileChannel(channel, position, cacheBuffer);
            if (bytesRead < 0) {
                throw new EOFException("tried to read past EOF. opSize [" + opSize + "] position [" + position + "] length [" + length + "]");
            }
            cacheBuffer.flip();
            position += opSize;
            lastOperationRead = TranslogStreams.readTranslogOperation(new BytesStreamInput(cacheBuffer.array(), 0, opSize, true));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public Translog.Operation next() {
        return this.lastOperationRead;
    }

    @Override
    public void seekForward(long length) {
        this.position += length;
    }

    @Override
    public void close() throws ElasticsearchException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        raf.decreaseRefCount(true);
    }
}
