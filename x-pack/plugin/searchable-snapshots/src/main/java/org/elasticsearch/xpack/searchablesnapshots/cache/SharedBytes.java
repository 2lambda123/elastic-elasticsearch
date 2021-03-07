/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.RefCounted;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.snapshots.SharedCacheConfiguration;
import org.elasticsearch.snapshots.SnapshotUtils;
import org.elasticsearch.snapshots.SnapshotsService;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public class SharedBytes extends AbstractRefCounted {

    private static final Logger logger = LogManager.getLogger(SharedBytes.class);

    private static final StandardOpenOption[] OPEN_OPTIONS = new StandardOpenOption[] {
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE };

    final SharedCacheConfiguration sharedCacheConfiguration;

    // TODO: for systems like Windows without true p-write/read support we should split this up into multiple channels since positional
    // operations in #IO are not contention-free there (https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6265734)
    private final FileChannel fileChannel;

    private final Path path;

    SharedBytes(SharedCacheConfiguration sharedCacheConfiguration, Environment environment) throws IOException {
        super("shared-bytes");
        this.sharedCacheConfiguration = sharedCacheConfiguration;
        final long fileSize = sharedCacheConfiguration.totalSize();
        Path cacheFile = null;
        if (fileSize > 0) {
            cacheFile = SnapshotUtils.findCacheSnapshotCacheFilePath(environment, fileSize);
            if (cacheFile == null) {
                throw new IOException("Could not find a directory with adequate free space for cache file");
            }
            // TODO: maybe make this faster by allocating a larger direct buffer if this is too slow for very large files
            // We fill either the full file or the bytes between its current size and the desired size once with zeros to fully allocate
            // the file up front
            final ByteBuffer fillBytes = ByteBuffer.allocate(Channels.WRITE_CHUNK_SIZE);
            this.fileChannel = FileChannel.open(cacheFile, OPEN_OPTIONS);
            long written = fileChannel.size();
            if (fileSize < written) {
                logger.info("creating shared snapshot cache file [size={}, path={}]", fileSize, cacheFile);
            } else if (fileSize == written) {
                logger.debug("reusing existing shared snapshot cache file [size={}, path={}]", fileSize, cacheFile);
            }
            fileChannel.position(written);
            while (written < fileSize) {
                final int toWrite = Math.toIntExact(Math.min(fileSize - written, Channels.WRITE_CHUNK_SIZE));
                fillBytes.position(0).limit(toWrite);
                Channels.writeToChannel(fillBytes, fileChannel);
                written += toWrite;
            }
            if (written > fileChannel.size()) {
                fileChannel.truncate(fileSize);
            }
        } else {
            this.fileChannel = null;
            for (Path path : environment.dataFiles()) {
                Files.deleteIfExists(path.resolve(SnapshotsService.CACHE_FILE_NAME));
            }
        }
        this.path = cacheFile;
    }

    @Override
    protected void closeInternal() {
        try {
            IOUtils.close(fileChannel, path == null ? null : () -> Files.deleteIfExists(path));
        } catch (IOException e) {
            logger.warn("Failed to clean up shared bytes file", e);
        }
    }

    private final Map<Integer, IO> ios = ConcurrentCollections.newConcurrentMap();

    IO getFileChannel(int sharedBytesPos) {
        assert fileChannel != null;
        return ios.compute(sharedBytesPos, (p, io) -> {
            if (io == null || io.tryIncRef() == false) {
                final IO newIO;
                boolean success = false;
                incRef();
                try {
                    newIO = new SingleIO(p);
                    success = true;
                } finally {
                    if (success == false) {
                        decRef();
                    }
                }
                return newIO;
            }
            return io;
        });
    }

    long getPhysicalOffset(long chunkPosition) {
        return sharedCacheConfiguration.getPhysicalOffset(chunkPosition);
    }

    public interface IO extends RefCounted {

        int read(ByteBuffer dst, long position) throws IOException;

        int write(ByteBuffer src, long position) throws IOException;
    }

    private final class SingleIO extends AbstractRefCounted implements IO {

        private final int sharedBytesPos;
        private final long pageStart;

        private SingleIO(final int sharedBytesPos) {
            super("shared-bytes-io");
            this.sharedBytesPos = sharedBytesPos;
            pageStart = getPhysicalOffset(sharedBytesPos);
        }

        @SuppressForbidden(reason = "Use positional reads on purpose")
        public int read(ByteBuffer dst, long position) throws IOException {
            checkOffsets(position, dst.remaining());
            return fileChannel.read(dst, position);
        }

        @SuppressForbidden(reason = "Use positional writes on purpose")
        public int write(ByteBuffer src, long position) throws IOException {
            checkOffsets(position, src.remaining());
            return fileChannel.write(src, position);
        }

        private void checkOffsets(long position, long length) {
            final long regionSize = sharedCacheConfiguration.regionSizeBySharedPageNumber(sharedBytesPos);
            long pageEnd = pageStart + regionSize;
            if (position < pageStart || position > pageEnd || position + length > pageEnd) {
                assert false;
                throw new IllegalArgumentException("bad access");
            }
        }

        @Override
        protected void closeInternal() {
            ios.remove(sharedBytesPos, this);
            SharedBytes.this.decRef();
        }
    }
}
