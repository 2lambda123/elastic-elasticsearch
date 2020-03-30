/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store.cache;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.store.IndexInputStats;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.carrotsearch.randomizedtesting.generators.RandomNumbers.randomIntBetween;
import static com.carrotsearch.randomizedtesting.generators.RandomPicks.randomFrom;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public final class TestUtils {
    private TestUtils() {
    }

    static CacheService createCacheService(final Random random) {
        final ByteSizeValue cacheSize = new ByteSizeValue(randomIntBetween(random, 1, 100),
            randomFrom(random, List.of(ByteSizeUnit.BYTES, ByteSizeUnit.KB, ByteSizeUnit.MB, ByteSizeUnit.GB)));
        return new CacheService(cacheSize, randomCacheRangeSize(random));
    }

    static ByteSizeValue randomCacheRangeSize(final Random random) {
        return new ByteSizeValue(randomIntBetween(random, 1, 100),
            randomFrom(random, List.of(ByteSizeUnit.BYTES, ByteSizeUnit.KB, ByteSizeUnit.MB)));
    }

    static long numberOfRanges(long fileSize, long rangeSize) {
        return numberOfRanges(Math.toIntExact(fileSize), Math.toIntExact(rangeSize));
    }

    static long numberOfRanges(int fileSize, int rangeSize) {
        long numberOfRanges = fileSize / rangeSize;
        if (fileSize % rangeSize > 0) {
            numberOfRanges++;
        }
        if (numberOfRanges == 0) {
            numberOfRanges++;
        }
        return numberOfRanges;
    }

    public static void assertCounter(IndexInputStats.Counter counter, long total, long count, long min, long max) {
        assertThat(counter.total(), equalTo(total));
        assertThat(counter.count(), equalTo(count));
        assertThat(counter.min(), equalTo(min));
        assertThat(counter.max(), equalTo(max));
    }

    /**
     * A {@link BlobContainer} that can read a single in-memory blob.
     * Any attempt to read a different blob will throw a {@link FileNotFoundException}
     */
    static BlobContainer singleBlobContainer(final String blobName, final byte[] blobContent) {
        return new BlobContainer() {

            @Override
            public InputStream readBlob(String name, long position, long length) throws IOException {
                if (blobName.equals(name) == false) {
                    throw new FileNotFoundException("Blob not found: " + name);
                }
                return Streams.limitStream(new ByteArrayInputStream(blobContent, Math.toIntExact(position), blobContent.length), length);
            }

            @Override
            public long readBlobPreferredLength() {
                return Long.MAX_VALUE;
            }

            @Override
            public Map<String, BlobMetaData> listBlobs() {
                throw unsupportedException();
            }

            @Override
            public BlobPath path() {
                throw unsupportedException();
            }

            @Override
            public InputStream readBlob(String blobName) {
                throw unsupportedException();
            }

            @Override
            public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) {
                throw unsupportedException();
            }

            @Override
            public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) {
                throw unsupportedException();
            }

            @Override
            public DeleteResult delete() {
                throw unsupportedException();
            }

            @Override
            public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) {
                throw unsupportedException();
            }

            @Override
            public Map<String, BlobContainer> children() {
                throw unsupportedException();
            }

            @Override
            public Map<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix) {
                throw unsupportedException();
            }

            private UnsupportedOperationException unsupportedException() {
                assert false : "this operation is not supported and should have not be called";
                return new UnsupportedOperationException("This operation is not supported");
            }
        };
    }
}
