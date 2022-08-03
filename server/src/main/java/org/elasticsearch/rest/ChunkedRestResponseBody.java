/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.rest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Streams;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.OutputStream;

/**
 * The body of a rest response that uses chunked HTTP encoding. Implementations are used to avoid materializing full responses on heap and
 * instead serialize only as much of the response as can be flushed to the network right away.
 */
public interface ChunkedRestResponseBody {

    /**
     * @return true once this response has been written fully.
     */
    boolean isDone();

    /**
     * Serializes approximately as many bytes of the response as request by {@code sizeHint} to a {@link ReleasableBytesReference} that
     * is created from buffers backed by the given {@code recycler}.
     *
     * @param sizeHint how many bytes to approximately serialize for the given chunk
     * @param recycler recycler used to acquire buffers
     * @return serialized chunk
     * @throws IOException on serialization failure
     */
    ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException;

    /**
     * Create a chunked response body to be written to a specific {@link RestChannel} from a {@link ChunkedToXContent}.
     *
     * @param chunkedToXContent chunked x-content instance to serialize
     * @param params parameters to use for serialization
     * @param channel channel the response will be written to
     * @return chunked rest response body
     */
    static ChunkedRestResponseBody fromXContent(ChunkedToXContent chunkedToXContent, ToXContent.Params params, RestChannel channel)
        throws IOException {

        return new ChunkedRestResponseBody() {

            private final OutputStream out = new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    target.write(b);
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    target.write(b, off, len);
                }
            };

            private final XContentBuilder builder = channel.newBuilder(
                channel.request().getXContentType(),
                null,
                true,
                Streams.noCloseStream(out)
            );

            private final ChunkedToXContent.ChunkedXContentSerialization serialization = chunkedToXContent.toXContentChunked(
                builder,
                params
            );

            private boolean done = false;

            private BytesStream target;

            @Override
            public boolean isDone() {
                return done;
            }

            @Override
            public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
                final RecyclerBytesStreamOutput chunkStream = new RecyclerBytesStreamOutput(recycler);
                assert this.target == null;
                this.target = chunkStream;
                while ((done = serialization.writeChunk()) == false) {
                    if (chunkStream.size() > sizeHint) {
                        break;
                    }
                }
                if (done) {
                    builder.close();
                }
                this.target = null;
                return new ReleasableBytesReference(chunkStream.bytes(), () -> IOUtils.closeWhileHandlingException(chunkStream));
            }
        };
    }
}
