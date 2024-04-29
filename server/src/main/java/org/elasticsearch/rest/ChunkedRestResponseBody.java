/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.rest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.Streams;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * The body of a rest response that uses chunked HTTP encoding. Implementations are used to avoid materializing full responses on heap and
 * instead serialize only as much of the response as can be flushed to the network right away.
 */
public interface ChunkedRestResponseBody {

    Logger logger = LogManager.getLogger(ChunkedRestResponseBody.class);

    /**
     * @return {@code true} if this body contains no more chunks and the REST layer should check for a possible continuation by calling
     * {@link #isEndOfResponse}, or {@code false} if the REST layer should request another chunk from this body using {@link #encodeChunk}.
     */
    boolean isDone();

    /**
     * @return {@code true} if this is the last chunked body in the response, or {@code false} if the REST layer should request further
     * chunked bodies by calling {@link #getContinuation}.
     */
    boolean isEndOfResponse();

    /**
     * <p>Asynchronously retrieves the next part of the body. Called if {@link #isEndOfResponse} returns {@code false}.</p>
     *
     * <p>Note that this is called on a transport thread, so implementations must take care to dispatch any nontrivial work elsewhere.</p>

     * <p>Note that the {@link Task} corresponding to any invocation of {@link Client#execute} completes as soon as the client action
     * returns its response, so it no longer exists when this method is called and cannot be used to receive cancellation notifications.
     * Instead, if the HTTP channel is closed while sending a response then the REST layer will invoke {@link RestResponse#close}. If the
     * HTTP channel is closed while the REST layer is waiting for a continuation then the {@link RestResponse} will not be closed until the
     * continuation listener is completed. Implementations will typically explicitly create a {@link CancellableTask} to represent the
     * computation and transmission of the entire {@link RestResponse}, and will cancel this task if the {@link RestResponse} is closed
     * prematurely.</p>
     *
     * @param listener Listener to complete with the next part of the body. By the point this is called we have already started to send
     *                 the body of the response, so there's no good ways to handle an exception here. Completing the listener exceptionally
     *                 will log an error, abort sending the response, and close the HTTP connection.
     */
    void getContinuation(ActionListener<ChunkedRestResponseBody> listener);

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
     * @return the response Content-Type header value for this response body
     */
    String getResponseContentTypeString();

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

            private final Iterator<? extends ToXContent> serialization = builder.getRestApiVersion() == RestApiVersion.V_7
                ? chunkedToXContent.toXContentChunkedV7(params)
                : chunkedToXContent.toXContentChunked(params);

            private BytesStream target;

            @Override
            public boolean isDone() {
                return serialization.hasNext() == false;
            }

            @Override
            public boolean isEndOfResponse() {
                return true;
            }

            @Override
            public void getContinuation(ActionListener<ChunkedRestResponseBody> listener) {
                assert false : "no continuations";
                listener.onFailure(new IllegalStateException("no continuations available"));
            }

            @Override
            public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
                try {
                    final RecyclerBytesStreamOutput chunkStream = new RecyclerBytesStreamOutput(recycler);
                    assert target == null;
                    target = chunkStream;
                    while (serialization.hasNext()) {
                        serialization.next().toXContent(builder, params);
                        if (chunkStream.size() >= sizeHint) {
                            break;
                        }
                    }
                    if (serialization.hasNext() == false) {
                        builder.close();
                    }
                    final var result = new ReleasableBytesReference(
                        chunkStream.bytes(),
                        () -> Releasables.closeExpectNoException(chunkStream)
                    );
                    target = null;
                    return result;
                } catch (Exception e) {
                    logger.error("failure encoding chunk", e);
                    throw e;
                } finally {
                    if (target != null) {
                        assert false : "failure encoding chunk";
                        IOUtils.closeWhileHandlingException(target);
                        target = null;
                    }
                }
            }

            @Override
            public String getResponseContentTypeString() {
                return builder.getResponseContentTypeString();
            }
        };
    }

    /**
     * Create a chunked response body to be written to a specific {@link RestChannel} from a stream of text chunks, each represented as a
     * consumer of a {@link Writer}.
     */
    static ChunkedRestResponseBody fromTextChunks(String contentType, Iterator<CheckedConsumer<Writer, IOException>> chunkIterator) {
        return new ChunkedRestResponseBody() {
            private RecyclerBytesStreamOutput currentOutput;
            private final Writer writer = new OutputStreamWriter(new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    assert currentOutput != null;
                    currentOutput.write(b);
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    assert currentOutput != null;
                    currentOutput.write(b, off, len);
                }

                @Override
                public void flush() {
                    assert currentOutput != null;
                    currentOutput.flush();
                }

                @Override
                public void close() {
                    assert currentOutput != null;
                    currentOutput.flush();
                }
            }, StandardCharsets.UTF_8);

            @Override
            public boolean isDone() {
                return chunkIterator.hasNext() == false;
            }

            @Override
            public boolean isEndOfResponse() {
                return true;
            }

            @Override
            public void getContinuation(ActionListener<ChunkedRestResponseBody> listener) {
                assert false : "no continuations";
                listener.onFailure(new IllegalStateException("no continuations available"));
            }

            @Override
            public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
                try {
                    assert currentOutput == null;
                    currentOutput = new RecyclerBytesStreamOutput(recycler);

                    while (chunkIterator.hasNext() && currentOutput.size() < sizeHint) {
                        chunkIterator.next().accept(writer);
                    }

                    if (chunkIterator.hasNext()) {
                        writer.flush();
                    } else {
                        writer.close();
                    }

                    final var chunkOutput = currentOutput;
                    final var result = new ReleasableBytesReference(
                        chunkOutput.bytes(),
                        () -> Releasables.closeExpectNoException(chunkOutput)
                    );
                    currentOutput = null;
                    return result;
                } catch (Exception e) {
                    logger.error("failure encoding text chunk", e);
                    throw e;
                } finally {
                    if (currentOutput != null) {
                        assert false : "failure encoding text chunk";
                        Releasables.closeExpectNoException(currentOutput);
                        currentOutput = null;
                    }
                }
            }

            @Override
            public String getResponseContentTypeString() {
                return contentType;
            }
        };
    }
}
