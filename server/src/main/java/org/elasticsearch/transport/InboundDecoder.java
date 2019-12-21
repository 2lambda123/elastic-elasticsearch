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

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.IOException;

public class InboundDecoder implements Releasable {

    static final ReleasableBytesReference END_CONTENT = new ReleasableBytesReference(BytesArray.EMPTY, () -> {});

    private final InboundAggregator aggregator;
    private final PageCacheRecycler recycler;
    private TransportDecompressor decompressor;
    private int networkMessageSize = -1;
    private int bytesConsumed = 0;

    public InboundDecoder(InboundAggregator aggregator) {
        this(aggregator, PageCacheRecycler.NON_RECYCLING_INSTANCE);
    }

    public InboundDecoder(InboundAggregator aggregator, PageCacheRecycler recycler) {
        this.aggregator = aggregator;
        this.recycler = recycler;
    }

    public int handle(TcpChannel channel, ReleasableBytesReference reference) throws IOException {
        try (reference) {
            if (isOnHeader()) {
                int messageLength = TcpTransport.readMessageLength(reference);
                if (messageLength == -1) {
                    return 0;
                } else if (messageLength == 0) {
                    aggregator.pingReceived(channel);
                    return 6;
                } else {
                    if (reference.length() < TcpHeader.BYTES_REQUIRED_FOR_VERSION) {
                        return 0;
                    } else {
                        int decoderHeaderSize = decoderHeaderSize(reference);
                        if (reference.length() < decoderHeaderSize) {
                            return 0;
                        } else {
                            networkMessageSize = messageLength + TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE;

                            Header header = readHeader(networkMessageSize, reference);
                            bytesConsumed += decoderHeaderSize;
                            if (header.isCompressed()) {
                                decompressor = new TransportDecompressor(recycler);
                            }
                            aggregator.headerReceived(header);
                            return bytesConsumed;
                        }

                    }
                }
            } else {
                int bytesToConsume = Math.min(reference.length(), networkMessageSize - bytesConsumed);
                bytesConsumed += bytesToConsume;
                ReleasableBytesReference retainedContent;
                if (isDone()) {
                    retainedContent = reference.retainedSlice(0, bytesToConsume);
                } else {
                    retainedContent = reference.retain();
                }
                if (decompressor != null) {
                    decompress(retainedContent);
                    ReleasableBytesReference decompressed;
                    while ((decompressed = decompressor.pollDecompressedPage()) != null) {
                        forwardNonEmptyContent(channel, decompressed);
                    }
                } else {
                    forwardNonEmptyContent(channel, retainedContent);
                }
                if (isDone()) {
                    decompressor = null;
                    networkMessageSize = -1;
                    bytesConsumed = 0;
                    aggregator.contentReceived(channel, END_CONTENT);
                }

                return bytesToConsume;
            }
        }
    }

    @Override
    public void close() {
        IOUtils.closeWhileHandlingException(decompressor);
        decompressor = null;
        networkMessageSize = -1;
        bytesConsumed = 0;
    }

    private void forwardNonEmptyContent(TcpChannel channel, ReleasableBytesReference content) {
        // Do not bother forwarding empty content
        if (content.length() == 0) {
            content.close();
        } else {
            aggregator.contentReceived(channel, content);
        }
    }

    private void decompress(ReleasableBytesReference content) throws IOException {
        try (content) {
            int consumed = decompressor.decompress(content);
            assert consumed == content.length();
        }
    }

    private boolean isDone() {
        return bytesConsumed == networkMessageSize;
    }

    private int decoderHeaderSize(BytesReference bytesReference) {
        Version remoteVersion = Version.fromId(bytesReference.getInt(TcpHeader.VERSION_POSITION));
        if (remoteVersion.before(TcpHeader.VERSION_WITH_HEADER_SIZE)) {
            return TcpHeader.headerSize(remoteVersion);
        } else {
            int variableHeaderSize = bytesReference.getInt(TcpHeader.VARIABLE_HEADER_SIZE_POSITION);
            return TcpHeader.headerSize(remoteVersion) + variableHeaderSize;
        }
    }

    private Header readHeader(int networkMessageSize, BytesReference bytesReference) throws IOException {
        try (StreamInput streamInput = bytesReference.streamInput()) {
            streamInput.skip(TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE);
            long requestId = streamInput.readLong();
            byte status = streamInput.readByte();
            Version remoteVersion = Version.fromId(streamInput.readInt());
            Header header = new Header(networkMessageSize, requestId, status, remoteVersion);
            if (remoteVersion.onOrAfter(TcpHeader.VERSION_WITH_HEADER_SIZE)) {
                header.finishParsingHeader(streamInput);
            }
            return header;
        }
    }

    private boolean isOnHeader() {
        return networkMessageSize == -1;
    }
}
