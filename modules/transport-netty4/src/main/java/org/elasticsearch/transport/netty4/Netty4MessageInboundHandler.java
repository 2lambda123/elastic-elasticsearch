/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.transport.Compression;
import org.elasticsearch.transport.Header;
import org.elasticsearch.transport.InboundDecoder;
import org.elasticsearch.transport.InboundMessage;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TcpHeader;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportDecompressor;
import org.elasticsearch.transport.TransportHandshaker;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * A handler (must be the last one!) that does size based frame decoding and forwards the actual message
 * to the relevant action.
 */
public class Netty4MessageInboundHandler extends ChannelInboundHandlerAdapter {

    private static final InboundMessage PING_MESSAGE = new InboundMessage(null, true);

    private final InboundDecoder decoder;
    private Exception uncaughtException;
    private final ArrayDeque<ByteBuf> pending = new ArrayDeque<>(2);
    private boolean isClosed = false;

    private int totalNetworkSize = -1;
    private int bytesConsumed = 0;

    private boolean isCompressed = false;

    private TransportDecompressor decompressor;

    private final Netty4Transport transport;

    private final InboundDecoder.ChannelType channelType = InboundDecoder.ChannelType.MIX;

    private static final ByteSizeValue maxHeaderSize = new ByteSizeValue(2, ByteSizeUnit.GB);

    private final Supplier<CircuitBreaker> circuitBreaker;
    private final Predicate<String> requestCanTripBreaker;

    private ByteBuf cummulation;
    private Header currentHeader;
    private Exception aggregationException;
    private boolean canTripBreaker = true;

    private boolean isOnHeader() {
        return totalNetworkSize == -1;
    }

    public Netty4MessageInboundHandler(Netty4Transport transport) {
        this.transport = transport;
        this.circuitBreaker = transport.getInflightBreaker();
        this.requestCanTripBreaker = actionName -> {
            final RequestHandlerRegistry<TransportRequest> reg = transport.getRequestHandlers().getHandler(actionName);
            if (reg == null) {
                assert transport.ignoreDeserializationErrors() : actionName;
                throw new ActionNotFoundTransportException(actionName);
            } else {
                return reg.canTripCircuitBreaker();
            }
        };
        this.decoder = new InboundDecoder(transport.recycler());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        assert Transports.assertDefaultThreadContext(transport.getThreadPool().getThreadContext());
        assert Transports.assertTransportThread();
        assert msg instanceof ByteBuf : "Expected message type ByteBuf, found: " + msg.getClass();

        final ByteBuf buffer = (ByteBuf) msg;
        Netty4TcpChannel channel = ctx.channel().attr(Netty4Transport.CHANNEL_KEY).get();
        handleBytes(channel, buffer);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        assert Transports.assertDefaultThreadContext(transport.getThreadPool().getThreadContext());
        ExceptionsHelper.maybeDieOnAnotherThread(cause);
        final Throwable unwrapped = ExceptionsHelper.unwrap(cause, ElasticsearchException.class);
        final Throwable newCause = unwrapped != null ? unwrapped : cause;
        Netty4TcpChannel tcpChannel = ctx.channel().attr(Netty4Transport.CHANNEL_KEY).get();
        if (newCause instanceof Error) {
            transport.onException(tcpChannel, new Exception(newCause));
        } else {
            transport.onException(tcpChannel, (Exception) newCause);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        isClosed = true;
        Releasables.closeExpectNoException(decoder, this::closeCurrentAggregation, () -> pending.forEach(ByteBuf::release), pending::clear);
        super.channelInactive(ctx);
    }

    public void handleBytes(TcpChannel channel, ByteBuf reference) throws IOException {
        if (uncaughtException != null) {
            throw new IllegalStateException("Pipeline state corrupted by uncaught exception", uncaughtException);
        }
        try {
            doHandleBytes(channel, reference);
        } catch (Exception e) {
            uncaughtException = e;
            throw e;
        }
    }

    public void doHandleBytes(TcpChannel channel, ByteBuf reference) throws IOException {
        channel.getChannelStats().markAccessed(transport.getThreadPool().relativeTimeInMillis());
        transport.getStatsTracker().markBytesRead(reference.readableBytes());
        cummulation = ByteToMessageDecoder.MERGE_CUMULATOR.cumulate(
            NettyAllocator.getAllocator(),
            cummulation == null ? Unpooled.EMPTY_BUFFER : cummulation,
            reference
        );
        while (cummulation.readableBytes() > 0) {
            try {
                final int bytesDecoded = decode(cummulation, channel);
                if (bytesDecoded == 0) {
                    break;
                }
            } finally {
                cummulation.discardReadBytes();
            }
        }
    }

    private void endContent(TcpChannel channel) throws IOException {
        cleanDecodeState();
        assert isAggregating();
        InboundMessage aggregated = finishAggregation();
        try {
            transport.getStatsTracker().markMessageReceived();
            transport.inboundMessage(channel, aggregated);
        } finally {
            aggregated.decRef();
        }
    }

    private void forwardFragment(ByteBuf fragment) {
        assert isAggregating();
        aggregate(fragment);
    }

    public void headerReceived(Header header) {
        ensureOpen();
        assert isAggregating() == false;
        assert cummulation == null;
        currentHeader = header;
        if (currentHeader.isRequest() && currentHeader.needsToReadVariableHeader() == false) {
            initializeRequestState();
        }
    }

    private void initializeRequestState() {
        assert currentHeader.needsToReadVariableHeader() == false;
        assert currentHeader.isRequest();
        if (currentHeader.isHandshake()) {
            canTripBreaker = false;
            return;
        }

        final String actionName = currentHeader.getActionName();
        try {
            canTripBreaker = requestCanTripBreaker.test(actionName);
        } catch (ActionNotFoundTransportException e) {
            shortCircuit(e);
        }
    }

    public void updateCompressionScheme(Compression.Scheme compressionScheme) {
        ensureOpen();
        assert isAggregating();
        assert cummulation == null;
        currentHeader.setCompressionScheme(compressionScheme);
    }

    public void aggregate(ByteBuf content) {
        ensureOpen();
        cummulation = ByteToMessageDecoder.MERGE_CUMULATOR.cumulate(
            NettyAllocator.getAllocator(),
            cummulation == null ? Unpooled.EMPTY_BUFFER : cummulation,
            content
        );
    }

    public InboundMessage finishAggregation() throws IOException {
        ensureOpen();
        final ReleasableBytesReference releasableContent = cummulation == null || cummulation.readableBytes() == 0
            ? ReleasableBytesReference.empty()
            : new ReleasableBytesReference(Netty4Utils.toBytesReference(cummulation), new ByteBufRefCounted(cummulation));

        final BreakerControl breakerControl = new BreakerControl(circuitBreaker);
        final InboundMessage aggregated = new InboundMessage(currentHeader, releasableContent, breakerControl);
        boolean success = false;
        try {
            if (aggregated.getHeader().needsToReadVariableHeader()) {
                aggregated.getHeader().finishParsingHeader(aggregated.openOrGetStreamInput());
                if (aggregated.getHeader().isRequest()) {
                    initializeRequestState();
                }
            }
            if (isShortCircuited() == false) {
                checkBreaker(aggregated.getHeader(), aggregated.getContentLength(), breakerControl);
            }
            if (isShortCircuited()) {
                aggregated.decRef();
                success = true;
                return new InboundMessage(aggregated.getHeader(), aggregationException);
            } else {
                assert uncompressedOrSchemeDefined(aggregated.getHeader());
                success = true;
                return aggregated;
            }
        } finally {
            resetCurrentAggregation();
            if (success == false) {
                aggregated.decRef();
            }
        }
    }

    private static boolean uncompressedOrSchemeDefined(Header header) {
        return header.isCompressed() == (header.getCompressionScheme() != null);
    }

    private void checkBreaker(final Header header, final int contentLength, final BreakerControl breakerControl) {
        if (header.isRequest() == false) {
            return;
        }
        assert header.needsToReadVariableHeader() == false;

        if (canTripBreaker) {
            try {
                circuitBreaker.get().addEstimateBytesAndMaybeBreak(contentLength, header.getActionName());
                breakerControl.setReservedBytes(contentLength);
            } catch (CircuitBreakingException e) {
                shortCircuit(e);
            }
        } else {
            circuitBreaker.get().addWithoutBreaking(contentLength);
            breakerControl.setReservedBytes(contentLength);
        }
    }

    public boolean isAggregating() {
        return currentHeader != null;
    }

    private void shortCircuit(Exception exception) {
        this.aggregationException = exception;
    }

    private boolean isShortCircuited() {
        return aggregationException != null;
    }

    private void closeCurrentAggregation() {
        releaseContent();
        resetCurrentAggregation();
    }

    private void releaseContent() {
        if (cummulation != null) {
            cummulation.release();
        }
    }

    private void resetCurrentAggregation() {
        cummulation = null;
        currentHeader = null;
        aggregationException = null;
        canTripBreaker = true;
    }

    private void releasePendingBytes(int bytesConsumed) {
        int bytesToRelease = bytesConsumed;
        while (bytesToRelease != 0) {
            ByteBuf reference = pending.pollFirst();
            try {
                assert reference != null;
                if (bytesToRelease < reference.readableBytes()) {
                    pending.addFirst(reference.retainedSlice(bytesToRelease, reference.readableBytes() - bytesToRelease));
                    bytesToRelease -= bytesToRelease;
                } else {
                    bytesToRelease -= reference.readableBytes();
                }
            } finally {
                reference.release();
            }
        }
    }

    public int decode(ByteBuf reference, TcpChannel channel) throws IOException {
        ensureOpen();
        try {
            return internalDecode(reference, channel);
        } catch (Exception e) {
            cleanDecodeState();
            throw e;
        }
    }

    public int internalDecode(ByteBuf byteBuf, TcpChannel channel) throws IOException {
        final ReleasableBytesReference reference = new ReleasableBytesReference(
            Netty4Utils.toBytesReference(byteBuf),
            new ByteBufRefCounted(byteBuf)
        );
        if (isOnHeader()) {
            int messageLength = TcpTransport.readMessageLength(reference);
            if (messageLength == -1) {
                return 0;
            } else if (messageLength == 0) {
                assert isAggregating() == false;
                transport.inboundMessage(channel, PING_MESSAGE);
                return 6;
            } else {
                int headerBytesToRead = headerBytesToRead(reference, maxHeaderSize);
                if (headerBytesToRead == 0) {
                    return 0;
                } else {
                    totalNetworkSize = messageLength + TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE;

                    Header header = readHeader(messageLength, reference, channelType);
                    bytesConsumed += headerBytesToRead;
                    if (header.isCompressed()) {
                        isCompressed = true;
                    }
                    headerReceived(header);

                    if (isDone()) {
                        endContent(channel);
                    }
                    return headerBytesToRead;
                }
            }
        } else {
            if (isCompressed && decompressor == null) {
                // Attempt to initialize decompressor
                TransportDecompressor decompressor = TransportDecompressor.getDecompressor(transport.recycler(), reference);
                if (decompressor == null) {
                    return 0;
                } else {
                    this.decompressor = decompressor;
                    assert isAggregating();
                    updateCompressionScheme(this.decompressor.getScheme());
                }
            }
            int remainingToConsume = totalNetworkSize - bytesConsumed;
            int maxBytesToConsume = Math.min(reference.length(), remainingToConsume);
            ReleasableBytesReference retainedContent;
            if (maxBytesToConsume == remainingToConsume) {
                retainedContent = reference.retainedSlice(0, maxBytesToConsume);
            } else {
                retainedContent = reference.retain();
            }

            int bytesConsumedThisDecode = 0;
            if (decompressor != null) {
                bytesConsumedThisDecode += decompress(retainedContent);
                bytesConsumed += bytesConsumedThisDecode;
                ReleasableBytesReference decompressed;
                while ((decompressed = decompressor.pollDecompressedPage(isDone())) != null) {
                    final ByteBuf dec = byteBuf.alloc().buffer(decompressed.length());
                    dec.writeBytes(decompressed.streamInput(), decompressed.length());
                    forwardFragment(dec);
                    decompressed.decRef();
                }
            } else {
                bytesConsumedThisDecode += maxBytesToConsume;
                bytesConsumed += maxBytesToConsume;
                forwardFragment(byteBuf);
            }
            if (isDone()) {
                endContent(channel);
            }

            return bytesConsumedThisDecode;
        }
    }

    private static int headerBytesToRead(BytesReference reference, ByteSizeValue maxHeaderSize) throws StreamCorruptedException {
        if (reference.length() < TcpHeader.BYTES_REQUIRED_FOR_VERSION) {
            return 0;
        }

        TransportVersion remoteVersion = TransportVersion.fromId(reference.getInt(TcpHeader.VERSION_POSITION));
        int fixedHeaderSize = TcpHeader.headerSize(remoteVersion);
        if (fixedHeaderSize > reference.length()) {
            return 0;
        } else if (remoteVersion.before(TcpHeader.VERSION_WITH_HEADER_SIZE)) {
            return fixedHeaderSize;
        } else {
            int variableHeaderSize = reference.getInt(TcpHeader.VARIABLE_HEADER_SIZE_POSITION);
            if (variableHeaderSize < 0) {
                throw new StreamCorruptedException("invalid negative variable header size: " + variableHeaderSize);
            }
            if (variableHeaderSize > maxHeaderSize.getBytes() - fixedHeaderSize) {
                throw new StreamCorruptedException(
                    "header size [" + (fixedHeaderSize + variableHeaderSize) + "] exceeds limit of [" + maxHeaderSize + "]"
                );
            }
            int totalHeaderSize = fixedHeaderSize + variableHeaderSize;
            if (totalHeaderSize > reference.length()) {
                return 0;
            } else {
                return totalHeaderSize;
            }
        }
    }

    private int decompress(ReleasableBytesReference content) throws IOException {
        try (content) {
            return decompressor.decompress(content);
        }
    }

    private static Header readHeader(int networkMessageSize, BytesReference bytesReference, InboundDecoder.ChannelType channelType)
        throws IOException {
        try (StreamInput streamInput = bytesReference.streamInput()) {
            streamInput.skip(TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE);
            long requestId = streamInput.readLong();
            byte status = streamInput.readByte();
            int remoteVersion = streamInput.readInt();

            Header header = new Header(networkMessageSize, requestId, status, TransportVersion.fromId(remoteVersion));
            if (channelType == InboundDecoder.ChannelType.SERVER && header.isResponse()) {
                throw new IllegalArgumentException("server channels do not accept inbound responses, only requests, closing channel");
            } else if (channelType == InboundDecoder.ChannelType.CLIENT && header.isRequest()) {
                throw new IllegalArgumentException("client channels do not accept inbound requests, only responses, closing channel");
            }
            if (header.isHandshake()) {
                checkHandshakeVersionCompatibility(header.getVersion());
            } else {
                checkVersionCompatibility(header.getVersion());
            }

            if (header.getVersion().onOrAfter(TcpHeader.VERSION_WITH_HEADER_SIZE)) {
                // Skip since we already have ensured enough data available
                streamInput.readInt();
                header.finishParsingHeader(streamInput);
            }
            return header;
        }
    }

    static void checkHandshakeVersionCompatibility(TransportVersion handshakeVersion) {
        if (TransportHandshaker.ALLOWED_HANDSHAKE_VERSIONS.contains(handshakeVersion) == false) {
            throw new IllegalStateException(
                "Received message from unsupported version: ["
                    + handshakeVersion
                    + "] allowed versions are: "
                    + TransportHandshaker.ALLOWED_HANDSHAKE_VERSIONS
            );
        }
    }

    static void checkVersionCompatibility(TransportVersion remoteVersion) {
        if (TransportVersion.isCompatible(remoteVersion) == false) {
            throw new IllegalStateException(
                "Received message from unsupported version: ["
                    + remoteVersion
                    + "] minimal compatible version is: ["
                    + TransportVersions.MINIMUM_COMPATIBLE
                    + "]"
            );
        }
    }

    private void cleanDecodeState() {
        try {
            Releasables.closeExpectNoException(decompressor);
        } finally {
            isCompressed = false;
            decompressor = null;
            totalNetworkSize = -1;
            bytesConsumed = 0;
        }
    }

    private boolean isDone() {
        return bytesConsumed == totalNetworkSize;
    }

    private void ensureOpen() {
        if (isClosed) {
            throw new IllegalStateException("Decoder is already closed");
        }
    }

    private record ByteBufRefCounted(ByteBuf buffer) implements RefCounted {

        @Override
        public void incRef() {
            buffer.retain();
        }

        @Override
        public boolean tryIncRef() {
            if (hasReferences() == false) {
                return false;
            }
            try {
                buffer.retain();
            } catch (RuntimeException e) {
                assert hasReferences() == false;
                return false;
            }
            return true;
        }

        @Override
        public boolean decRef() {
            return buffer.release();
        }

        @Override
        public boolean hasReferences() {
            return buffer.refCnt() > 0;
        }
    }

    private static class BreakerControl implements Releasable {

        private static final int CLOSED = -1;

        private final Supplier<CircuitBreaker> circuitBreaker;
        private final AtomicInteger bytesToRelease = new AtomicInteger(0);

        private BreakerControl(Supplier<CircuitBreaker> circuitBreaker) {
            this.circuitBreaker = circuitBreaker;
        }

        private void setReservedBytes(int reservedBytes) {
            final boolean set = bytesToRelease.compareAndSet(0, reservedBytes);
            assert set : "Expected bytesToRelease to be 0, found " + bytesToRelease.get();
        }

        @Override
        public void close() {
            final int toRelease = bytesToRelease.getAndSet(CLOSED);
            assert toRelease != CLOSED;
            if (toRelease > 0) {
                circuitBreaker.get().addWithoutBreaking(-toRelease);
            }
        }
    }
}
