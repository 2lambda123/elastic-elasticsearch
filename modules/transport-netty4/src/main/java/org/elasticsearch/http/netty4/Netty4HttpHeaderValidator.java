/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.TriConsumer;

import java.util.ArrayDeque;

public class Netty4HttpHeaderValidator extends ChannelInboundHandlerAdapter {

    public static final TriConsumer<HttpRequest, Channel, ActionListener<Void>> NOOP_VALIDATOR = ((
        httpRequest,
        channel,
        listener) -> listener.onResponse(null));

    private final TriConsumer<HttpRequest, Channel, ActionListener<Void>> validator;
    private ArrayDeque<HttpObject> pending = new ArrayDeque<>(4);
    private STATE state = STATE.WAITING_TO_START;

    public Netty4HttpHeaderValidator(TriConsumer<HttpRequest, Channel, ActionListener<Void>> validator) {
        this.validator = validator;
    }

    STATE getState() {
        return state;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        assert msg instanceof HttpObject;
        final HttpObject httpObject = (HttpObject) msg;

        if (state == STATE.WAITING_TO_START) {
            assert pending.isEmpty();
            pending.add(ReferenceCountUtil.retain(httpObject));
            requestStart(ctx);
        } else if (state == STATE.QUEUEING_DATA) {
            pending.add(ReferenceCountUtil.retain(httpObject));
        } else if (state == STATE.HANDLING_QUEUED_DATA) {
            pending.add(ReferenceCountUtil.retain(httpObject));
            // Immediately return as this can only happen from a reentrant read(). We do not want to change
            // autoread in this case.
            return;
        } else if (state == STATE.FORWARDING_DATA) {
            assert pending.isEmpty();
            if (httpObject instanceof LastHttpContent) {
                state = STATE.WAITING_TO_START;
            }
            ctx.fireChannelRead(httpObject);
        } else if (state == STATE.DROPPING_DATA_PERMANENTLY || state == STATE.DROPPING_DATA_UNTIL_NEXT_REQUEST) {
            assert pending.isEmpty();
            if (state == STATE.DROPPING_DATA_UNTIL_NEXT_REQUEST && httpObject instanceof LastHttpContent) {
                state = STATE.WAITING_TO_START;
            }
        } else {
            throw new AssertionError("Unknown state: " + state);
        }

        ctx.channel().config().setAutoRead(shouldRead());
    }

    private void requestStart(ChannelHandlerContext ctx) {
        assert pending.isEmpty() == false;

        HttpObject httpObject = pending.getFirst();
        assert httpObject instanceof HttpRequest;
        // We failed in the decoding step for other reasons. Pass down the pipeline.
        if (httpObject.decoderResult().isFailure()) {
            ctx.fireChannelRead(pending.pollFirst());
            return;
        }

        state = STATE.QUEUEING_DATA;
        validator.apply((HttpRequest) httpObject, ctx.channel(), new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                // Always use "Submit" to prevent reentrancy concerns if we are still on event loop
                ctx.channel().eventLoop().submit(() -> validationSuccess(ctx));
            }

            @Override
            public void onFailure(Exception e) {
                // Always use "Submit" to prevent reentrancy concerns if we are still on event loop
                ctx.channel().eventLoop().submit(() -> validationFailure(ctx, e));
            }
        });
    }

    private void validationSuccess(ChannelHandlerContext ctx) {
        assert ctx.channel().eventLoop().inEventLoop();
        state = STATE.HANDLING_QUEUED_DATA;

        int pendingMessages = pending.size();

        boolean fullRequestForwarded = false;
        HttpObject object;
        while ((object = pending.poll()) != null) {
            ctx.fireChannelRead(object);
            ReferenceCountUtil.release(object);
            if (object instanceof LastHttpContent) {
                fullRequestForwarded = true;
                break;
            }
        }
        if (pending.size() <= 4 && pendingMessages > 32) {
            // Prevent the ArrayDeque from becoming forever large due to a single large message.
            ArrayDeque<HttpObject> old = pending;
            pending = new ArrayDeque<>(4);
            pending.addAll(old);
        }

        if (fullRequestForwarded) {
            state = STATE.WAITING_TO_START;
            if (pending.isEmpty() == false) {
                requestStart(ctx);
            }
        } else {
            state = STATE.FORWARDING_DATA;
        }

        ctx.channel().config().setAutoRead(shouldRead());
    }

    private void validationFailure(ChannelHandlerContext ctx, Exception e) {
        assert ctx.channel().eventLoop().inEventLoop();
        state = STATE.HANDLING_QUEUED_DATA;
        HttpMessage messageToForward = (HttpMessage) pending.remove();
        boolean fullRequestConsumed;
        if (messageToForward instanceof LastHttpContent toRelease) {
            messageToForward = (HttpMessage) toRelease.replace(Unpooled.EMPTY_BUFFER);
            toRelease.release();
            fullRequestConsumed = true;
        } else {
            fullRequestConsumed = dropData();
        }
        messageToForward.setDecoderResult(DecoderResult.failure(e));
        ctx.fireChannelRead(messageToForward);

        if (pending.isEmpty() == false && fullRequestConsumed == false) {
            // There should not be any data re-enqueued when we are dropping permanently
            assert state == STATE.HANDLING_QUEUED_DATA;
            fullRequestConsumed = dropData();
        }

        assert state == STATE.HANDLING_QUEUED_DATA;
        if (pending.isEmpty()) {
            if (fullRequestConsumed) {
                state = STATE.WAITING_TO_START;
            } else {
                state = STATE.DROPPING_DATA_UNTIL_NEXT_REQUEST;
            }
        } else {
            state = STATE.WAITING_TO_START;
            requestStart(ctx);
        }

        ctx.channel().config().setAutoRead(shouldRead());
    }

    private boolean dropData() {
        HttpObject toRelease;
        while ((toRelease = pending.poll()) != null) {
            ReferenceCountUtil.release(toRelease);
            if (toRelease instanceof LastHttpContent) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        state = STATE.DROPPING_DATA_PERMANENTLY;
        for (HttpObject msg : pending) {
            ReferenceCountUtil.release(msg);
        }
        super.channelInactive(ctx);
    }

    private boolean shouldRead() {
        return (state == STATE.QUEUEING_DATA || state == STATE.DROPPING_DATA_PERMANENTLY) == false;
    }

    enum STATE {
        WAITING_TO_START,
        QUEUEING_DATA,
        // This is an intermediate state in case an event handler down the line triggers a reentrant read().
        // Data will be intermittently queued for later handling while in this state.
        HANDLING_QUEUED_DATA,
        FORWARDING_DATA,
        DROPPING_DATA_UNTIL_NEXT_REQUEST,
        DROPPING_DATA_PERMANENTLY
    }
}
