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

package org.elasticsearch.nio;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NioSocketChannelTests extends ESTestCase {

    private SocketSelector selector;
    private Thread thread;

    @Before
    @SuppressWarnings("unchecked")
    public void startSelector() throws IOException {
        selector = new SocketSelector(new SocketEventHandler(logger));
        thread = new Thread(selector::runLoop);
        thread.start();
        FutureUtils.get(selector.isRunningFuture());
    }

    @After
    public void stopSelector() throws IOException, InterruptedException {
        selector.close();
        thread.join();
    }

    @SuppressWarnings("unchecked")
    public void testClose() throws Exception {
        AtomicBoolean isClosed = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        try(SocketChannel rawChannel = SocketChannel.open()) {
            NioSocketChannel socketChannel = new NioSocketChannel(rawChannel, selector);
            socketChannel.setContext(new BytesChannelContext(socketChannel, mock(BiConsumer.class),
                mock(SocketChannelContext.ReadConsumer.class), InboundChannelBuffer.allocatingInstance()));
            socketChannel.addCloseListener(ActionListener.toBiConsumer(new ActionListener<Void>() {
                @Override
                public void onResponse(Void o) {
                    isClosed.set(true);
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    isClosed.set(true);
                    latch.countDown();
                }
            }));

            assertTrue(socketChannel.isOpen());
            assertTrue(rawChannel.isOpen());
            assertFalse(isClosed.get());

            PlainActionFuture<Void> closeFuture = PlainActionFuture.newFuture();
            socketChannel.addCloseListener(ActionListener.toBiConsumer(closeFuture));
            selector.queueChannelClose(socketChannel);
            closeFuture.actionGet();

            assertFalse(rawChannel.isOpen());
            assertFalse(socketChannel.isOpen());
            latch.await();
            assertTrue(isClosed.get());
        }
    }

    @SuppressWarnings("unchecked")
    public void testConnectSucceeds() throws Exception {
        SocketChannel rawChannel = mock(SocketChannel.class);
        when(rawChannel.finishConnect()).thenReturn(true);
        NioSocketChannel socketChannel = new DoNotRegisterChannel(rawChannel, selector);
        socketChannel.setContext(mock(SocketChannelContext.class));
        selector.scheduleForRegistration(socketChannel);

        PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
        socketChannel.addConnectListener(ActionListener.toBiConsumer(connectFuture));
        connectFuture.get(100, TimeUnit.SECONDS);

        assertTrue(socketChannel.isConnectComplete());
        assertTrue(socketChannel.isOpen());
    }

    @SuppressWarnings("unchecked")
    public void testConnectFails() throws Exception {
        SocketChannel rawChannel = mock(SocketChannel.class);
        when(rawChannel.finishConnect()).thenThrow(new ConnectException());
        NioSocketChannel socketChannel = new DoNotRegisterChannel(rawChannel, selector);
        socketChannel.setContext(mock(SocketChannelContext.class));
        selector.scheduleForRegistration(socketChannel);

        PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
        socketChannel.addConnectListener(ActionListener.toBiConsumer(connectFuture));
        ExecutionException e = expectThrows(ExecutionException.class, () -> connectFuture.get(100, TimeUnit.SECONDS));
        assertTrue(e.getCause() instanceof IOException);

        assertFalse(socketChannel.isConnectComplete());
        // Even if connection fails the channel is 'open' until close() is called
        assertTrue(socketChannel.isOpen());
    }
}
