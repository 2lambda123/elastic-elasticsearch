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

package org.elasticsearch.transport.nio;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.transport.nio.channel.ChannelFactory;
import org.elasticsearch.transport.nio.channel.ConnectFuture;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.elasticsearch.transport.nio.channel.NioServerSocketChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

public class NioTransport extends TcpTransport<NioChannel> {

    public static final String TRANSPORT_WORKER_THREAD_NAME_PREFIX = Transports.NIO_TRANSPORT_WORKER_THREAD_NAME_PREFIX;
    public static final String TRANSPORT_ACCEPTOR_THREAD_NAME_PREFIX = Transports.NIO_TRANSPORT_ACCEPTOR_THREAD_NAME_PREFIX;

    public static final Setting<Integer> NIO_WORKER_COUNT =
        new Setting<>("transport.nio.worker_count",
            (s) -> Integer.toString(EsExecutors.numberOfProcessors(s) * 2),
            (s) -> Setting.parseInt(s, 1, "transport.nio.worker_count"), Setting.Property.NodeScope);

    public static final Setting<Integer> NIO_ACCEPTOR_COUNT =
        intSetting("transport.nio.acceptor_count", 1, 1, Setting.Property.NodeScope);

    protected final OpenChannels openChannels = new OpenChannels(logger);
    private final TcpReadHandler tcpReadHandler = new TcpReadHandler(this);
    private final ConcurrentMap<String, ChannelFactory> profileToChannelFactory = newConcurrentMap();
    private final ArrayList<AcceptingSelector> acceptors = new ArrayList<>();
    private final ArrayList<SocketSelector> socketSelectors = new ArrayList<>();
    private NioClient client;
    private int acceptorNumber;

    public NioTransport(Settings settings, ThreadPool threadPool, NetworkService networkService, BigArrays bigArrays,
                        NamedWriteableRegistry namedWriteableRegistry, CircuitBreakerService circuitBreakerService) {
        super("nio", settings, threadPool, bigArrays, circuitBreakerService, namedWriteableRegistry, networkService);
    }

    @Override
    public long getNumOpenServerConnections() {
        return openChannels.serverChannelsCount();
    }

    @Override
    protected InetSocketAddress getLocalAddress(NioChannel channel) {
        return channel.getLocalAddress();
    }

    @Override
    protected NioServerSocketChannel bind(String name, InetSocketAddress address) throws IOException {
        ChannelFactory channelFactory = this.profileToChannelFactory.get(name);
        AcceptingSelector selector = acceptors.get(++acceptorNumber % NioTransport.NIO_ACCEPTOR_COUNT.get(settings));
        return channelFactory.openNioServerSocketChannel(name, address, selector);
    }

    @Override
    protected void sendMessage(NioChannel channel, BytesReference reference, ActionListener<NioChannel> listener) {
        if (channel instanceof NioSocketChannel) {
            NioSocketChannel nioSocketChannel = (NioSocketChannel) channel;
            nioSocketChannel.getWriteContext().sendMessage(reference, listener);
        } else {
            logger.error("cannot send message to channel of this type [{}]", channel.getClass());
        }
    }

    @Override
    protected ConnectFuture initiateChannel(DiscoveryNode node, TimeValue connectTimeout)
        throws IOException {
        NioSocketChannel channel = client.initiateConnection(node.getAddress().address());
        if (channel == null) {
            throw new ElasticsearchException("client is shutdown");
        }
        return channel.getConnectFuture();
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            int workerCount = NioTransport.NIO_WORKER_COUNT.get(settings);
            for (int i = 0; i < workerCount; ++i) {
                SocketSelector selector = new SocketSelector(getSocketEventHandler());
                socketSelectors.add(selector);
            }

            for (SocketSelector selector : socketSelectors) {
                if (selector.isRunning() == false) {
                    ThreadFactory threadFactory = daemonThreadFactory(this.settings, TRANSPORT_WORKER_THREAD_NAME_PREFIX);
                    threadFactory.newThread(selector::runLoop).start();
                    selector.isRunningFuture().actionGet();
                }
            }

            client = createClient();

            if (NetworkService.NETWORK_SERVER.get(settings)) {
                int acceptorCount = NioTransport.NIO_ACCEPTOR_COUNT.get(settings);
                for (int i = 0; i < acceptorCount; ++i) {
                    Supplier<SocketSelector> selectorSupplier = new RoundRobinSelectorSupplier(socketSelectors);
                    AcceptorEventHandler eventHandler = new AcceptorEventHandler(logger, openChannels, selectorSupplier,
                        this::serverAcceptedChannel);
                    AcceptingSelector acceptor = new AcceptingSelector(eventHandler);
                    acceptors.add(acceptor);
                }

                for (AcceptingSelector acceptor : acceptors) {
                    if (acceptor.isRunning() == false) {
                        ThreadFactory threadFactory = daemonThreadFactory(this.settings, TRANSPORT_ACCEPTOR_THREAD_NAME_PREFIX);
                        threadFactory.newThread(acceptor::runLoop).start();
                        acceptor.isRunningFuture().actionGet();
                    }
                }

                // loop through all profiles and start them up, special handling for default one
                for (ProfileSettings profileSettings : profileSettings) {
                    profileToChannelFactory.putIfAbsent(profileSettings.profileName, new ChannelFactory(profileSettings, tcpReadHandler));
                    bindServer(profileSettings);
                }
            }

            super.doStart();
            success = true;
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        } finally {
            if (success == false) {
                doStop();
            }
        }
    }

    @Override
    protected void stopInternal() {
        NioShutdown nioShutdown = new NioShutdown(logger);
        nioShutdown.orderlyShutdown(openChannels, client, acceptors, socketSelectors);

        profileToChannelFactory.clear();
        socketSelectors.clear();
    }

    protected SocketEventHandler getSocketEventHandler() {
        return new SocketEventHandler(logger, this::exceptionCaught, openChannels);
    }

    final void exceptionCaught(NioSocketChannel channel, Throwable cause) {
        final Throwable unwrapped = ExceptionsHelper.unwrap(cause, ElasticsearchException.class);
        final Throwable t = unwrapped != null ? unwrapped : cause;
        onException(channel, t instanceof Exception ? (Exception) t : new ElasticsearchException(t));
    }

    private NioClient createClient() {
        Supplier<SocketSelector> selectorSupplier = new RoundRobinSelectorSupplier(socketSelectors);
        ChannelFactory channelFactory = new ChannelFactory(new ProfileSettings(settings, "default"), tcpReadHandler);
        return new NioClient(openChannels, selectorSupplier, channelFactory);
    }
}
