/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.readiness;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.http.HttpServerTransport;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ReadinessService extends AbstractLifecycleComponent implements ClusterStateListener {
    private static final String READINESS_SERVICE_THREADPOOL_NAME = "readiness-service";
    private static final String READINESS_WORKER_THREADPOOL_NAME = "readiness-worker";
    private static final Logger logger = LogManager.getLogger(ReadinessService.class);
    private static final int RESPONSE_TIMEOUT_MILLIS = 1_000;

    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "readiness.service.enabled",
        true,
        Setting.Property.NodeScope
    );

    private final Environment environment;
    private final ExecutorService workerExecutor;
    private final ExecutorService executor;

    private ServerSocketChannel serverChannel;

    private volatile boolean ready = false;
    private final boolean enabled;

    private final HttpServerTransport httpTransport;

    public ReadinessService(Environment environment, HttpServerTransport httpTransport) {
        this.httpTransport = httpTransport;
        this.serverChannel = null;
        this.environment = environment;
        this.enabled = ENABLED_SETTING.get(environment.settings());
        if (enabled) {
            this.executor = EsExecutors.newFixed(
                READINESS_SERVICE_THREADPOOL_NAME,
                1,
                1000,
                EsExecutors.daemonThreadFactory("elasticsearch[readiness-service]"),
                new ThreadContext(environment.settings()),
                false
            );

            this.workerExecutor = EsExecutors.newScaling(
                READINESS_WORKER_THREADPOOL_NAME,
                0,
                EsExecutors.allocatedProcessors(environment.settings()) * 2,
                60,
                TimeUnit.SECONDS,
                false,
                EsExecutors.daemonThreadFactory("elasticsearch[readiness-worker]"),
                new ThreadContext(environment.settings())
            );
        } else {
            logger.info("readiness service disabled");
            this.workerExecutor = null;
            this.executor = null;
        }
    }

    Path getSocketPath() {
        return environment.logsFile().resolve("readiness.socket");
    }

    ServerSocketChannel setupUnixDomainSocket() {
        Path socketPath = getSocketPath();

        try {
            Files.deleteIfExists(socketPath);
            UnixDomainSocketAddress socketAddress = UnixDomainSocketAddress.of(socketPath);
            ServerSocketChannel serverChannel = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                try {
                    serverChannel.bind(socketAddress);
                } catch (IOException e) {
                    throw new IllegalStateException("I/O exception while trying to bind the unix domain socket", e);
                }
                return null;
            });

            return serverChannel;
        } catch (IOException e) {
            throw new IllegalStateException("I/O exception while trying to create unix domain socket", e);
        }
    }

    @Override
    protected void doStart() {
        if (enabled == false) {
            return;
        }

        this.serverChannel = setupUnixDomainSocket();

        executor.execute(wrapReadinessService(() -> {
            while (serverChannel.isOpen()) {
                AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                    try (SocketChannel channel = serverChannel.accept()) {
                        List<Callable<Void>> responders = List.of(() -> {
                            sendStatus(channel);
                            return null;
                        });
                        workerExecutor.invokeAll(responders, RESPONSE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                    } catch (IOException | InterruptedException e) {
                        logger.debug("encountered exception while responding to readiness check request", e);
                    }
                    return null;
                });
            }
        }, e -> logger.error("error starting readiness service", e)));

        logger.info("readiness service up and running");
    }

    @Override
    protected void doStop() {
        if (enabled == false) {
            return;
        }
        try {
            if (this.serverChannel != null) {
                this.serverChannel.close();
            }
        } catch (IOException e) {
            logger.warn("error closing readiness service channel", e);
        } finally {
            this.ready = false;
            logger.info("readiness service stopped");
        }
    }

    @Override
    protected void doClose() throws IOException {
        try {
            Files.deleteIfExists(getSocketPath());
        } catch (IOException e) {
            logger.warn("error cleaning up readiness service socket file", e);
        }
    }

    void sendStatus(SocketChannel channel) {
        try {
            BoundTransportAddress boundAddress = httpTransport.boundAddress();
            StringBuilder sb = new StringBuilder(Boolean.toString(ready));

            if (boundAddress != null && boundAddress.publishAddress() != null) {
                sb.append(',').append(boundAddress.publishAddress().getPort());
            }

            Channels.writeToChannel(sb.toString().getBytes(StandardCharsets.UTF_8), channel);
        } catch (IOException e) {
            logger.warn("encountered I/O exception while responding to readiness client", e);
        }
    }

    private static AbstractRunnable wrapReadinessService(Runnable run, Consumer<Exception> exceptionConsumer) {
        return new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                exceptionConsumer.accept(e);
            }

            @Override
            protected void doRun() {
                run.run();
            }
        };
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (enabled == false) {
            return;
        }
        this.ready = event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false;
    }
}
