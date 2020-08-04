/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation.logging;

import co.elastic.logging.log4j2.EcsLayout;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.ECSJsonLayout;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.RateLimitingFilter;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;

import java.util.function.Consumer;

/**
 * This component manages the construction and lifecycle of the {@link DeprecationIndexingAppender}.
 * It also starts and stops the appender
 */
public class DeprecationIndexingComponent extends AbstractLifecycleComponent implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(DeprecationIndexingComponent.class);

    public static final Setting<Boolean> WRITE_DEPRECATION_LOGS_TO_INDEX = Setting.boolSetting(
        "cluster.deprecation_indexing.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final DeprecationIndexingAppender appender;
    private final BulkProcessor processor;

    public DeprecationIndexingComponent(ThreadPool threadPool, Client client) {
        this.processor = getBulkProcessor(new OriginSettingClient(client, ClientHelper.DEPRECATION_ORIGIN));
        final Consumer<IndexRequest> consumer = buildIndexRequestConsumer(threadPool);

        final LoggerContext context = (LoggerContext) LogManager.getContext(false);
        final Configuration configuration = context.getConfiguration();

        final EcsLayout ecsLayout = ECSJsonLayout.newBuilder().setType("deprecation").setConfiguration(configuration).build();

        this.appender = new DeprecationIndexingAppender("deprecation_indexing_appender", new RateLimitingFilter(), ecsLayout, consumer);
    }

    @Override
    protected void doStart() {
        this.appender.start();
        Loggers.addAppender(LogManager.getLogger("org.elasticsearch.deprecation"), this.appender);
    }

    @Override
    protected void doStop() {
        Loggers.removeAppender(LogManager.getLogger("org.elasticsearch.deprecation"), this.appender);
        this.appender.stop();
    }

    @Override
    protected void doClose() {
        this.processor.close();
    }

    /**
     * Listens for changes to the cluster state, in order to know whether to toggle indexing
     * and to set the cluster UUID and node ID. These can't be set in the constructor because
     * the initial cluster state won't be set yet.
     *
     * @param event the cluster state event to process
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final ClusterState state = event.state();
        appender.setEnabled(WRITE_DEPRECATION_LOGS_TO_INDEX.get(state.getMetadata().settings()));
    }

    /**
     * Constructs a {@link Consumer} that knows what to do with the {@link IndexRequest} instances that the
     * {@link DeprecationIndexingAppender} creates. This logic is separated from the service in order to make
     * testing significantly easier, and to separate concerns.
     * <p>
     * Writes are done via {@link BulkProcessor}, which handles batching up writes and retries.
     *
     * @param threadPool due to <a href="https://github.com/elastic/elasticsearch/issues/50440">#50440</a>,
     *                   extra care must be taken to avoid blocking the thread that writes a deprecation message.
     * @return           a consumer that accepts an index request and handles all the details of writing it
     *                   into the cluster
     */
    private Consumer<IndexRequest> buildIndexRequestConsumer(ThreadPool threadPool) {
        return indexRequest -> {
            try {
                // TODO: remove the threadpool wrapping when the .add call is non-blocking
                // (it can currently execute the bulk request occasionally)
                // see: https://github.com/elastic/elasticsearch/issues/50440
                threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> this.processor.add(indexRequest));
            } catch (Exception e) {
                logger.error("Failed to queue deprecation message index request: " + e.getMessage(), e);
            }
        };
    }

    /**
     * Constructs a bulk processor for writing documents
     * @param client the client to use
     * @return an initialised bulk processor
     */
    private BulkProcessor getBulkProcessor(Client client) {
        final OriginSettingClient originSettingClient = new OriginSettingClient(client, ClientHelper.DEPRECATION_ORIGIN);

        final BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {}

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {}

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.error("Bulk write of deprecation logs failed: " + failure.getMessage(), failure);
            }
        };

        return BulkProcessor.builder(originSettingClient::bulk, listener)
            .setBulkActions(100)
            .setFlushInterval(TimeValue.timeValueSeconds(5))
            .build();
    }
}
