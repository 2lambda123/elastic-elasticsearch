/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.dlm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ResultDeduplicator;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.scheduler.TimeValueSchedule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;

import java.io.Closeable;
import java.time.Clock;
import java.util.List;
import java.util.function.LongSupplier;

import static org.elasticsearch.core.Strings.format;

/**
 * This service will implement the needed actions (e.g. rollover, retention) to manage the data streams with a DLM lifecycle configured.
 * It runs on the master node and it schedules a job according to the configured {@link DataLifecycleService#DLM_POLL_INTERVAL_SETTING}.
 */
public class DataLifecycleService implements ClusterStateListener, Closeable, SchedulerEngine.Listener {

    public static final String DLM_POLL_INTERVAL = "indices.dlm.poll_interval";
    public static final Setting<TimeValue> DLM_POLL_INTERVAL_SETTING = Setting.timeSetting(
        DLM_POLL_INTERVAL,
        TimeValue.timeValueMinutes(10),
        TimeValue.timeValueSeconds(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private static final Logger logger = LogManager.getLogger(DataLifecycleService.class);
    /**
     * Name constant for the job DLM schedules
     */
    private static final String DATA_LIFECYCLE_JOB_NAME = "dlm";

    private final Settings settings;
    private final Client client;
    private final ClusterService clusterService;
    private final ResultDeduplicator<TransportRequest, Void> transportActionsDeduplicator;
    private final LongSupplier nowSupplier;
    private final Clock clock;
    private volatile boolean isMaster = false;
    private volatile TimeValue pollInterval;
    private SchedulerEngine.Job scheduledJob;
    private final SetOnce<SchedulerEngine> scheduler = new SetOnce<>();

    public DataLifecycleService(
        Settings settings,
        Client client,
        ClusterService clusterService,
        Clock clock,
        ThreadPool threadPool,
        LongSupplier nowSupplier
    ) {
        this.settings = settings;
        this.client = client;
        this.clusterService = clusterService;
        this.clock = clock;
        this.transportActionsDeduplicator = new ResultDeduplicator<>(threadPool.getThreadContext());
        this.nowSupplier = nowSupplier;
        this.scheduledJob = null;
        this.pollInterval = DLM_POLL_INTERVAL_SETTING.get(settings);
        clusterService.addListener(this);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DLM_POLL_INTERVAL_SETTING, this::updatePollInterval);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // wait for the cluster state to be recovered
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        final boolean prevIsMaster = this.isMaster;
        if (prevIsMaster != event.localNodeMaster()) {
            this.isMaster = event.localNodeMaster();
            if (this.isMaster) {
                // we weren't the master, and now we are
                onBecomeMaster();
            } else {
                // we were the master, and now we aren't
                cancelJob();
            }
        }
    }

    @Override
    public void close() {
        SchedulerEngine engine = scheduler.get();
        if (engine != null) {
            engine.stop();
        }
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        if (event.getJobName().equals(DATA_LIFECYCLE_JOB_NAME)) {
            logger.trace("DLM job triggered: " + event.getJobName() + ", " + event.getScheduledTime() + ", " + event.getTriggeredTime());
            run(clusterService.state());
        }
    }

    /**
     * Iterates over the DLM managed data streams and executes the needed operations
     * to satisfy the configured Lifecycle.
     */
    // default visibility for testing purposes
    void run(ClusterState state) {
        for (DataStream dataStream : state.metadata().dataStreams().values()) {
            if (dataStream.getLifecycle() == null) {
                continue;
            }

            IndexMetadata writeIndex = state.metadata().index(dataStream.getWriteIndex());
            if (writeIndex != null && isManagedByDLM(dataStream, writeIndex)) {
                RolloverRequest rolloverRequest = getDefaultRolloverRequest(dataStream);
                transportActionsDeduplicator.executeOnce(
                    rolloverRequest,
                    ActionListener.noop(),
                    (req, reqListener) -> rolloverDataStream(rolloverRequest, reqListener)
                );
            }

            TimeValue retention = getRetentionConfiguration(dataStream);
            if (retention != null) {
                List<Index> backingIndices = dataStream.getIndices();
                // we'll look at the current write index in the next run if it's rolled over (and not the write index anymore)
                for (int i = 0; i < backingIndices.size() - 1; i++) {
                    IndexMetadata backingIndex = state.metadata().index(backingIndices.get(i));
                    if (backingIndex == null || isManagedByDLM(dataStream, backingIndex) == false) {
                        continue;
                    }

                    TimeValue indexLifecycleDate = getCreationOrRolloverDate(dataStream.getName(), backingIndex);

                    long nowMillis = nowSupplier.getAsLong();
                    if (nowMillis >= indexLifecycleDate.getMillis() + retention.getMillis()) {
                        // there's an opportunity here to batch the delete requests (i.e. delete 100 indices / request)
                        // let's start simple and reevaluate
                        DeleteIndexRequest deleteRequest = new DeleteIndexRequest(backingIndex.getIndex().getName()).masterNodeTimeout(
                            TimeValue.MAX_VALUE
                        );

                        // time to delete the index
                        transportActionsDeduplicator.executeOnce(
                            deleteRequest,
                            ActionListener.noop(),
                            (req, reqListener) -> deleteIndex(deleteRequest, reqListener)
                        );
                    }
                }
            }
        }
    }

    private void rolloverDataStream(RolloverRequest rolloverRequest, ActionListener<Void> listener) {
        // "saving" the rollover target name here so we don't capture the entire request
        String rolloverTarget = rolloverRequest.getRolloverTarget();
        client.admin().indices().rolloverIndex(rolloverRequest, new ActionListener<>() {
            @Override
            public void onResponse(RolloverResponse rolloverResponse) {
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(() -> format("DLM rollover of [%s] failed", rolloverTarget), e);
                listener.onFailure(e);
            }
        });
    }

    private void deleteIndex(DeleteIndexRequest deleteIndexRequest, ActionListener<Void> listener) {
        assert deleteIndexRequest.indices() != null && deleteIndexRequest.indices().length == 1 : "DLM deletes one index at a time";
        // "saving" the index name here so we don't capture the entire request
        String targetIndex = deleteIndexRequest.indices()[0];
        client.admin().indices().delete(deleteIndexRequest, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                // we're capturin
                logger.error(() -> format("DLM request to delete [%s] failed", targetIndex), e);
                listener.onFailure(e);
            }
        });
    }

    @Nullable
    static TimeValue getRetentionConfiguration(DataStream dataStream) {
        TimeValue retention = null;
        if (dataStream.getLifecycle() != null) {
            retention = dataStream.getLifecycle().getDataRetention();
        }
        return retention;
    }

    /**
     * Calculate the age of the index since creation or rollover time if the index was already rolled.
     * The rollover target is the data stream name the index is a part of.
     */
    static TimeValue getCreationOrRolloverDate(String rolloverTarget, IndexMetadata index) {
        RolloverInfo rolloverInfo = index.getRolloverInfos().get(rolloverTarget);
        if (rolloverInfo != null) {
            return TimeValue.timeValueMillis(rolloverInfo.getTime());
        } else {
            return TimeValue.timeValueMillis(index.getCreationDate());
        }
    }

    /**
     * This is quite a shallow method but the purpose of its existence is to have only one place to modify once we
     * introduce the index.lifecycle.prefer_ilm setting. Once the prefer_ilm setting exists the method will also
     * make more sense as it will encapsulate a bit more logic.
     */
    private static boolean isManagedByDLM(DataStream parentDataStream, IndexMetadata indexMetadata) {
        return indexMetadata.getLifecyclePolicyName() == null && parentDataStream.getLifecycle() != null;
    }

    private RolloverRequest getDefaultRolloverRequest(DataStream dataStream) {
        RolloverRequest rolloverRequest = new RolloverRequest(dataStream.getName(), null).masterNodeTimeout(TimeValue.MAX_VALUE);

        // TODO get rollover from cluster setting once we have it
        rolloverRequest.addMaxIndexAgeCondition(TimeValue.timeValueDays(30));
        rolloverRequest.addMaxPrimaryShardSizeCondition(ByteSizeValue.ofGb(50));
        rolloverRequest.addMaxPrimaryShardDocsCondition(200_000_000);
        // don't rollover an empty index
        rolloverRequest.addMinIndexDocsCondition(1);
        return rolloverRequest;
    }

    private void onBecomeMaster() {
        maybeScheduleJob();
    }

    private void updatePollInterval(TimeValue newInterval) {
        this.pollInterval = newInterval;
        maybeScheduleJob();
    }

    private void cancelJob() {
        if (scheduler.get() != null) {
            scheduler.get().remove(DATA_LIFECYCLE_JOB_NAME);
            scheduledJob = null;
        }
    }

    private boolean isClusterServiceStoppedOrClosed() {
        final Lifecycle.State state = clusterService.lifecycleState();
        return state == Lifecycle.State.STOPPED || state == Lifecycle.State.CLOSED;
    }

    private void maybeScheduleJob() {
        if (this.isMaster) {
            if (scheduler.get() == null) {
                // don't create scheduler if the node is shutting down
                if (isClusterServiceStoppedOrClosed() == false) {
                    scheduler.set(new SchedulerEngine(settings, clock));
                    scheduler.get().register(this);
                }
            }

            // scheduler could be null if the node might be shutting down
            if (scheduler.get() != null) {
                scheduledJob = new SchedulerEngine.Job(DATA_LIFECYCLE_JOB_NAME, new TimeValueSchedule(pollInterval));
                scheduler.get().add(scheduledJob);
            }
        }
    }
}
