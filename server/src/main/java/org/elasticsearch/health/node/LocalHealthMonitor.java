/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.metadata.HealthMetadata;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.health.node.selection.HealthNodeTaskExecutor;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * This class monitors the health of the node regarding the load on several resources.
 * Currently, it only checks for available disk space. Furthermore, it informs the health
 * node about the local health upon change or when a new node is detected.
 */
public class LocalHealthMonitor implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(LocalHealthMonitor.class);

    public static final Setting<TimeValue> INTERVAL_SETTING = Setting.timeSetting(
        "health.reporting.local.monitor.interval",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueSeconds(10),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final DiskCheck diskCheck;

    private volatile TimeValue monitorInterval;
    private volatile Scheduler.ScheduledCancellable scheduled;
    private volatile IndividualNodeHealth lastReportedHealth = null;
    private volatile boolean enabled;
    private volatile boolean healthMetadataInitialized;

    public LocalHealthMonitor(Settings settings, ClusterService clusterService, NodeService nodeService, ThreadPool threadPool) {
        this.threadPool = threadPool;
        this.monitorInterval = INTERVAL_SETTING.get(settings);
        this.enabled = HealthNodeTaskExecutor.ENABLED_SETTING.get(settings);
        this.clusterService = clusterService;
        this.diskCheck = new DiskCheck(nodeService);
        clusterService.addListener(this);
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(INTERVAL_SETTING, this::setMonitorInterval);
        clusterSettings.addSettingsUpdateConsumer(HealthNodeTaskExecutor.ENABLED_SETTING, this::setEnabled);
    }

    void setMonitorInterval(TimeValue monitorInterval) {
        this.monitorInterval = monitorInterval;
        if (scheduled != null && scheduled.cancel()) {
            scheduleNextRunIfEnabled(new TimeValue(1));
        }
    }

    void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (scheduled != null) {
            scheduled.cancel();
            if (enabled) {
                scheduleNextRunIfEnabled(new TimeValue(1));
            }
        }
    }

    private void scheduleNextRunIfEnabled(TimeValue time) {
        if (enabled && threadPool.scheduler().isShutdown() == false) {
            scheduled = threadPool.schedule(this::monitorHealth, time, ThreadPool.Names.MANAGEMENT);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // Wait until every node in the cluster is upgraded to 8.4.0 or later
        if (event.state().nodesIfRecovered().getMinNodeVersion().onOrAfter(Version.V_8_4_0)) {
            // Wait until the health metadata is available in the cluster state
            if (healthMetadataInitialized == false) {
                healthMetadataInitialized = HealthMetadata.getFromClusterState(event.state()) != null;
                if (healthMetadataInitialized) {
                    scheduleNextRunIfEnabled(TimeValue.timeValueMillis(1));
                }
            } else if (newHealthNodeSelected(event)) {
                sendHealth(lastReportedHealth);
            }
        }
    }

    private boolean newHealthNodeSelected(ClusterChangedEvent event) {
        DiscoveryNode previous = HealthNode.findHealthNode(event.previousState());
        DiscoveryNode current = HealthNode.findHealthNode(event.state());
        if (current == null) {
            return false;
        } else if (previous == null) {
            return true;
        } else {
            return current.getId().equals(previous.getId()) == false;
        }
    }

    // Visible for testing
    void monitorHealth() {
        ClusterState clusterState = clusterService.state();
        HealthMetadata healthMetadata = HealthMetadata.getFromClusterState(clusterState);
        assert healthMetadata != null : "health metadata should have been initialized.";
        IndividualNodeHealth previousHealth = this.lastReportedHealth;
        IndividualNodeHealth currentHealth = new IndividualNodeHealth(diskCheck.getHealth(healthMetadata, clusterState));
        if (currentHealth.equals(previousHealth) == false) {
            sendHealth(currentHealth);
        }
        scheduleNextRunIfEnabled(monitorInterval);
    }

    // This method is synchronized to ensure that we keep track of the last reported health state to the health node.
    // Note: this is for demonstration purposes, when we will be actually sending the information to the health node we might
    // choose a different way to keep it thread safe.
    private synchronized void sendHealth(IndividualNodeHealth currentHealth) {
        logger.info("Sending node health [{}] to health node", currentHealth);
        this.lastReportedHealth = currentHealth;
    }

    IndividualNodeHealth getLastReportedHealth() {
        return lastReportedHealth;
    }

    /**
     * Determines the disk health of this node by checking if it exceeds the thresholds defined in the health metadata.
     */
    static class DiskCheck {
        private final NodeService nodeService;

        DiskCheck(NodeService nodeService) {
            this.nodeService = nodeService;
        }

        IndividualNodeHealth.Disk getHealth(HealthMetadata healthMetadata, ClusterState clusterState) {
            DiscoveryNode node = clusterState.getNodes().getLocalNode();
            HealthMetadata.Disk diskMetadata = healthMetadata.getDiskMetadata();
            DiskUsage usage = getDiskUsage();
            if (usage == null) {
                return new IndividualNodeHealth.Disk(HealthStatus.UNKNOWN, IndividualNodeHealth.Disk.Cause.NODE_HAS_NO_DISK_STATS);
            }

            ByteSizeValue totalBytes = ByteSizeValue.ofBytes(usage.getTotalBytes());

            if (node.isDedicatedFrozenNode()) {
                long frozenFloodStageThreshold = diskMetadata.getFreeBytesFrozenFloodStageWatermark(totalBytes).getBytes();
                if (usage.getFreeBytes() < frozenFloodStageThreshold) {
                    logger.debug("flood stage disk watermark [{}] exceeded on {}", frozenFloodStageThreshold, usage);
                    return new IndividualNodeHealth.Disk(HealthStatus.RED, IndividualNodeHealth.Disk.Cause.FROZEN_NODE_OVER_FLOOD_STAGE_THRESHOLD);
                }
                return new IndividualNodeHealth.Disk(HealthStatus.GREEN);
            }

            long floodStageThreshold = diskMetadata.getFreeBytesFloodStageWatermark(totalBytes).getBytes();
            if (usage.getFreeBytes() < floodStageThreshold) {
                return new IndividualNodeHealth.Disk(HealthStatus.RED, IndividualNodeHealth.Disk.Cause.NODE_OVER_THE_FLOOD_STAGE_THRESHOLD);
            }

            long highThreshold = diskMetadata.getFreeBytesHighWatermark(totalBytes).getBytes();
            if (usage.getFreeBytes() < highThreshold && hasRelocatingShards(clusterState, node.getId()) == false) {
                return new IndividualNodeHealth.Disk(HealthStatus.YELLOW, IndividualNodeHealth.Disk.Cause.NODE_OVER_HIGH_THRESHOLD);
            }
            return new IndividualNodeHealth.Disk(HealthStatus.GREEN);
        }

        private DiskUsage getDiskUsage() {
            NodeStats nodeStats = nodeService.stats(
                CommonStatsFlags.NONE,
                false,
                false,
                false,
                false,
                true,
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                false
            );
            final String nodeId = nodeStats.getNode().getId();
            final String nodeName = nodeStats.getNode().getName();
            if (nodeStats.getFs() == null) {
                logger.debug("node [{}/{}] did not return any filesystem stats", nodeName, nodeId);
                return null;
            }

            FsInfo.Path leastAvailablePath = null;
            for (FsInfo.Path info : nodeStats.getFs()) {
                if (leastAvailablePath == null) {
                    leastAvailablePath = info;
                } else if (leastAvailablePath.getAvailable().getBytes() > info.getAvailable().getBytes()) {
                    leastAvailablePath = info;
                }
            }
            if (leastAvailablePath == null) {
                logger.debug("node [{}/{}] did not return any filesystem stats", nodeName, nodeId);
                return null;
            }
            if (leastAvailablePath.getTotal().getBytes() < 0) {
                logger.debug("node [{}/{}] reported negative total disk space", nodeName, nodeId);
                return null;
            }

            return new DiskUsage(
                nodeId,
                nodeName,
                leastAvailablePath.getPath(),
                leastAvailablePath.getTotal().getBytes(),
                leastAvailablePath.getAvailable().getBytes()
            );
        }

        private boolean hasRelocatingShards(ClusterState clusterState, String nodeId) {
            return clusterState.getRoutingNodes().node(nodeId).shardsWithState(ShardRoutingState.RELOCATING).isEmpty() == false;
        }
    }
}
