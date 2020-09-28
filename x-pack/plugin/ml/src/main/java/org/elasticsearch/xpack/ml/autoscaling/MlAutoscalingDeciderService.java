/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.autoscaling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingCapacity.AutoscalingResources;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderService;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecision;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.newSetFromMap;
import static org.elasticsearch.xpack.core.ml.MlTasks.getDataFrameAnalyticsState;
import static org.elasticsearch.xpack.core.ml.MlTasks.getJobStateModifiedForReassignments;
import static org.elasticsearch.xpack.ml.job.JobNodeSelector.AWAITING_LAZY_ASSIGNMENT;

public class MlAutoscalingDeciderService implements
    AutoscalingDeciderService<MlAutoscalingDeciderConfiguration>,
    LocalNodeMasterListener {

    private static final Logger logger = LogManager.getLogger(MlAutoscalingDeciderService.class);
    private static final Duration DEFAULT_MEMORY_REFRESH_RATE = Duration.ofMinutes(15);
    private static final String MEMORY_STALE = "unable to make scaling decision as job memory requirements are stale";

    private final NodeLoadDetector nodeLoadDetector;
    private final Set<String> anomalyJobsInQueue;
    private final Set<String> analyticsJobsInQueue;
    private final Supplier<Long> timeSupplier;

    private volatile boolean isMaster;
    private volatile boolean running;
    private volatile int maxMachineMemoryPercent;
    private volatile int maxOpenJobs;
    private volatile long lastTimeToScale;
    private volatile AutoscalingDecision lastMaxDecision;

    public MlAutoscalingDeciderService(MlMemoryTracker memoryTracker, Settings settings, ClusterService clusterService) {
        this(new NodeLoadDetector(memoryTracker), settings, clusterService, System::currentTimeMillis);
    }

    MlAutoscalingDeciderService(NodeLoadDetector nodeLoadDetector,
                                Settings settings,
                                ClusterService clusterService,
                                Supplier<Long> timeSupplier) {
        this.nodeLoadDetector = nodeLoadDetector;
        this.maxMachineMemoryPercent = MachineLearning.MAX_MACHINE_MEMORY_PERCENT.get(settings);
        this.maxOpenJobs = MachineLearning.MAX_OPEN_JOBS_PER_NODE.get(settings);
        this.analyticsJobsInQueue = newSetFromMap(new ConcurrentHashMap<>());
        this.anomalyJobsInQueue = newSetFromMap(new ConcurrentHashMap<>());
        this.timeSupplier = timeSupplier;
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MachineLearning.MAX_MACHINE_MEMORY_PERCENT,
            this::setMaxMachineMemoryPercent);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MachineLearning.MAX_OPEN_JOBS_PER_NODE, this::setMaxOpenJobs);
        clusterService.addLocalNodeMasterListener(this);
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void afterStart() {
                running = true;
                if (isMaster) {
                    nodeLoadDetector.getMlMemoryTracker().asyncRefresh();
                }
            }

            @Override
            public void beforeStop() {
                running = false;
            }
        });
    }

    void setMaxMachineMemoryPercent(int maxMachineMemoryPercent) {
        this.maxMachineMemoryPercent = maxMachineMemoryPercent;
    }

    void setMaxOpenJobs(int maxOpenJobs) {
        this.maxOpenJobs = maxOpenJobs;
    }

    @Override
    public void onMaster() {
        isMaster = true;
        if (running) {
            nodeLoadDetector.getMlMemoryTracker().asyncRefresh();
        }
    }

    static AutoscalingCapacity currentScale(final List<DiscoveryNode> nodes) {
        AutoscalingCapacity.AutoscalingResources tierResources = new AutoscalingCapacity.AutoscalingResources(
            null,
            new ByteSizeValue(nodes.stream()
                .map(MlAutoscalingDeciderService::getNodeMemory)
                .mapToLong(l -> l.orElse(0L))
                .sum())
        );
        AutoscalingCapacity.AutoscalingResources nodeResources = new AutoscalingCapacity.AutoscalingResources(
            null,
            new ByteSizeValue(nodes.stream()
                .map(MlAutoscalingDeciderService::getNodeMemory)
                .mapToLong(l -> l.orElse(-1L))
                .max()
                .orElse(0L))
        );
        return new AutoscalingCapacity(tierResources, nodeResources);
    }

    static OptionalLong getNodeMemory(DiscoveryNode node) {
        Map<String, String> nodeAttributes = node.getAttributes();
        OptionalLong machineMemory = OptionalLong.empty();
        String machineMemoryStr = nodeAttributes.get(MachineLearning.MACHINE_MEMORY_NODE_ATTR);
        try {
            machineMemory = OptionalLong.of(Long.parseLong(machineMemoryStr));
        } catch (NumberFormatException e) {
            logger.debug(() -> new ParameterizedMessage(
                "could not parse stored machine memory string value [{}] in node attribute [{}]",
                machineMemoryStr,
                MachineLearning.MACHINE_MEMORY_NODE_ATTR));
        }
        return machineMemory;
    }

    static List<DiscoveryNode> getNodes(final ClusterState clusterState) {
        return clusterState.nodes()
            .mastersFirstStream()
            .filter(MachineLearning::isMlNode)
            .collect(Collectors.toList());
    }

    @Override
    public void offMaster() {
        isMaster = false;
    }

    @Override
    public AutoscalingDecision scale(MlAutoscalingDeciderConfiguration decider, AutoscalingDeciderContext context) {
        if (isMaster == false) {
            throw new IllegalArgumentException("request for scaling information is only allowed on the master node");
        }
        final Duration memoryTrackingStale;
        long previousTimeStamp = this.lastTimeToScale;
        this.lastTimeToScale = this.timeSupplier.get();
        if (previousTimeStamp == 0L) {
            memoryTrackingStale = DEFAULT_MEMORY_REFRESH_RATE;
        } else {
            memoryTrackingStale = Duration.ofMillis(TimeValue.timeValueMinutes(1).millis() + this.lastTimeToScale - previousTimeStamp);
        }

        final ClusterState clusterState = context.state();

        PersistentTasksCustomMetadata tasks = clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        Collection<PersistentTask<?>> anomalyDetectionTasks = anomalyDetectionTasks(tasks);
        Collection<PersistentTask<?>> dataframeAnalyticsTasks = dataframeAnalyticsTasks(tasks);
        List<DiscoveryNode> nodes = getNodes(clusterState);
        List<String> waitingAnomalyJobs = anomalyDetectionTasks.stream()
            .filter(t -> AWAITING_LAZY_ASSIGNMENT.equals(t.getAssignment()))
            .map(t -> MlTasks.jobId(t.getId()))
            .collect(Collectors.toList());
        List<String> waitingAnalyticsJobs = dataframeAnalyticsTasks.stream()
            .filter(t -> AWAITING_LAZY_ASSIGNMENT.equals(t.getAssignment()))
            .map(t -> MlTasks.dataFrameAnalyticsId(t.getId()))
            .collect(Collectors.toList());

        final AutoscalingCapacity currentScale = currentScale(nodes);
        final MlScalingReason.Builder reasonBuilder = MlScalingReason.builder()
                .setWaitingAnomalyJobs(waitingAnomalyJobs)
                .setWaitingAnalyticsJobs(waitingAnalyticsJobs)
                .setCurrentMlCapacity(currentScale)
                .setPassedConfiguration(decider);

        final Optional<AutoscalingDecision> scaleUpDecision = checkForScaleUp(decider,
            nodes,
            waitingAnomalyJobs,
            waitingAnalyticsJobs,
            memoryTrackingStale,
            currentScale,
            reasonBuilder);

        if (scaleUpDecision.isPresent()) {
            return scaleUpDecision.get();
        }
        if (waitingAnalyticsJobs.isEmpty() == false || waitingAnomalyJobs.isEmpty() == false) {
            return new AutoscalingDecision(
                currentScale,
                reasonBuilder
                    .setSimpleReason("Passing currently perceived capacity as there are analytics and anomaly jobs in the queue, " +
                        "but the number in the queue is less than the configured maximum allowed.")
                    .build());
        }
        if (nodeLoadDetector.getMlMemoryTracker().isRecentlyRefreshed(memoryTrackingStale) == false) {
            nodeLoadDetector.getMlMemoryTracker().asyncRefresh();
            return new AutoscalingDecision(null, reasonBuilder.setSimpleReason(MEMORY_STALE).build());
        }

        return checkForScaleDown(decider, nodes, clusterState, reasonBuilder);
    }

    Optional<AutoscalingDecision> checkForScaleUp(MlAutoscalingDeciderConfiguration decider,
                                                  List<DiscoveryNode> nodes,
                                                  List<String> waitingAnomalyJobs,
                                                  List<String> waitingAnalyticsJobs,
                                                  Duration memoryTrackingStale,
                                                  AutoscalingCapacity currentScale,
                                                  MlScalingReason.Builder reasonBuilder) {
        anomalyJobsInQueue.retainAll(waitingAnomalyJobs);
        analyticsJobsInQueue.retainAll(waitingAnalyticsJobs);

        if (waitingAnalyticsJobs.size() > decider.getNumAnalyticsJobsInQueue()
            || waitingAnomalyJobs.size() > decider.getNumAnomalyJobsInQueue()) {
            long nodeMemory = currentScale.node().memory().getBytes();
            long tierMemory = currentScale.tier().memory().getBytes();
            Optional<AutoscalingCapacity> analyticsCapacity  = Optional.empty();
            Optional<AutoscalingCapacity> anomalyCapacity  = Optional.empty();
            if (waitingAnalyticsJobs.size() > decider.getNumAnalyticsJobsInQueue()) {
                List<Long> analyticsRequirements = waitingAnalyticsJobs
                    .stream()
                    // TODO do we want to verify memory requirements aren't stale? Or just consider `null` a fastpath?
                    .map(this::getAnalyticsMemoryRequirement)
                    .collect(Collectors.toList());
                analyticsCapacity = requiredCapacity(analyticsRequirements, decider.getNumAnalyticsJobsInQueue());
                if (analyticsCapacity.isEmpty()) {
                    return Optional.of(new AutoscalingDecision(null, reasonBuilder.setSimpleReason(MEMORY_STALE).build()));
                }
            }
            if (waitingAnomalyJobs.size() > decider.getNumAnomalyJobsInQueue()) {
                List<Long> anomalyRequirements = waitingAnomalyJobs
                    .stream()
                    // TODO do we want to verify memory requirements aren't stale? Or just consider `null` a fastpath?
                    .map(this::getAnomalyMemoryRequirement)
                    .collect(Collectors.toList());
                anomalyCapacity = requiredCapacity(anomalyRequirements, decider.getNumAnomalyJobsInQueue());
                if (anomalyCapacity.isEmpty()) {
                    return Optional.of(new AutoscalingDecision(null, reasonBuilder.setSimpleReason(MEMORY_STALE).build()));
                }
            }
            tierMemory += anomalyCapacity.orElse(AutoscalingCapacity.ZERO).tier().memory().getBytes();
            tierMemory += analyticsCapacity.orElse(AutoscalingCapacity.ZERO).tier().memory().getBytes();
            nodeMemory = Math.max(anomalyCapacity.orElse(AutoscalingCapacity.ZERO).node().memory().getBytes(), nodeMemory);
            nodeMemory = Math.max(analyticsCapacity.orElse(AutoscalingCapacity.ZERO).node().memory().getBytes(), nodeMemory);
            return Optional.of(new AutoscalingDecision(
                new AutoscalingCapacity(
                    new AutoscalingResources(null, new ByteSizeValue(tierMemory)),
                    new AutoscalingResources(null, new ByteSizeValue(nodeMemory))),
                reasonBuilder.setSimpleReason("requesting scale up as number of jobs in queues exceeded configured limit").build()));
        }

        //TODO verify waiting jobs could eventually be assigned
        // otherwise scale
        return Optional.empty();
    }

    static Optional<AutoscalingCapacity> requiredCapacity(List<Long> jobSizes, int maxNumInQueue) {
        if (jobSizes.stream().anyMatch(Objects::isNull)) {
            return Optional.empty();
        }
        jobSizes.sort(Comparator.comparingLong(Long::longValue).reversed());
        long tierMemory = 0L;
        long nodeMemory = jobSizes.get(0);
        Iterator<Long> iter = jobSizes.iterator();
        while (jobSizes.size() > maxNumInQueue && iter.hasNext()) {
            tierMemory += iter.next();
            iter.remove();
        }
        return Optional.of(new AutoscalingCapacity(new AutoscalingResources(null, new ByteSizeValue(tierMemory)),
            new AutoscalingResources(null, new ByteSizeValue(nodeMemory))));
    }

    private Long getAnalyticsMemoryRequirement(String analyticsId) {
        return nodeLoadDetector.getMlMemoryTracker().getDataFrameAnalyticsJobMemoryRequirement(analyticsId);
    }

    private Long getAnomalyMemoryRequirement(String anomalyId) {
        return nodeLoadDetector.getMlMemoryTracker().getAnomalyDetectorJobMemoryRequirement(anomalyId);
    }

    AutoscalingDecision checkForScaleDown(MlAutoscalingDeciderConfiguration decider,
                                          List<DiscoveryNode> nodes,
                                          ClusterState clusterState,
                                          MlScalingReason.Builder reasonBuilder) {
        List<NodeLoadDetector.NodeLoad> nodeLoads = new ArrayList<>();
        boolean isMemoryAccurateFlag = true;
        for (DiscoveryNode node : nodes) {
            NodeLoadDetector.NodeLoad nodeLoad = nodeLoadDetector.detectNodeLoad(clusterState,
                true,
                node,
                maxOpenJobs,
                maxMachineMemoryPercent,
                true);
            if (nodeLoad.getError() != null) {
                logger.warn("[{}] failed to gather node load limits, failure [{}]", node.getId(), nodeLoad.getError());
                continue;
            }
            nodeLoads.add(nodeLoad);
            isMemoryAccurateFlag = isMemoryAccurateFlag && nodeLoad.isUseMemory();
        }
        // Even if we verify that memory usage is up today before checking node capacity, we could still run into stale information.
        // We should not make a decision if the memory usage is stale/inaccurate.
        if (isMemoryAccurateFlag == false) {
            logger.info("nodes' view of memory usage is stale. Request refresh before making scaling decision.");
            nodeLoadDetector.getMlMemoryTracker().asyncRefresh();
            return new AutoscalingDecision(null, reasonBuilder.setSimpleReason(MEMORY_STALE).build());

        }
        // TODO check for scale down
        return new AutoscalingDecision(null, reasonBuilder.setSimpleReason(MEMORY_STALE).build());
    }

    private static Collection<PersistentTask<?>> anomalyDetectionTasks(PersistentTasksCustomMetadata tasksCustomMetadata) {
        if (tasksCustomMetadata == null) {
            return Collections.emptyList();
        }

        return tasksCustomMetadata.findTasks(MlTasks.JOB_TASK_NAME,
            t -> getJobStateModifiedForReassignments(t).isAnyOf(JobState.OPENED, JobState.OPENING));
    }

    private static Collection<PersistentTask<?>> dataframeAnalyticsTasks(PersistentTasksCustomMetadata tasksCustomMetadata) {
        if (tasksCustomMetadata == null) {
            return Collections.emptyList();
        }

        return tasksCustomMetadata.findTasks(MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            t -> getDataFrameAnalyticsState(t).isAnyOf(DataFrameAnalyticsState.STARTED, DataFrameAnalyticsState.STARTING));
    }

    @Override
    public String name() {
        return "ml";
    }

}

