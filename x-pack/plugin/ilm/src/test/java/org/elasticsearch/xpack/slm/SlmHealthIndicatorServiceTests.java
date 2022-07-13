/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.UserAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.slm.SnapshotInvocationRecord;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.ServerHealthComponents.SNAPSHOT;
import static org.elasticsearch.xpack.core.ilm.OperationMode.RUNNING;
import static org.elasticsearch.xpack.core.ilm.OperationMode.STOPPED;
import static org.elasticsearch.xpack.core.ilm.OperationMode.STOPPING;
import static org.elasticsearch.xpack.slm.SlmHealthIndicatorService.NAME;
import static org.elasticsearch.xpack.slm.SlmHealthIndicatorService.SLM_NOT_RUNNING;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SlmHealthIndicatorServiceTests extends ESTestCase {

    public void testIsGreenWhenRunningAndPoliciesConfigured() {
        var clusterState = createClusterStateWith(new SnapshotLifecycleMetadata(createSlmPolicy(), RUNNING, null));
        var service = createSlmHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(true),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    SNAPSHOT,
                    GREEN,
                    "Snapshot Lifecycle Management is running",
                    null,
                    new SimpleHealthIndicatorDetails(Map.of("slm_status", RUNNING, "policies", 1, "unhealthy_policies", 0)),
                    Collections.emptyList(),
                    Collections.emptyList()
                )
            )
        );
    }

    public void testIsYellowWhenNotRunningAndPoliciesConfigured() {
        var status = randomFrom(STOPPED, STOPPING);
        var clusterState = createClusterStateWith(new SnapshotLifecycleMetadata(createSlmPolicy(), status, null));
        var service = createSlmHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(true),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    SNAPSHOT,
                    YELLOW,
                    "Snapshot Lifecycle Management is not running",
                    SlmHealthIndicatorService.HELP_URL,
                    new SimpleHealthIndicatorDetails(Map.of("slm_status", status, "policies", 1, "unhealthy_policies", 0)),
                    Collections.singletonList(
                        new HealthIndicatorImpact(
                            3,
                            "Scheduled snapshots are not running. New backup snapshots will not be created automatically.",
                            List.of(ImpactArea.BACKUP)
                        )
                    ),
                    List.of(SLM_NOT_RUNNING)
                )
            )
        );
    }

    public void testIsGreenWhenNotRunningAndNoPolicies() {
        var status = randomFrom(STOPPED, STOPPING);
        var clusterState = createClusterStateWith(new SnapshotLifecycleMetadata(Map.of(), status, null));
        var service = createSlmHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(true),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    SNAPSHOT,
                    GREEN,
                    "No Snapshot Lifecycle Management policies configured",
                    null,
                    new SimpleHealthIndicatorDetails(Map.of("slm_status", status, "policies", 0, "unhealthy_policies", 0)),
                    Collections.emptyList(),
                    Collections.emptyList()
                )
            )
        );
    }

    public void testIsGreenWhenNoMetadata() {
        var clusterState = createClusterStateWith(null);
        var service = createSlmHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(true),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    SNAPSHOT,
                    GREEN,
                    "No Snapshot Lifecycle Management policies configured",
                    null,
                    new SimpleHealthIndicatorDetails(Map.of("slm_status", RUNNING, "policies", 0, "unhealthy_policies", 0)),
                    Collections.emptyList(),
                    Collections.emptyList()
                )
            )
        );
    }

    public void testIsGreenWhenPoliciesHaveFailedForLessThanWarningThreshold() {
        long execTime = System.currentTimeMillis();
        long window = TimeUnit.HOURS.toMillis(24) - 5000L; // Just under 24 hours.
        var clusterState = createClusterStateWith(
            new SnapshotLifecycleMetadata(
                createSlmPolicyWithInvocations(
                    snapshotInvocation(execTime, execTime + 1000L),
                    snapshotInvocation(execTime + window, execTime + window + 1000L),
                    randomLongBetween(0, 4)
                ),
                RUNNING,
                null
            )
        );
        var service = createSlmHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(true),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    SNAPSHOT,
                    GREEN,
                    "Snapshot Lifecycle Management is running",
                    null,
                    new SimpleHealthIndicatorDetails(Map.of("slm_status", RUNNING, "policies", 1, "unhealthy_policies", 0)),
                    Collections.emptyList(),
                    Collections.emptyList()
                )
            )
        );
    }

    public void testIsYellowWhenPoliciesHaveFailedForMoreThanWarningThreshold() {
        long execTime = System.currentTimeMillis();
        long window = TimeUnit.HOURS.toMillis(24) + 5000L; // 24 hours and some extra room.
        var clusterState = createClusterStateWith(
            new SnapshotLifecycleMetadata(
                createSlmPolicyWithInvocations(
                    snapshotInvocation(execTime, execTime + 1000L),
                    snapshotInvocation(execTime + window, execTime + window + 1000L),
                    randomLongBetween(5L, Long.MAX_VALUE)
                ),
                RUNNING,
                null
            )
        );
        var service = createSlmHealthIndicatorService(clusterState);

        HealthIndicatorResult calculate = service.calculate(true);
        assertThat(
            calculate,
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    SNAPSHOT,
                    YELLOW,
                    "Encountered [1] unhealthy snapshot lifecycle management policies.",
                    SlmHealthIndicatorService.HELP_URL,
                    new SimpleHealthIndicatorDetails(Map.of("slm_status", RUNNING, "policies", 1, "unhealthy_policies", 1)),
                    Collections.singletonList(
                        new HealthIndicatorImpact(
                            2,
                            "Some automated snapshots have not had a successful execution recently. Indices restored from affected " +
                                "snapshots may not contain recent changes.",
                            List.of(ImpactArea.BACKUP)
                        )
                    ),
                    List.of(new UserAction(
                        SlmHealthIndicatorService.ACTION_CHECK_RECENTLY_FAILED_SNAPSHOTS,
                        List.of("test-policy")
                    ))
                )
            )
        );
    }

    private static ClusterState createClusterStateWith(SnapshotLifecycleMetadata metadata) {
        var builder = new ClusterState.Builder(new ClusterName("test-cluster"));
        if (metadata != null) {
            builder.metadata(new Metadata.Builder().putCustom(SnapshotLifecycleMetadata.TYPE, metadata));
        }
        return builder.build();
    }

    private static Map<String, SnapshotLifecyclePolicyMetadata> createSlmPolicy() {
        return createSlmPolicyWithInvocations(null, null, 0L);
    }

    private static Map<String, SnapshotLifecyclePolicyMetadata> createSlmPolicyWithInvocations(
        SnapshotInvocationRecord lastSuccess,
        SnapshotInvocationRecord lastFailure,
        long invocationsSinceLastSuccess
    ) {
        return Map.of(
            "test-policy",
            SnapshotLifecyclePolicyMetadata.builder()
                .setPolicy(new SnapshotLifecyclePolicy("id", "test-policy", "", "test-repository", null, null))
                .setVersion(1L)
                .setModifiedDate(System.currentTimeMillis())
                .setLastSuccess(lastSuccess)
                .setLastFailure(lastFailure)
                .setInvocationsSinceLastSuccess(invocationsSinceLastSuccess)
                .build()
        );
    }

    private static SnapshotInvocationRecord snapshotInvocation(long startTime, long stopTime) {
        return new SnapshotInvocationRecord("test-policy-snapshot", startTime, stopTime, null);
    }

    private static SlmHealthIndicatorService createSlmHealthIndicatorService(ClusterState clusterState) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(
                SlmHealthIndicatorService.GLOBAL_SLM_SNAPSHOT_FAILURE_WARNING_COUNT
            )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return new SlmHealthIndicatorService(clusterService);
    }
}
