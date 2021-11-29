/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.core.ccr.action.GetAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class AutoFollowRestoreGlobalStateIT extends CcrIntegTestCase {

    public void testShouldHandleRestoringGlobalState() {

        var timeout = TimeValue.MAX_VALUE;

        var repoName = "test-repository";
        var repoPath = UUIDs.randomBase64UUID();
        var snapshotName = "test-snapshot-with-global-state";

        //unregister remote
        var remote = followerClient().settings().get("cluster.remote.leader_cluster.seeds");

        followerClient().admin().cluster().updateSettings(
            new ClusterUpdateSettingsRequest().persistentSettings(Settings.builder().putNull("cluster.remote.leader_cluster.seeds")).masterNodeTimeout(timeout)
        ).actionGet();

        assertThat(
            followerClient().admin().cluster().state(
                new ClusterStateRequest().masterNodeTimeout(timeout)
            ).actionGet()
            .getState().metadata().persistentSettings().get("cluster.remote.leader_cluster.seeds"),
            nullValue()
        );

        //create repository
        followerClient().admin().cluster().putRepository(
            new PutRepositoryRequest(repoName).type("fs").settings(Settings.builder().put("location", repoPath)).masterNodeTimeout(timeout)
        ).actionGet();

        //make snapshot with a global state
        followerClient().admin().cluster().createSnapshot(
            new CreateSnapshotRequest(repoName, snapshotName).includeGlobalState(true).waitForCompletion(true).masterNodeTimeout(timeout)
        ).actionGet();

        //register remote
        followerClient().admin().cluster().updateSettings(
            new ClusterUpdateSettingsRequest().persistentSettings(Settings.builder().put("cluster.remote.leader_cluster.seeds", remote)).masterNodeTimeout(timeout)
        ).actionGet();

        //create auto-follow pattern
        var putAutoFollowerRequest = new PutAutoFollowPatternAction.Request();
        putAutoFollowerRequest.setName("pattern-1");
        putAutoFollowerRequest.setRemoteCluster("leader_cluster");
        putAutoFollowerRequest.setLeaderIndexPatterns(List.of("logs-*"));
        putAutoFollowerRequest.setLeaderIndexExclusionPatterns(List.of());
        putAutoFollowerRequest.setFollowIndexNamePattern("copy-{{leader_index}}");
        assertTrue(followerClient().execute(PutAutoFollowPatternAction.INSTANCE, putAutoFollowerRequest).actionGet().isAcknowledged());

        //restore from a snapshot with global state
        followerClient().admin().cluster().restoreSnapshot(
            new RestoreSnapshotRequest(repoName, snapshotName).includeGlobalState(true).waitForCompletion(true).masterNodeTimeout(timeout)
        ).actionGet();

        //assert remote is present
        assertThat(
            followerClient().admin().cluster().state(
                new ClusterStateRequest().masterNodeTimeout(timeout)
            ).actionGet()
            .getState().metadata().persistentSettings().get("cluster.remote.leader_cluster.seeds"),
            equalTo(remote)
        );

        //assert auto-follow is present
        var getAutoFollowerRequest = new GetAutoFollowPatternAction.Request();
        var getAutoFollowerResponse = followerClient().execute(GetAutoFollowPatternAction.INSTANCE, getAutoFollowerRequest).actionGet();
        assertThat(getAutoFollowerResponse.getAutoFollowPatterns().keySet(), equalTo(Set.of("pattern-1")));
        assertThat(getAutoFollowerResponse.getAutoFollowPatterns().get("pattern-1").isActive(), equalTo(true));
    }
}
