/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery.plan;

import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.util.CollectionUtils.concatLists;

public class MaximizeSnapshotFileReusePlanner implements ShardRecoveryPlanner {
    @Override
    public ShardRecoveryPlan computePlan(String shardIdentifier,
                                         Store.MetadataSnapshot sourceMetadata,
                                         Store.MetadataSnapshot targetMetadata,
                                         long startingSeqNo,
                                         int translogOps,
                                         Store.RecoveryDiff sourceTargetDiff,
                                         List<StoreFileMetadata> filesMissingInTarget,
                                         List<ShardSnapshot> availableSnapshots) {
        Map<String, StoreFileMetadata> filesToRecoverFromSource = filesMissingInTarget
            .stream()
            .collect(Collectors.toMap(StoreFileMetadata::name, Function.identity()));
        Store.MetadataSnapshot filesToRecoverFromSourceSnapshot =
            new Store.MetadataSnapshot(filesToRecoverFromSource, emptyMap(), 0);

        int filesToRecoverFromSnapshot = 0;
        ShardRecoveryPlan plan = null;
        for (ShardSnapshot shardSnapshot : availableSnapshots) {
            Store.RecoveryDiff snapshotDiff = filesToRecoverFromSourceSnapshot.recoveryDiff(shardSnapshot.getMetadataSnapshot());
            if (snapshotDiff.identical.size() > filesToRecoverFromSnapshot) {
                final ShardRecoveryPlan.SnapshotFilesToRecover snapshotFilesToRecover =
                    new ShardRecoveryPlan.SnapshotFilesToRecover(shardSnapshot.getIndexId(),
                        shardSnapshot.getRepository(),
                        shardSnapshot.getSnapshotFiles(snapshotDiff.identical));

                plan = new ShardRecoveryPlan(snapshotFilesToRecover,
                    concatLists(snapshotDiff.missing, snapshotDiff.different),
                    sourceTargetDiff.identical,
                    startingSeqNo,
                    translogOps,
                    sourceMetadata
                );
                filesToRecoverFromSnapshot = plan.getSnapshotFilesToRecover().size();
            }
        }

        return plan;
    }
}
