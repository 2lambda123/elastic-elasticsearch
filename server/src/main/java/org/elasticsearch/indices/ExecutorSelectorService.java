/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;

/**
 * Some operations need to use different executors for different index patterns.
 * Specifically, some operations on system indices are considered critical and
 * should use the "system_critical_read" or "system_critical_write" thread pools
 * rather than the "system_read" or "system_write" thread pools.
 */
public class ExecutorSelectorService {

    private final SystemIndices systemIndices;

    public ExecutorSelectorService(SystemIndices systemIndices) {
        this.systemIndices = systemIndices;
    }

    // TODO[wrb]: javadoc
    public String getGetExecutor(String indexName) {
        SystemIndexDescriptor indexDescriptor = systemIndices.findMatchingDescriptor(indexName);
        if (Objects.nonNull(indexDescriptor)) {
            return indexDescriptor.getThreadPools().getGetPoolName();
        }

        SystemDataStreamDescriptor dataStreamDescriptor = systemIndices.findMatchingDataStreamDescriptor(indexName);
        if (Objects.nonNull(dataStreamDescriptor)) {
            return dataStreamDescriptor.getThreadPools().getGetPoolName();
        }

        return ThreadPool.Names.GET;
    }

    // TODO[wrb]: javadoc
    public String getSearchExecutor(String indexName) {
        SystemIndexDescriptor indexDescriptor = systemIndices.findMatchingDescriptor(indexName);
        if (Objects.nonNull(indexDescriptor)) {
            return indexDescriptor.getThreadPools().getSearchPoolName();
        }

        SystemDataStreamDescriptor dataStreamDescriptor = systemIndices.findMatchingDataStreamDescriptor(indexName);
        if (Objects.nonNull(dataStreamDescriptor)) {
            return dataStreamDescriptor.getThreadPools().getSearchPoolName();
        }

        return ThreadPool.Names.SEARCH;
    }

    // TODO[wrb]: javadoc
    public String getWriteExecutor(String indexName) {
        SystemIndexDescriptor indexDescriptor = systemIndices.findMatchingDescriptor(indexName);
        if (Objects.nonNull(indexDescriptor)) {
            return indexDescriptor.getThreadPools().getWritePoolName();
        }

        SystemDataStreamDescriptor dataStreamDescriptor = systemIndices.findMatchingDataStreamDescriptor(indexName);
        if (Objects.nonNull(dataStreamDescriptor)) {
            return dataStreamDescriptor.getThreadPools().getWritePoolName();
        }

        return ThreadPool.Names.WRITE;
    }

    // TODO[wrb]: javadoc
    public static String getWriteExecutorForShard(ExecutorSelectorService executorSelectorService, IndexShard shard) {
        return executorSelectorService.getWriteExecutor(shard.shardId().getIndexName());
    }
}
