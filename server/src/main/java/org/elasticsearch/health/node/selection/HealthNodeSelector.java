/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node.selection;

import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;

/**
 * Main component used for selecting the health node of the cluster
 */
public class HealthNodeSelector extends AllocatedPersistentTask {

    public static final boolean FEATURE_FLAG_ENABLED = "true".equals(System.getProperty("es.health_node_feature_flag_enabled"));

    public static boolean isEnabled() {
        return FEATURE_FLAG_ENABLED;
    }

    public static final String TASK_NAME = "health-node-selector";

    HealthNodeSelector(long id, String type, String action, String description, TaskId parentTask, Map<String, String> headers) {
        super(id, type, action, description, parentTask, headers);
    }

    @Override
    protected void onCancelled() {
        markAsCompleted();
    }
}
