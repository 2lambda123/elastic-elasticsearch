/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.Writeable;

public class NodesStatsAction extends ActionType<NodesStatsResponse> {

    public static final NodesStatsAction INSTANCE = new NodesStatsAction();
    public static final String NAME = "cluster:monitor/nodes/stats";

    private NodesStatsAction() {
        super(NAME, Writeable.Reader.localOnly());
    }
}
