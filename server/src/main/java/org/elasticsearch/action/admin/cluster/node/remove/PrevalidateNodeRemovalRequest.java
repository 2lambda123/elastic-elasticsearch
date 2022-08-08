/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.remove;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;

public class PrevalidateNodeRemovalRequest extends ActionRequest {
    private final String[] nodeIds;

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public PrevalidateNodeRemovalRequest(String... nodesIds) {
        this.nodeIds = nodesIds;
    }
}
