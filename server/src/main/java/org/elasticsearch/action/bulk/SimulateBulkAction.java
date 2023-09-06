/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.transport.TransportRequestOptions;

public class SimulateBulkAction extends ActionType<BulkResponse> {

    public static final SimulateBulkAction INSTANCE = new SimulateBulkAction();
    public static final String NAME = "indices:data/simulate/bulk";

    private static final TransportRequestOptions TRANSPORT_REQUEST_OPTIONS = TransportRequestOptions.of(
        null,
        TransportRequestOptions.Type.BULK
    );

    private SimulateBulkAction() {
        super(NAME, BulkResponse::new);
    }

    @Override
    public TransportRequestOptions transportOptions() {
        return TRANSPORT_REQUEST_OPTIONS;
    }
}
