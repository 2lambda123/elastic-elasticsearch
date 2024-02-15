/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.Index;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Exception indicating that one or more requested indices are failure indices.
 */
public final class FailureIndexException extends ElasticsearchException {

    public FailureIndexException(Index index) {
        super("failure_index");
        setIndex(index);
    }

    public FailureIndexException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
