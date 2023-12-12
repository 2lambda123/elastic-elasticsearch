/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.StoredAsyncTask;

import java.util.List;
import java.util.Map;

public class EsqlQueryTask extends StoredAsyncTask<EsqlQueryResponse> {
    public EsqlQueryTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTaskId,
        Map<String, String> headers,
        Map<String, String> originHeaders,
        AsyncExecutionId asyncExecutionId,
        TimeValue keepAlive
    ) {
        super(id, type, action, description, parentTaskId, headers, originHeaders, asyncExecutionId, keepAlive);
    }

    @Override
    public EsqlQueryResponse getCurrentResult() {  // TODO: what semantics we want here
        // for esql searches we never store a search response in the task (neither partial, nor final)
        // we kill the task on final response, so if the task is still present, it means the search is still running
        // System.out.println("HEGO: EsqlQueryTask:: getCurrentResult");

        return new EsqlQueryResponse(List.of(), List.of(), null, false, getExecutionId().getEncoded(), true);
    }
}
