/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.dataframe.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.dataframe.action.StopDataFrameJobAction;

import java.io.IOException;

public class RestStopDataFrameJobAction extends BaseRestHandler {

    public RestStopDataFrameJobAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST, DataFrameField.REST_BASE_PATH_JOBS_BY_ID + "_stop", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String id = restRequest.param(DataFrameField.ID.getPreferredName());
        TimeValue timeout = restRequest.paramAsTime(DataFrameField.TIMEOUT.getPreferredName(), StopDataFrameJobAction.DEFAULT_TIMEOUT);
        boolean waitForCompletion = restRequest.paramAsBoolean(DataFrameField.WAIT_FOR_COMPLETION.getPreferredName(), false);

        StopDataFrameJobAction.Request request = new StopDataFrameJobAction.Request(id, waitForCompletion, timeout);

        return channel -> client.execute(StopDataFrameJobAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "data_frame_stop_job_action";
    }
}
