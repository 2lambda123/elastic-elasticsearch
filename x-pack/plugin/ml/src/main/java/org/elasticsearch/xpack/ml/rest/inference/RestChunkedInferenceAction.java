/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.inference;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ml.action.ChunkedInferenceAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

/**
 * This REST endpoint is purely for development purposes and
 * will be removed once there is another way to call the
 * ChunkedInferenceAction
 */
public class RestChunkedInferenceAction extends BaseRestHandler {

    static final String PATH = BASE_PATH + "trained_models/{" + TrainedModelConfig.MODEL_ID.getPreferredName() + "}/_chunked_infer";

    @Override
    public String getName() {
        return "xpack_ml_chunk_infer_trained_models_deployment_action";
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(Route.builder(POST, PATH).build());
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String modelId = restRequest.param(TrainedModelConfig.MODEL_ID.getPreferredName());
        if (restRequest.hasContent() == false) {
            throw ExceptionsHelper.badRequestException("requires body");
        }

        TimeValue inferTimeout = null;
        if (restRequest.hasParam(InferModelAction.Request.TIMEOUT.getPreferredName())) {
            inferTimeout = restRequest.paramAsTime(
                InferModelAction.Request.TIMEOUT.getPreferredName(),
                ChunkedInferenceAction.Request.DEFAULT_TIMEOUT
            );
        }
        var request = ChunkedInferenceAction.parseRequest(modelId, inferTimeout, restRequest.contentParser());

        return channel -> client.execute(ChunkedInferenceAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
