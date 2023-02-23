/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.entsearch.engine.Engine;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetEngineAction extends ActionType<GetEngineAction.Response> {

    public static final GetEngineAction INSTANCE = new GetEngineAction();
    public static final String NAME = "cluster:admin/xpack/engines/get";

    private GetEngineAction() {
        super(NAME, GetEngineAction.Response::new);
    }

    public static class Request extends ActionRequest {

        private final String engineId;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.engineId = in.readString();
        }

        public Request(String engineId) {
            this.engineId = engineId;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (engineId == null || engineId.isEmpty()) {
                validationException = addValidationError("engineId missing", validationException);
            }

            return validationException;
        }

        public String getEngineId() {
            return engineId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(engineId, request.engineId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(engineId);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final Engine engine;

        public Response(StreamInput in) throws IOException {
            super(in);
            this.engine = new Engine(in);
        }

        public Response(Engine engine) {
            this.engine = engine;
        }

        public Response(String engineId, String[] indices, String analyticsCollectionName, TimeValue updatedAt) {
            this.engine = new Engine(engineId, indices, analyticsCollectionName, updatedAt);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            engine.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return engine.toXContent(builder, params);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(engine, response.engine);
        }

        @Override
        public int hashCode() {
            return Objects.hash(engine);
        }
    }
}
