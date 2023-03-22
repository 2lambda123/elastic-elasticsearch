/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class QuerySearchApplicationAction extends ActionType<SearchResponse> {

    public static final QuerySearchApplicationAction INSTANCE = new QuerySearchApplicationAction();
    public static final String NAME = "cluster:admin/xpack/application/search_application/search";

    public QuerySearchApplicationAction() {
        super(NAME, SearchResponse::new);
    }

    public static class Request extends ActionRequest {
        private final String name;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
        }

        public Request(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (name == null || name.isEmpty()) {
                validationException = addValidationError("name missing", validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            QuerySearchApplicationAction.Request request = (QuerySearchApplicationAction.Request) o;
            return Objects.equals(name, request.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        private final String name;

        public Response(String name) {
            this.name = name;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            name = in.readString();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }
}
