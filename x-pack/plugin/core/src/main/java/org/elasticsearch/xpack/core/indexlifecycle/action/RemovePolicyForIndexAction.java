/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class RemovePolicyForIndexAction extends Action<RemovePolicyForIndexAction.Response> {
    public static final RemovePolicyForIndexAction INSTANCE = new RemovePolicyForIndexAction();
    public static final String NAME = "indices:admin/ilm/remove_policy";

    protected RemovePolicyForIndexAction() {
        super(NAME);
    }

    @Override
    public RemovePolicyForIndexAction.Response newResponse() {
        return new Response();
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static final ParseField HAS_FAILURES_FIELD = new ParseField("has_failures");
        public static final ParseField FAILED_INDEXES_FIELD = new ParseField("failed_indexes");
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<Response, Void> PARSER = new ConstructingObjectParser<>(
                "change_policy_for_index_response", a -> new Response((List<String>) a[0]));
        static {
            PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), FAILED_INDEXES_FIELD);
            // Needs to be declared but not used in constructing the response object
            PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), HAS_FAILURES_FIELD);
        }

        private List<String> failedIndexes;

        public Response() {
        }

        public Response(List<String> failedIndexes) {
            if (failedIndexes == null) {
                throw new IllegalArgumentException(FAILED_INDEXES_FIELD.getPreferredName() + " cannot be null");
            }
            this.failedIndexes = failedIndexes;
        }

        public List<String> getFailedIndexes() {
            return failedIndexes;
        }

        public boolean hasFailures() {
            return failedIndexes.isEmpty() == false;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(HAS_FAILURES_FIELD.getPreferredName(), hasFailures());
            builder.field(FAILED_INDEXES_FIELD.getPreferredName(), failedIndexes);
            builder.endObject();
            return builder;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            failedIndexes = in.readList(StreamInput::readString);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringList(failedIndexes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(failedIndexes);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(failedIndexes, other.failedIndexes);
        }

    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest.Replaceable {

        private String[] indices;
        private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

        public Request() {
        }

        public Request(String... indices) {
            if (indices == null) {
                throw new IllegalArgumentException("indices cannot be null");
            }
            this.indices = indices;
        }

        @Override
        public Request indices(String... indices) {
            this.indices = indices;
            return this;
        }

        @Override
        public String[] indices() {
            return indices;
        }

        public void indicesOptions(IndicesOptions indicesOptions) {
            this.indicesOptions = indicesOptions;
        }

        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            indices = in.readStringArray();
            indicesOptions = IndicesOptions.readIndicesOptions(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(indices);
            indicesOptions.writeIndicesOptions(out);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(indices), indicesOptions);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.deepEquals(indices, other.indices) &&
                    Objects.equals(indicesOptions, other.indicesOptions);
        }

    }
}
