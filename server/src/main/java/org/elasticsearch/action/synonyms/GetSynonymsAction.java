/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.synonyms.SynonymsManagementAPIService;
import org.elasticsearch.synonyms.SynonymsSet;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetSynonymsAction extends ActionType<GetSynonymsAction.Response> {

    public static final GetSynonymsAction INSTANCE = new GetSynonymsAction();
    public static final String NAME = "cluster:admin/synonyms/get";

    public GetSynonymsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {
        private final String SynonymsSetId;
        private final int from;
        private final int size;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.SynonymsSetId = in.readString();
            this.from = in.readInt();
            this.size = in.readInt();
        }

        public Request(String SynonymsSetId, int from, int size) {
            Objects.requireNonNull(SynonymsSetId, "Synonym set ID cannot be null");
            this.SynonymsSetId = SynonymsSetId;
            this.from = from;
            this.size = size;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (from < 0) {
                validationException = addValidationError("from must be a positive integer", validationException);
            }
            if (size < 0) {
                validationException = addValidationError("size must be a positive integer", validationException);
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(SynonymsSetId);
            out.writeInt(from);
            out.writeInt(size);
        }

        public String synonymsSetId() {
            return SynonymsSetId;
        }

        public int from() {
            return from;
        }

        public int size() {
            return size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return from == request.from && size == request.size && Objects.equals(SynonymsSetId, request.SynonymsSetId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(SynonymsSetId, from, size);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final SynonymsManagementAPIService.SynonymsSetResult synonymsSetResults;

        public Response(StreamInput in) throws IOException {
            super(in);
            this.synonymsSetResults = new SynonymsManagementAPIService.SynonymsSetResult(in.readLong(), new SynonymsSet(in));
        }

        public Response(SynonymsManagementAPIService.SynonymsSetResult synonymsSetResult) {
            super();
            Objects.requireNonNull(synonymsSetResult, "Synonyms set result must not be null");
            this.synonymsSetResults = synonymsSetResult;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("count", synonymsSetResults.totalSynonymRules());
                synonymsSetResults.synonymsSet().toXContent(builder, params);
            }
            builder.endObject();

            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(synonymsSetResults.totalSynonymRules());
            synonymsSetResults.synonymsSet().writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(this.synonymsSetResults, response.synonymsSetResults);
        }

        @Override
        public int hashCode() {
            return Objects.hash(synonymsSetResults);
        }
    }
}
