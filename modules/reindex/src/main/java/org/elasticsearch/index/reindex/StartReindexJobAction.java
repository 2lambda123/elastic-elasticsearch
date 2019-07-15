/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class StartReindexJobAction extends ActionType<StartReindexJobAction.Response> {

    public static final StartReindexJobAction INSTANCE = new StartReindexJobAction();
    // TODO: Name
    public static final String NAME = "indices:data/write/start_reindex";

    private StartReindexJobAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject, CompositeIndicesRequest {

        private final ReindexRequest reindexRequest;
        private final boolean waitForCompletion;


        public Request(ReindexRequest reindexRequest) {
            this(reindexRequest, false);
        }

        public Request(ReindexRequest reindexRequest, boolean waitForCompletion) {
            this.reindexRequest = reindexRequest;
            this.waitForCompletion = waitForCompletion;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            reindexRequest = new ReindexRequest(in);
            waitForCompletion = in.readBoolean();
        }

        @Override
        public void readFrom(StreamInput in) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            reindexRequest.writeTo(out);
            out.writeBoolean(waitForCompletion);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) {
            return builder;
        }

        public ReindexRequest getReindexRequest() {
            return reindexRequest;
        }

        public boolean getWaitForCompletion() {
            return waitForCompletion;
        }
    }

    public static class Response extends ActionResponse {

        static final ParseField TASK_ID = new ParseField("task_id");
        static final ParseField REINDEX_RESPONSE = new ParseField("reindex_response");

        private static final ConstructingObjectParser<Response, Void> PARSER = new ConstructingObjectParser<>(
            "start_reindex_response", true, args -> new Response((String) args[0], (BulkByScrollResponse) args[1]));

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), TASK_ID);
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
                (parser, context) -> BulkByScrollResponse.fromXContent(parser), REINDEX_RESPONSE);
        }

        private String taskId;
        private BulkByScrollResponse reindexResponse;

        public Response(String taskId) {
            this.taskId = taskId;
        }

        public Response(String taskId, BulkByScrollResponse reindexResponse) {
            this.taskId = taskId;
            this.reindexResponse = reindexResponse;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            taskId = in.readString();
            reindexResponse = in.readOptionalStreamable(BulkByScrollResponse::new);
        }

        @Override
        public void readFrom(StreamInput in) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(taskId);
            out.writeOptionalWriteable(reindexResponse);
        }

        public String getTaskId() {
            return taskId;
        }

        public BulkByScrollResponse getReindexResponse() {
            return reindexResponse;
        }

        public static Response fromXContent(final XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }
}
