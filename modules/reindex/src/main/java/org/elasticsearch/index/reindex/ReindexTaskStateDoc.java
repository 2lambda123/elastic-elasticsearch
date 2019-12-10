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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class ReindexTaskStateDoc implements ToXContentObject {

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<ReindexTaskStateDoc, Void> PARSER =
        new ConstructingObjectParser<>("reindex/index_state", a -> new ReindexTaskStateDoc((ReindexRequest) a[0], (List<List<String>>) a[1],
            (Long) a[2], (BulkByScrollResponse) a[3], (ElasticsearchException) a[4], (Integer) a[5],
            (ScrollableHitSource.Checkpoint) a[6]));

    private static final String REINDEX_REQUEST = "request";
    private static final String INDEX_GROUPS = "index_groups";
    private static final String ALLOCATION = "allocation";
    private static final String REINDEX_RESPONSE = "response";
    private static final String REINDEX_EXCEPTION = "exception";
    private static final String FAILURE_REST_STATUS = "failure_rest_status";
    private static final String REINDEX_CHECKPOINT = "checkpoint";

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> ReindexRequest.fromXContentWithParams(p),
            new ParseField(REINDEX_REQUEST));
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> p.list(), new ParseField(INDEX_GROUPS));
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), new ParseField(ALLOCATION));
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> BulkByScrollResponse.fromXContent(p),
            new ParseField(REINDEX_RESPONSE));
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> ElasticsearchException.fromXContent(p),
            new ParseField(REINDEX_EXCEPTION));
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), new ParseField(FAILURE_REST_STATUS));
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> ScrollableHitSource.Checkpoint.fromXContent(p),
            new ParseField(REINDEX_CHECKPOINT));
    }

    private final ReindexRequest reindexRequest;
    private final List<? extends Collection<String>> indexGroups;
    private final Long allocationId;
    private final BulkByScrollResponse reindexResponse;
    private final ElasticsearchException exception;
    private final RestStatus failureStatusCode;
    private final ScrollableHitSource.Checkpoint checkpoint;

    public ReindexTaskStateDoc(ReindexRequest reindexRequest, List<? extends Collection<String>> indexGroups) {
        this(reindexRequest, indexGroups, null, null, null, (RestStatus) null, null);
    }

    public ReindexTaskStateDoc(ReindexRequest reindexRequest, List<? extends Collection<String>> indexGroups, @Nullable Long allocationId,
                               @Nullable BulkByScrollResponse reindexResponse, @Nullable ElasticsearchException exception,
                               @Nullable Integer failureStatusCode, ScrollableHitSource.Checkpoint checkpoint) {
        this(reindexRequest, indexGroups, allocationId, reindexResponse, exception,
            failureStatusCode == null ? null : RestStatus.fromCode(failureStatusCode), checkpoint);
    }

    public ReindexTaskStateDoc(ReindexRequest reindexRequest, List<? extends Collection<String>> indexGroups, @Nullable Long allocationId,
                               @Nullable BulkByScrollResponse reindexResponse, @Nullable ElasticsearchException exception,
                               @Nullable ScrollableHitSource.Checkpoint checkpoint) {
        this(reindexRequest, indexGroups, allocationId, reindexResponse,
            exception, exception != null ? exception.status() : null, checkpoint);
    }

    private ReindexTaskStateDoc(ReindexRequest reindexRequest, List<? extends Collection<String>> indexGroups, @Nullable Long allocationId,
                                @Nullable BulkByScrollResponse reindexResponse, @Nullable ElasticsearchException exception,
                                @Nullable RestStatus failureStatusCode, @Nullable ScrollableHitSource.Checkpoint checkpoint) {
        this.indexGroups = indexGroups;
        this.allocationId = allocationId;
        assert (reindexResponse == null) || (exception == null) : "Either response or exception must be null";
        this.reindexRequest = reindexRequest;
        this.reindexResponse = reindexResponse;
        this.exception = exception;
        this.failureStatusCode = failureStatusCode;
        this.checkpoint = checkpoint;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(REINDEX_REQUEST);
        reindexRequest.toXContent(builder, params, true);
        builder.field(INDEX_GROUPS, indexGroups);
        if (allocationId != null) {
            builder.field(ALLOCATION, allocationId);
        }
        if (reindexResponse != null) {
            builder.field(REINDEX_RESPONSE);
            builder.startObject();
            reindexResponse.toXContent(builder, params);
            builder.endObject();
        }
        if (exception != null) {
            builder.field(REINDEX_EXCEPTION);
            builder.startObject();
            ElasticsearchException.generateThrowableXContent(builder, params, exception);
            builder.endObject();
            builder.field(FAILURE_REST_STATUS, failureStatusCode.getStatus());
        }
        if (checkpoint != null) {
            builder.field(REINDEX_CHECKPOINT);
            checkpoint.toXContent(builder, params);
        }
        return builder.endObject();
    }

    public static ReindexTaskStateDoc fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ReindexRequest getReindexRequest() {
        return reindexRequest;
    }

    public List<? extends Collection<String>> getIndexGroups() {
        return indexGroups;
    }

    public BulkByScrollResponse getReindexResponse() {
        return reindexResponse;
    }

    public ElasticsearchException getException() {
        return exception;
    }

    public RestStatus getFailureStatusCode() {
        return failureStatusCode;
    }

    public ScrollableHitSource.Checkpoint getCheckpoint() {
        return checkpoint;
    }

    public Long getAllocationId() {
        return allocationId;
    }

    public ReindexTaskStateDoc withCheckpoint(ScrollableHitSource.Checkpoint checkpoint, BulkByScrollTask.Status status) {
        // todo: also store and resume from status.
        return new ReindexTaskStateDoc(reindexRequest, indexGroups, allocationId, reindexResponse,
            exception, failureStatusCode, checkpoint);
    }

    public ReindexTaskStateDoc withNewAllocation(long newAllocationId) {
        return new ReindexTaskStateDoc(reindexRequest, indexGroups, newAllocationId, reindexResponse,
            exception, failureStatusCode, checkpoint);
    }

    public ReindexTaskStateDoc withFinishedState(@Nullable BulkByScrollResponse reindexResponse,
                                                 @Nullable ElasticsearchException exception) {
        return new ReindexTaskStateDoc(reindexRequest, indexGroups, allocationId, reindexResponse, exception, checkpoint);
    }
}
