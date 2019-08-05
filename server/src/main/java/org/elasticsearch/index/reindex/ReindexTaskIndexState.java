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

import java.io.IOException;

public class ReindexTaskIndexState implements ToXContentObject {

    public static final ConstructingObjectParser<ReindexTaskIndexState, Void> PARSER =
        new ConstructingObjectParser<>("reindex/index_state", a -> new ReindexTaskIndexState((ReindexRequest) a[0]));

    private static final String REINDEX_REQUEST = "request";
    private static final String REINDEX_RESPONSE = "response";
    private static final String REINDEX_EXCEPTION = "exception";

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> ReindexRequest.fromXContentWithParams(p),
            new ParseField(REINDEX_REQUEST));
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> BulkByScrollResponse.fromXContent(p),
            new ParseField(REINDEX_RESPONSE));
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> ElasticsearchException.fromXContent(p),
            new ParseField(REINDEX_EXCEPTION));
    }

    private final ReindexRequest reindexRequest;
    private final BulkByScrollResponse reindexResponse;
    private final ElasticsearchException exception;

    public ReindexTaskIndexState(ReindexRequest reindexRequest) {
        this(reindexRequest, null, null);
    }

    public ReindexTaskIndexState(ReindexRequest reindexRequest, @Nullable BulkByScrollResponse reindexResponse,
                                 @Nullable ElasticsearchException exception) {
        assert (reindexResponse == null) || (exception == null) : "Either response or exception must be null";
        this.reindexRequest = reindexRequest;
        this.reindexResponse = reindexResponse;
        this.exception = exception;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(REINDEX_REQUEST);
        reindexRequest.toXContent(builder, params, true);
        if (reindexResponse != null) {
            builder.field(REINDEX_RESPONSE);
            reindexResponse.toXContent(builder, params);
        }
        if (exception != null) {
            builder.field(REINDEX_EXCEPTION);
            exception.toXContent(builder, params);
        }
        return builder.endObject();
    }

    public static ReindexTaskIndexState fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ReindexRequest getReindexRequest() {
        return reindexRequest;
    }
}
