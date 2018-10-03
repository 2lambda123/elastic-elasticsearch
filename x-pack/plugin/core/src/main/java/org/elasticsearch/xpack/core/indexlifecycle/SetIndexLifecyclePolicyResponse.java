/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class SetIndexLifecyclePolicyResponse extends ActionResponse implements ToXContentObject {

    public static final ParseField HAS_FAILURES_FIELD = new ParseField("has_failures");
    public static final ParseField FAILED_INDEXES_FIELD = new ParseField("failed_indexes");
    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SetIndexLifecyclePolicyResponse, Void> PARSER = new ConstructingObjectParser<>(
        "change_policy_for_index_response", a -> new SetIndexLifecyclePolicyResponse((List<String>) a[0]));
    static {
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), FAILED_INDEXES_FIELD);
        // Needs to be declared but not used in constructing the response object
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), HAS_FAILURES_FIELD);
    }

    private List<String> failedIndexes;

    public SetIndexLifecyclePolicyResponse() {
    }

    public SetIndexLifecyclePolicyResponse(List<String> failedIndexes) {
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

    public static SetIndexLifecyclePolicyResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
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
        SetIndexLifecyclePolicyResponse other = (SetIndexLifecyclePolicyResponse) obj;
        return Objects.equals(failedIndexes, other.failedIndexes);
    }

}

