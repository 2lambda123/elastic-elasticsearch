/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.slm;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

public class SnapshotRetentionConfiguration implements ToXContentObject, Writeable {

    public static final SnapshotRetentionConfiguration EMPTY = new SnapshotRetentionConfiguration((TimeValue) null);

    private static final ParseField EXPIRE_AFTER = new ParseField("expire_after");

    private static final ConstructingObjectParser<SnapshotRetentionConfiguration, Void> PARSER =
        new ConstructingObjectParser<>("snapshot_retention", true, a -> {
            TimeValue expireAfter = a[0] == null ? null : TimeValue.parseTimeValue((String) a[0], EXPIRE_AFTER.getPreferredName());
            return new SnapshotRetentionConfiguration(expireAfter);
        });

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), EXPIRE_AFTER);
    }

    // TODO: add the rest of the configuration values
    private final TimeValue expireAfter;

    public SnapshotRetentionConfiguration(@Nullable TimeValue expireAfter) {
        this.expireAfter = expireAfter;
    }

    SnapshotRetentionConfiguration(StreamInput in) throws IOException {
        this.expireAfter = in.readOptionalTimeValue();
    }

    public static SnapshotRetentionConfiguration parse(XContentParser parser, String name) {
        return PARSER.apply(parser, null);
    }

    public TimeValue getExpireAfter() {
        return this.expireAfter;
    }

    /**
     * Return a predicate by which a SnapshotInfo can be tested to see
     * whether it should be deleted according to this retention policy.
     * @param allSnapshots a list of all snapshot pertaining to this SLM policy and repository
     */
    public Predicate<SnapshotInfo> getSnapshotDeletionPredicate(final List<SnapshotInfo> allSnapshots) {
        return si -> {
            if (this.expireAfter != null) {
                TimeValue snapshotAge = new TimeValue(System.currentTimeMillis() - si.startTime());
                if (snapshotAge.compareTo(this.expireAfter) > 0) {
                    return true;
                } else {
                    return false;
                }
            }
            // If nothing matched, the snapshot is not eligible for deletion
            return false;
        };
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (expireAfter != null) {
            builder.field(EXPIRE_AFTER.getPreferredName(), expireAfter.getStringRep());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalTimeValue(this.expireAfter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expireAfter);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        SnapshotRetentionConfiguration other = (SnapshotRetentionConfiguration) obj;
        return Objects.equals(this.expireAfter, other.expireAfter);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
