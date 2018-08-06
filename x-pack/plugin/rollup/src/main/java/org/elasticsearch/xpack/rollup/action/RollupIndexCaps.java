/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.protocol.xpack.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.GetRollupCapsAction;
import org.elasticsearch.xpack.core.rollup.action.RollupJobCaps;
import org.elasticsearch.protocol.xpack.rollup.job.RollupJobConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class RollupIndexCaps implements Writeable, ToXContentFragment {
    static ParseField ROLLUP_JOBS = new ParseField("rollup_jobs");
    private static ParseField INDEX_NAME = new ParseField(RollupField.TYPE_NAME);

    //TODO find a way to make this parsing less hacky :(
    // Note: we ignore unknown fields since there may be unrelated metadata
    private static final ObjectParser<RollupIndexCaps, Void> METADATA_PARSER
            = new ObjectParser<>(GetRollupCapsAction.NAME, true, RollupIndexCaps::new);
    static {
        /*
            Rollup index metadata layout is:

            "_doc": {
              "_meta" : {
                "_rollup": {
                  "job-1": {
                  ... job config, parsable by RollupJobConfig.PARSER ...
                  },
                  "job-2": {
                    ... job config, parsable by RollupJobConfig.PARSER ...
                  }
                },
                "rollup-version": "7.0.0"
              }
            }
         */
        METADATA_PARSER.declareField((parser, rollupIndexCaps, aVoid) -> {
            // "_doc"
            if (parser.currentName().equals(RollupField.TYPE_NAME) && parser.currentToken().equals(XContentParser.Token.START_OBJECT)) {
                parser.nextToken();// START_OBJECT
                List<RollupJobConfig> jobs = new ArrayList<>();

                // "meta"
                if (parser.currentName().equals("_meta") && parser.currentToken().equals(XContentParser.Token.FIELD_NAME)) {
                    parser.nextToken(); // FIELD_NAME
                    parser.nextToken(); // START_OBJECT

                    // "_rollup"
                    if (parser.currentName().equals(RollupField.ROLLUP_META) &&
                            parser.currentToken().equals(XContentParser.Token.FIELD_NAME)) {
                        parser.nextToken(); // FIELD_NAME

                        // "job-1"
                        while (parser.nextToken().equals(XContentParser.Token.END_OBJECT) == false) {
                            jobs.add(RollupJobConfig.fromXContent(parser, null));
                        }
                    }
                }
                rollupIndexCaps.setJobs(jobs);
            }
        }, INDEX_NAME, ObjectParser.ValueType.OBJECT);
    }

    private List<RollupJobCaps> jobCaps = Collections.emptyList();
    private String rollupIndexName;

    private RollupIndexCaps() { }

    public RollupIndexCaps(String rollupIndexName, List<RollupJobConfig> jobs) {
        this.rollupIndexName = rollupIndexName;
        this.jobCaps = Objects.requireNonNull(jobs, "List of Rollup Jobs cannot be null")
                .stream().map(RollupJobCaps::new).collect(Collectors.toList());
    }

    RollupIndexCaps(StreamInput in) throws IOException {
        this.rollupIndexName = in.readString();
        this.jobCaps = in.readList(RollupJobCaps::new);
    }

    protected List<RollupJobCaps> getJobCaps() {
        return jobCaps;
    }

    List<RollupJobCaps> getJobCapsByIndexPattern(String index) {
        return jobCaps.stream().filter(cap -> index.equals(MetaData.ALL) ||
                    cap.getIndexPattern().equals(index)).collect(Collectors.toList());
    }

    void setJobs(List<RollupJobConfig> jobs) {
        this.jobCaps = jobs.stream().map(RollupJobCaps::new).collect(Collectors.toList());
    }

    boolean hasCaps() {
        return jobCaps.isEmpty() == false;
    }

    public List<String> getRollupIndices() {
        return jobCaps.stream().map(RollupJobCaps::getRollupIndex).collect(Collectors.toList());
    }

    static RollupIndexCaps parseMetadataXContent(BytesReference source, String indexName) {
        XContentParser parser;
        try {
            parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
                    source, XContentType.JSON);
        } catch (Exception e) {
            throw new RuntimeException("Unable to parse mapping metadata for index ["
                    + indexName + "]", e);
        }
        return METADATA_PARSER.apply(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(rollupIndexName);
        out.writeList(jobCaps);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(rollupIndexName);
        builder.field(ROLLUP_JOBS.getPreferredName(), jobCaps);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        RollupIndexCaps that = (RollupIndexCaps) other;

        return Objects.equals(this.rollupIndexName, that.rollupIndexName)
                && Objects.equals(this.jobCaps, that.jobCaps);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rollupIndexName, jobCaps);
    }
}
