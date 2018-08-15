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
package org.elasticsearch.protocol.xpack.rollup;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.rollup.job.RollupJobConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents the Rollup capabilities for a specific job on a single rollup index
 */
public class RollupJobCaps implements Writeable, ToXContentObject {
    private static ParseField JOB_ID = new ParseField("job_id");
    private static ParseField ROLLUP_INDEX = new ParseField("rollup_index");
    private static ParseField INDEX_PATTERN = new ParseField("index_pattern");
    private static ParseField FIELDS = new ParseField("fields");
    private static final String NAME = "rollup_job_caps";

    public static final ConstructingObjectParser<RollupJobCaps, Void> PARSER = new ConstructingObjectParser<>(NAME,
        a -> {
            @SuppressWarnings("unchecked")
            List<Tuple<String, RollupFieldCaps>> caps = (List<Tuple<String, RollupFieldCaps>>) a[3];
            if (caps.isEmpty()) {
                return new RollupJobCaps((String) a[0], (String) a[1], (String) a[2], Collections.emptyMap());
            }
            Map<String, RollupFieldCaps> mapCaps = new HashMap<>(caps.size());
            caps.forEach(c -> mapCaps.put(c.v1(), c.v2()));
            return new RollupJobCaps((String) a[0], (String) a[1], (String) a[2], mapCaps);
        });

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), JOB_ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ROLLUP_INDEX);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_PATTERN);
        PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(),
            (p, c, name) -> new Tuple<>(name, RollupFieldCaps.fromXContent(p)), FIELDS);
    }

    private String jobID;
    private String rollupIndex;
    private String indexPattern;
    private Map<String, RollupFieldCaps> fieldCapLookup;

    // TODO now that these rollup caps are being used more widely (e.g. search), perhaps we should
    // store the RollupJob and translate into FieldCaps on demand for json output.  Would make working with
    // it internally a lot easier
    public RollupJobCaps(RollupJobConfig job) {
        Map<String, List<Map<String, Object>>> tempFieldCaps = new HashMap<>();

        jobID = job.getId();
        rollupIndex = job.getRollupIndex();
        indexPattern = job.getIndexPattern();
        Map<String, Object> dateHistoAggCap = job.getGroupConfig().getDateHistogram().toAggCap();
        String dateField = job.getGroupConfig().getDateHistogram().getField();
        List<Map<String, Object>> fieldCaps = tempFieldCaps.getOrDefault(dateField, new ArrayList<>());
        fieldCaps.add(dateHistoAggCap);
        tempFieldCaps.put(dateField, fieldCaps);

        if (job.getGroupConfig().getHistogram() != null) {
            Map<String, Object> histoAggCap = job.getGroupConfig().getHistogram().toAggCap();
            Arrays.stream(job.getGroupConfig().getHistogram().getFields()).forEach(field -> {
                List<Map<String, Object>> caps = tempFieldCaps.getOrDefault(field, new ArrayList<>());
                caps.add(histoAggCap);
                tempFieldCaps.put(field, caps);
            });
        }

        if (job.getGroupConfig().getTerms() != null) {
            Map<String, Object> termsAggCap = job.getGroupConfig().getTerms().toAggCap();
            Arrays.stream(job.getGroupConfig().getTerms().getFields()).forEach(field -> {
                List<Map<String, Object>> caps = tempFieldCaps.getOrDefault(field, new ArrayList<>());
                caps.add(termsAggCap);
                tempFieldCaps.put(field, caps);
            });
        }

        if (job.getMetricsConfig().size() > 0) {
            job.getMetricsConfig().forEach(metricConfig -> {
                List<Map<String, Object>> metrics = metricConfig.toAggCap();
                metrics.forEach(m -> {
                    List<Map<String, Object>> caps = tempFieldCaps
                        .getOrDefault(metricConfig.getField(), new ArrayList<>());
                    caps.add(m);
                    tempFieldCaps.put(metricConfig.getField(), caps);
                });
            });
        }

        // Convert the temp lists into RollupFieldCaps
        this.fieldCapLookup = Collections.unmodifiableMap(tempFieldCaps.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                e -> new RollupFieldCaps(e.getValue()))));
    }

    public RollupJobCaps(String jobID, String rollupIndex, String indexPattern, Map<String, RollupFieldCaps> fieldCapLookup) {
        this.jobID = jobID;
        this.rollupIndex = rollupIndex;
        this.indexPattern = indexPattern;
        this.fieldCapLookup = Collections.unmodifiableMap(Objects.requireNonNull(fieldCapLookup));
    }

    public RollupJobCaps(StreamInput in) throws IOException {
        this.jobID = in.readString();
        this.rollupIndex = in.readString();
        this.indexPattern = in.readString();
        this.fieldCapLookup = Collections.unmodifiableMap(in.readMap(StreamInput::readString, RollupFieldCaps::new));
    }

    public Map<String, RollupFieldCaps> getFieldCaps() {
        return fieldCapLookup;
    }

    public String getRollupIndex() {
        return rollupIndex;
    }

    public String getIndexPattern() {
        return indexPattern;
    }

    public String getJobID() {
        return jobID;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobID);
        out.writeString(rollupIndex);
        out.writeString(indexPattern);
        out.writeMap(fieldCapLookup, StreamOutput::writeString, (o, value) -> value.writeTo(o));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(JOB_ID.getPreferredName(), jobID);
        builder.field(ROLLUP_INDEX.getPreferredName(), rollupIndex);
        builder.field(INDEX_PATTERN.getPreferredName(), indexPattern);
        builder.startObject(FIELDS.getPreferredName());
        for (Map.Entry<String, RollupFieldCaps> fieldCap : fieldCapLookup.entrySet()) {
            builder.array(fieldCap.getKey(), fieldCap.getValue());
        }
        builder.endObject();
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

        RollupJobCaps that = (RollupJobCaps) other;

        return Objects.equals(this.jobID, that.jobID)
            && Objects.equals(this.indexPattern, that.indexPattern)
            && Objects.equals(this.rollupIndex, that.rollupIndex)
            && Objects.deepEquals(this.fieldCapLookup, that.fieldCapLookup);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobID, rollupIndex, fieldCapLookup);
    }

    public static class RollupFieldCaps implements Writeable, ToXContentObject {
        private List<Map<String, Object>> aggs;
        private static final String NAME = "rollup_field_caps";

        public static final Function<String, ConstructingObjectParser<RollupFieldCaps, Void>> PARSER = fieldName -> {
            @SuppressWarnings("unchecked")
            ConstructingObjectParser<RollupFieldCaps, Void> parser
                = new ConstructingObjectParser<>(NAME, a -> new RollupFieldCaps((List<Map<String, Object>>) a[0]));

            parser.declareObjectArray(ConstructingObjectParser.constructorArg(),
                (p, c) -> p.map(), new ParseField(fieldName));
            return parser;
        };

        public static RollupFieldCaps fromXContent(XContentParser parser) throws IOException {
            List<Map<String, Object>> aggs = new ArrayList<>();
            if (parser.nextToken().equals(XContentParser.Token.START_ARRAY)) {
                while (parser.nextToken().equals(XContentParser.Token.START_OBJECT)) {
                    aggs.add(Collections.unmodifiableMap(parser.map()));
                }
            }
            return new RollupFieldCaps(Collections.unmodifiableList(aggs));
        }

        RollupFieldCaps(StreamInput in) throws IOException {
            int size = in.readInt();
            aggs = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                aggs.add(Collections.unmodifiableMap(in.readMap()));
            }
            aggs = Collections.unmodifiableList(aggs);
        }

        RollupFieldCaps(List<Map<String, Object>> aggs) {
            this.aggs = Collections.unmodifiableList(Objects.requireNonNull(aggs));
        }

        public List<Map<String, Object>> getAggs() {
            return aggs;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(aggs.size());
            for (Map<String, Object> agg : aggs) {
                out.writeMap(agg);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            for (Map<String, Object> agg : aggs) {
                builder.map(agg);
            }
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

            RollupFieldCaps that = (RollupFieldCaps) other;
            return Objects.deepEquals(this.aggs, that.aggs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(aggs);
        }
    }
}
