/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class IndexLifecycleFeatureSetUsage extends XPackFeatureSet.Usage {

    private List<PolicyStats> policyStats;

    public IndexLifecycleFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        if (input.readBoolean()) {
            policyStats = input.readList(PolicyStats::new);
        }
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_0_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        boolean hasPolicyStats = policyStats != null;
        out.writeBoolean(hasPolicyStats);
        if (hasPolicyStats) {
            out.writeList(policyStats);
        }
    }

    public IndexLifecycleFeatureSetUsage() {
        this((List<PolicyStats>)null);
    }

    public IndexLifecycleFeatureSetUsage(List<PolicyStats> policyStats) {
        super(XPackField.INDEX_LIFECYCLE, true, true);
        this.policyStats = policyStats;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        if (policyStats != null) {
            builder.field("policy_count", policyStats.size());
            builder.field("policy_stats", policyStats);
        }
    }

    public List<PolicyStats> getPolicyStats() {
        return policyStats;
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled, policyStats);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        IndexLifecycleFeatureSetUsage other = (IndexLifecycleFeatureSetUsage) obj;
        return Objects.equals(available, other.available) &&
                Objects.equals(enabled, other.enabled) &&
                Objects.equals(policyStats, other.policyStats);
    }

    public static final class PolicyStats implements ToXContentObject, Writeable {

        public static final ParseField INDICES_MANAGED_FIELD = new ParseField("indices_managed");

        private final Map<String, PhaseStats> phaseStats;
        private final int indicesManaged;

        public PolicyStats(Map<String, PhaseStats> phaseStats, int numberIndicesManaged) {
            this.phaseStats = phaseStats;
            this.indicesManaged = numberIndicesManaged;
        }

        public PolicyStats(StreamInput in) throws IOException {
            this.phaseStats = in.readMap(StreamInput::readString, PhaseStats::new);
            this.indicesManaged = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(phaseStats, StreamOutput::writeString, (o, p) -> p.writeTo(o));
            out.writeVInt(indicesManaged);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(LifecyclePolicy.PHASES_FIELD.getPreferredName(), phaseStats);
            builder.field(INDICES_MANAGED_FIELD.getPreferredName(), indicesManaged);
            builder.endObject();
            return builder;
        }

        public Map<String, PhaseStats> getPhaseStats() {
            return phaseStats;
        }

        public int getIndicesManaged() {
            return indicesManaged;
        }

        @Override
        public int hashCode() {
            return Objects.hash(phaseStats, indicesManaged);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            PolicyStats other = (PolicyStats) obj;
            return Objects.equals(phaseStats, other.phaseStats) &&
                    Objects.equals(indicesManaged, other.indicesManaged);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public static final class PhaseStats implements ToXContentObject, Writeable {
        private final String[] actionNames;
        private final TimeValue minimumAge;

        public PhaseStats(TimeValue after, String[] actionNames) {
            this.actionNames = actionNames;
            this.minimumAge = after;
        }

        public PhaseStats(StreamInput in) throws IOException {
            actionNames = in.readStringArray();
            minimumAge = in.readTimeValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringArray(actionNames);
            out.writeTimeValue(minimumAge);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Phase.MIN_AGE.getPreferredName(), minimumAge.getMillis());
            builder.field(Phase.ACTIONS_FIELD.getPreferredName(), actionNames);
            builder.endObject();
            return builder;
        }

        public String[] getActionNames() {
            return actionNames;
        }

        public TimeValue getAfter() {
            return minimumAge;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(actionNames), minimumAge);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            PhaseStats other = (PhaseStats) obj;
            return Objects.equals(minimumAge, other.minimumAge) &&
                    Objects.deepEquals(actionNames, other.actionNames);
        }
    }
}
