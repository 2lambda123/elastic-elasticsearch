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
package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ml.datafeed.DatafeedConfig;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Request object to get {@link DatafeedConfig} objects with the matching {@code datafeedId}s.
 *
 * {@code _all} explicitly gets all the datafeeds in the cluster
 * An empty request (no {@code datafeedId}s) implicitly gets all the datafeeds in the cluster
 */
public class GetDatafeedRequest implements Validatable, ToXContentObject {

    public static final ParseField DATAFEED_IDS = new ParseField("datafeed_ids");
    public static final ParseField ALLOW_NO_MATCH = new ParseField("allow_no_match");
    public static final String FOR_EXPORT = "for_export";

    private static final String ALL_DATAFEEDS = "_all";
    private final List<String> datafeedIds;
    private Boolean allowNoMatch;
    private Boolean forExport;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<GetDatafeedRequest, Void> PARSER = new ConstructingObjectParser<>(
        "get_datafeed_request",
        true, a -> new GetDatafeedRequest(a[0] == null ? new ArrayList<>() : (List<String>) a[0]));

    static {
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), DATAFEED_IDS);
        PARSER.declareBoolean(GetDatafeedRequest::setAllowNoMatch, ALLOW_NO_MATCH);
    }

    /**
     * Helper method to create a query that will get ALL datafeeds
     * @return new {@link GetDatafeedRequest} object searching for the datafeedId "_all"
     */
    public static GetDatafeedRequest getAllDatafeedsRequest() {
        return new GetDatafeedRequest(ALL_DATAFEEDS);
    }

    /**
     * Get the specified {@link DatafeedConfig} configurations via their unique datafeedIds
     * @param datafeedIds must not contain any null values
     */
    public GetDatafeedRequest(String... datafeedIds) {
        this(Arrays.asList(datafeedIds));
    }

    GetDatafeedRequest(List<String> datafeedIds) {
        if (datafeedIds.stream().anyMatch(Objects::isNull)) {
            throw new NullPointerException("datafeedIds must not contain null values");
        }
        this.datafeedIds = new ArrayList<>(datafeedIds);
    }

    /**
     * All the datafeedIds for which to get configuration information
     */
    public List<String> getDatafeedIds() {
        return datafeedIds;
    }

    /**
     * Whether to ignore if a wildcard expression matches no datafeeds.
     *
     * @param allowNoMatch If this is {@code false}, then an error is returned when a wildcard (or {@code _all})
     *                        does not match any datafeeds
     */
    public void setAllowNoMatch(boolean allowNoMatch) {
        this.allowNoMatch = allowNoMatch;
    }

    public Boolean getAllowNoMatch() {
        return allowNoMatch;
    }

    /**
     * Setting this flag to `true` removes certain fields from the configuration on retrieval.
     *
     * This is useful when getting the configuration and wanting to put it in another cluster.
     *
     * Default value is false.
     * @param forExport Boolean value indicating if certain fields should be removed
     */
    public void setForExport(boolean forExport) {
        this.forExport = forExport;
    }

    public Boolean getForExport() {
        return forExport;
    }

    @Override
    public int hashCode() {
        return Objects.hash(datafeedIds, forExport, allowNoMatch);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || other.getClass() != getClass()) {
            return false;
        }

        GetDatafeedRequest that = (GetDatafeedRequest) other;
        return Objects.equals(datafeedIds, that.datafeedIds) &&
            Objects.equals(allowNoMatch, that.allowNoMatch) &&
            Objects.equals(forExport, that.forExport);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (datafeedIds.isEmpty() == false) {
            builder.field(DATAFEED_IDS.getPreferredName(), datafeedIds);
        }

        if (allowNoMatch != null) {
            builder.field(ALLOW_NO_MATCH.getPreferredName(), allowNoMatch);
        }

        builder.endObject();
        return builder;
    }
}
