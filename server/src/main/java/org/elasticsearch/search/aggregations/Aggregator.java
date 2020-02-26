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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Iterator;

/**
 * An Aggregator.
 */
// IMPORTANT: DO NOT add methods to this class unless strictly required.
// On the other hand, if you can remove methods from it, you are highly welcome!
public abstract class Aggregator extends BucketCollector implements Releasable {

    /**
     * Parses the aggregation request and creates the appropriate aggregator factory for it.
     *
     * @see AggregationBuilder
     */
    @FunctionalInterface
    public interface Parser {
        /**
         * Returns the aggregator factory with which this parser is associated, may return {@code null} indicating the
         * aggregation should be skipped (e.g. when trying to aggregate on unmapped fields).
         *
         * @param aggregationName   The name of the aggregation
         * @param parser            The parser
         * @return                  The resolved aggregator factory or {@code null} in case the aggregation should be skipped
         * @throws java.io.IOException      When parsing fails
         */
        AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException;
    }

    /**
     * Returns whether one of the parents is a {@link BucketsAggregator}.
     */
    public static boolean descendsFromBucketAggregator(Aggregator parent) {
        while (parent != null) {
            if (parent instanceof BucketsAggregator) {
                return true;
            }
            parent = parent.parent();
        }
        return false;
    }

    /**
     * Return the name of this aggregator.
     */
    public abstract String name();

    /**
     * Return the {@link SearchContext} attached with this {@link Aggregator}.
     */
    public abstract SearchContext context();

    /**
     * Return the parent aggregator.
     */
    public abstract Aggregator parent();

    /**
     * Return the sub aggregator with the provided name.
     */
    public abstract Aggregator subAggregator(String name);

    /**
     * Resolve the next step of the sort path as though this aggregation
     * supported sorting. This is usually the "first step" when resolving
     * a sort path because most aggs that support sorting their buckets
     * aren't valid in the middle of a sort path.
     * <p>
     * For example, the {@code terms} aggs supports sorting its buckets, but
     * that sort path itself can't contain a different {@code terms}
     * aggregation.
     */
    public final Aggregator resolveSortPathOnValidAgg(AggregationPath.PathElement next, Iterator<AggregationPath.PathElement> path) {
        Aggregator n = subAggregator(next.name);
        if (n == null) {
            throw new IllegalArgumentException("The provided aggregation [" + next + "] either does not exist, or is "
                    + "a pipeline aggregation and cannot be used to sort the buckets.");
        }
        if (false == path.hasNext()) {
            return n;
        }
        if (next.key != null) {
            throw new IllegalArgumentException("Key only allowed on last aggregation path element but got [" + next + "]");
        }
        return n.resolveSortPath(path.next(), path);
    }

    /**
     * Resolve a sort path to the target.
     * <p>
     * The default implementation throws an exception but we override it on aggregations that support sorting.
     */
    public Aggregator resolveSortPath(AggregationPath.PathElement next, Iterator<AggregationPath.PathElement> path) {
        throw new IllegalArgumentException("Buckets can only be sorted on a sub-aggregator path " +
                "that is built out of zero or more single-bucket aggregations within the path and a final " +
                "single-bucket or a metrics aggregation at the path end. [" + name() + "] is not single-bucket.");
    }

    /**
     * Validates the "key" portion of a sort on this aggregation.
     * <p>
     * The default implementation throws an exception but we override it on aggregations that support sorting.
     */
    public void validateSortPathKey(String key) {
        throw new IllegalArgumentException("Buckets can only be sorted on a sub-aggregator path " +
                "that is built out of zero or more single-bucket aggregations within the path and a final " +
                "single-bucket or a metrics aggregation at the path end.");
    }

    /**
     * Build an aggregation for data that has been collected into {@code bucket}.
     */
    public abstract InternalAggregation buildAggregation(long bucket) throws IOException;

    /**
     * Build an empty aggregation.
     */
    public abstract InternalAggregation buildEmptyAggregation();

    /** Aggregation mode for sub aggregations. */
    public enum SubAggCollectionMode implements Writeable {

        /**
         * Creates buckets and delegates to child aggregators in a single pass over
         * the matching documents
         */
        DEPTH_FIRST(new ParseField("depth_first")),

        /**
         * Creates buckets for all matching docs and then prunes to top-scoring buckets
         * before a second pass over the data when child aggregators are called
         * but only for docs from the top-scoring buckets
         */
        BREADTH_FIRST(new ParseField("breadth_first"));

        public static final ParseField KEY = new ParseField("collect_mode");

        private final ParseField parseField;

        SubAggCollectionMode(ParseField parseField) {
            this.parseField = parseField;
        }

        public ParseField parseField() {
            return parseField;
        }

        public static SubAggCollectionMode parse(String value, DeprecationHandler deprecationHandler) {
            SubAggCollectionMode[] modes = SubAggCollectionMode.values();
            for (SubAggCollectionMode mode : modes) {
                if (mode.parseField.match(value, deprecationHandler)) {
                    return mode;
                }
            }
            throw new ElasticsearchParseException("no [{}] found for value [{}]", KEY.getPreferredName(), value);
        }

        public static SubAggCollectionMode readFromStream(StreamInput in) throws IOException {
            return in.readEnum(SubAggCollectionMode.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }
    }
}
