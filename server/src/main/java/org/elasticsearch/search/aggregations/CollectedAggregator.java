/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.aggregations.bucket.IteratorAndCurrent;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.SamplingContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Stores the collected output of an aggregator, which can then be serialized back to the
 * coordinating node for merging.  This replaces the old {@link Aggregation} and {@link InternalAggregation}
 * classes.  There are two key differences:
 *
 * There should be exactly one {@link CollectedAggregator} per {@link Aggregator}, which is to say
 * one per shard per aggregation.
 *
 * The {@link CollectedAggregator} is {@link Releasable}.  This allows it to manage memory for data
 * structures that live longer than the collection phase.  Generally speaking, these should be released by the
 * aggregation context via {@link org.elasticsearch.search.aggregations.support.AggregationContext#addReleasable(Aggregator)}
 */
public abstract class CollectedAggregator implements Releasable, VersionedNamedWriteable {
    /**
     * The user supplied name of this aggregation.
     */
    protected final String name;

    /**
     * User supplied metadata.  From a framework perspective, this is an opaque pass through that
     * we just need to return in our response.
     */
    protected final Map<String, Object> metadata;

    /**
     * Constructs an aggregation result with a given name.  This constructor should be called on the data node
     * side to build the collected aggregation from the leaf reader results.
     *
     * @param name The name of the aggregation.
     */
    protected CollectedAggregator(String name, Map<String, Object> metadata) {
        this.name = name;
        this.metadata = metadata;

    }

    /**
     * Read from a stream.  This constructor should be used on the coordinating node side to deserialize a collected
     * aggregation that has been sent over the wire.  We want to avoid allocating new {@link BigArrays} in this path.
     */
    protected CollectedAggregator(StreamInput in) throws IOException {
        name = in.readString();
        metadata = in.readMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeGenericValue(metadata);
    }

    public String getName() {
        return name;
    }

    /**
     * Reduces the given aggregations to a single one and returns it. In <b>most</b> cases, the assumption will be the all given
     * aggregations are of the same type (the same type as this aggregation). For best efficiency, when implementing,
     * try reusing an existing instance (typically the first in the given list) to save on redundant object
     * construction.
     *
     * @param reductionTarget - {@link CollectedAggregator} with a mutable {@link org.elasticsearch.common.util.BigArray} that will be used
     *                        to hold the results of the reduction.
     * @param buckets         - List of compatible {@link CollectedAggregator} from the other data nodes
     * @param reduceContext   - The current reduction context
     * @see #mustReduceOnSingleInternalAgg()
     */
    public abstract CollectedAggregator reduceBuckets(
        CollectedAggregator reductionTarget,
        List<MultiBucketsAggregation.Bucket> buckets,
        AggregationReduceContext reduceContext
    );

    /**
     * Reduce the given aggregations as top level aggregators.  Essentially, this means reducing with a parent bucket key of 0.
     * @param aggregators - The list of aggregators to reduce
     * @param reduceContext - source for {@link BigArrays} to store the reduced data
     * @return - a single {@link CollectedAggregator} holding the reduced buckets for all the input aggregators
     */
    public abstract CollectedAggregator reduceTopLevel(List<CollectedAggregator> aggregators, AggregationReduceContext reduceContext);

    public abstract IteratorAndCurrent<KeyComparable<?>> getKeysIteratorForOwningBucketOrd(long owningBucketOrd);
    /**
     * Signal the framework if the {@linkplain CollectedAggregator#reduceBuckets(CollectedAggregator, List, AggregationReduceContext)}
     * phase needs to be called when there is only one {@linkplain CollectedAggregator}.
     */
    // NOCOMMIT: does this still make sense in the new design?
    protected abstract boolean mustReduceOnSingleInternalAgg();

    /**
     * Called by the parent sampling context. Should only ever be called once as some aggregations scale their internal values
     * @param samplingContext the current sampling context
     * @return new aggregation with the sampling context applied, could be the same aggregation instance if nothing needs to be done
     */
    public abstract CollectedAggregator finalizeSampling(SamplingContext samplingContext);

    /**
     * Return true if this aggregation and can lead a reduction.  If this agg returns
     * false, it should return itself if asked to lead a reduction
     */
    public boolean canLeadReduction() {
        return true;
    }

    public abstract CollectedAggregator reducePipelines(
        CollectedAggregator agg,
        AggregationReduceContext context,
        PipelineAggregator.PipelineTree pipelines
    );

    public abstract InternalAggregation convertToLegacy(long bucketOrdinal);

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CollectedAggregator that = (CollectedAggregator) o;
        return Objects.equals(name, that.name) && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, metadata);
    }

    @Override
    public String toString() {
        return "CollectedAggregator{" + "name='" + name + '\'' + ", metadata=" + metadata + '}';
    }
}
