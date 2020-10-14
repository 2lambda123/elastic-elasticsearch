/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.MultiValuesSource;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Map;

/**
 * Metric Aggregation for joining sorted geo_point values into a single path
 **/
final class GeoLineAggregator extends MetricsAggregator {
    /** Multiple ValuesSource with field names */
    private final MultiValuesSource.AnyMultiValuesSource valuesSources;
    static final int MAX_PATH_SIZE = 10000;

    private final GeoLineBucketedSort sort;
    private final GeoLineBucketedSort.Extra extra;
    private final boolean includeSorts;
    private final SortOrder sortOrder;

    GeoLineAggregator(String name, MultiValuesSource.AnyMultiValuesSource valuesSources, SearchContext context,
                      Aggregator parent, Map<String,Object> metaData, boolean includeSorts, SortOrder sortOrder) throws IOException {
        super(name, context, parent, metaData);
        this.valuesSources = valuesSources;
        if (valuesSources != null) {
            this.extra = new GeoLineBucketedSort.Extra(context.bigArrays(), valuesSources);
            this.sort = new GeoLineBucketedSort(context.bigArrays(), sortOrder, null, MAX_PATH_SIZE, valuesSources, extra);
        } else {
            this.extra = null;
            this.sort = null;
        }
        this.includeSorts = includeSorts;
        this.sortOrder = sortOrder;
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSources != null && valuesSources.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        if (valuesSources == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        BucketedSort.Leaf leafSort = sort.forLeaf(ctx);

        return new LeafBucketCollector(){
            @Override
            public void collect(int doc, long bucket) throws IOException {
                leafSort.collect(doc, bucket);
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSources == null) {
            return buildEmptyAggregation();
        }
        boolean complete = sort.inHeapMode(bucket) == false;
        double[] sortVals = sort.getSortValues(bucket);
        long[] bucketLine = sort.getPoints(bucket);
        new PathArraySorter(bucketLine, sortVals, sortOrder).sort();
        return new InternalGeoLine(name, bucketLine, sortVals, metadata(), complete, includeSorts, sortOrder);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalGeoLine(name, null, null, metadata(), true, includeSorts, sortOrder);
    }

    @Override
    public void doClose() {
        Releasables.close(sort, extra);
    }
}
