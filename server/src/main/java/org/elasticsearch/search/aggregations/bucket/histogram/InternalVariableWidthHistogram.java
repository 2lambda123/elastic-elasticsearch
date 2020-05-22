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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.*;

public class InternalVariableWidthHistogram extends InternalMultiBucketAggregation<InternalVariableWidthHistogram, InternalVariableWidthHistogram.Bucket>
    implements Histogram, HistogramFactory{

    public static class Bucket extends InternalMultiBucketAggregation.InternalBucket implements Histogram.Bucket, KeyComparable<Bucket> {

        public static class BucketBounds {
            public double min;
            public double max;

            public BucketBounds(double min, double max) {
                assert min <= max;
                this.min = min;
                this.max = max;
            }

            public BucketBounds(StreamInput in) throws IOException {
                this(in.readDouble(), in.readDouble());
            }

            public void writeTo(StreamOutput out) throws IOException {
                out.writeDouble(min);
                out.writeDouble(max);
            }

            public boolean equals(Object obj){
                if (this == obj) return true;
                if (obj == null || getClass() != obj.getClass()) return false;
                BucketBounds that = (BucketBounds) obj;
                return min == that.min && max == that.max;
            }

            @Override
            public int hashCode() {
                return Objects.hash(getClass(), min, max);
            }
        }

        private final BucketBounds bounds;
        private long docCount;
        private InternalAggregations aggregations;
        protected final transient DocValueFormat format;
        private double centroid;

        public Bucket(double centroid,
                      BucketBounds bounds,
                      long docCount,
                      DocValueFormat format,
                      InternalAggregations aggregations) {
            this.format = format;
            this.centroid = centroid;
            this.bounds = bounds;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        /**
         * Read from a stream.
         */
        public Bucket(StreamInput in, DocValueFormat format) throws IOException {
            this.format = format;
            centroid = in.readDouble();
            docCount = in.readVLong();
            bounds = new BucketBounds(in);
            aggregations = new InternalAggregations(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(centroid);
            out.writeVLong(docCount);
            bounds.writeTo(out);
            aggregations.writeTo(out);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != InternalVariableWidthHistogram.Bucket.class) {
                return false;
            }
            InternalVariableWidthHistogram.Bucket that = (InternalVariableWidthHistogram.Bucket) obj;
            return centroid == that.centroid
                && bounds.equals(that.bounds)
                && docCount == that.docCount
                && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), centroid, bounds, docCount, aggregations);
        }

        @Override
        public String getKeyAsString() {
            return format.format((double) getKey()).toString();
        }

        @Override
        public Object getKey() { return centroid; }

        public double min() { return bounds.min; }

        public double max() { return bounds.max; }

        public double centroid() { return centroid; }

        /**
         * Buckets are compared using their centroids. But, in the final XContent returned by the aggregation,
         * we want the bucket's key to be its min. Otherwise, it would look like the distances between centroids
         * are buckets, which is incorrect.
         */
        public double getKeyForXContent(){ return bounds.min; }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        InternalVariableWidthHistogram.Bucket reduce(List<InternalVariableWidthHistogram.Bucket> buckets, ReduceContext context) {
            List<InternalAggregations> aggregations = new ArrayList<>(buckets.size());
            long docCount = 0;
            double min = this.bounds.min;
            double max = this.bounds.max;
            double sum = 0;
            for (InternalVariableWidthHistogram.Bucket bucket : buckets) {
                docCount += bucket.docCount;
                min = Math.min(min, bucket.bounds.min);
                max = Math.max(max, bucket.bounds.max);
                sum += bucket.docCount * (double) bucket.getKey();
                aggregations.add((InternalAggregations) bucket.getAggregations());
            }
            InternalAggregations aggs = InternalAggregations.reduce(aggregations, context);
            double centroid = sum / docCount;
            BucketBounds bounds = new BucketBounds(min, max);
            return new Bucket(centroid, bounds, docCount, format, aggs);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            String keyAsString = format.format((double) getKey()).toString();
            builder.startObject();
            if (format != DocValueFormat.RAW) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), keyAsString);
            }
            builder.field(CommonFields.KEY.getPreferredName(), Double.toString(getKeyForXContent()));
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int compareKey(InternalVariableWidthHistogram.Bucket other) {
            return Double.compare(centroid, other.centroid);
        }

        public DocValueFormat getFormatter() {
            return format;
        }
    }

    static class EmptyBucketInfo {

        final InternalAggregations subAggregations;

        EmptyBucketInfo(InternalAggregations subAggregations) {
            this.subAggregations = subAggregations;
        }

        EmptyBucketInfo(StreamInput in) throws IOException {
            this(new InternalAggregations(in));
        }

        public void writeTo(StreamOutput out) throws IOException {
            subAggregations.writeTo(out);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            EmptyBucketInfo that = (EmptyBucketInfo) obj;
            return Objects.equals(subAggregations, that.subAggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), subAggregations);
        }
    }

    private List<Bucket> buckets;
    private final DocValueFormat format;
    private final int numClusters;
    final EmptyBucketInfo emptyBucketInfo;

    InternalVariableWidthHistogram(String name, List<Bucket> buckets, EmptyBucketInfo emptyBucketInfo, int numClusters,
                                   DocValueFormat formatter, List<PipelineAggregator> pipelineAggregators,
                                   Map<String, Object> metaData){
        super(name, pipelineAggregators, metaData);
        this.buckets = buckets;
        this.emptyBucketInfo = emptyBucketInfo;
        this.format = formatter;
        this.numClusters = numClusters;
    }

    /**
     * Stream from a stream.
     */
    public InternalVariableWidthHistogram(StreamInput in) throws IOException{
        super(in);
        emptyBucketInfo = new EmptyBucketInfo(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        buckets = in.readList(stream -> new Bucket(stream, format));
        numClusters = in.readVInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        emptyBucketInfo.writeTo(out);
        out.writeNamedWriteable(format);
        out.writeList(buckets);
        out.writeVInt(numClusters);
    }

    @Override
    public InternalVariableWidthHistogram create(List<Bucket> buckets) {
        return new InternalVariableWidthHistogram(name, buckets, emptyBucketInfo, numClusters,
            format, pipelineAggregators(), metaData);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.centroid, prototype.bounds, prototype.docCount, prototype.format, aggregations);
    }

    @Override
    public Bucket createBucket(Number key, long docCount, InternalAggregations aggregations) {
        return new Bucket(key.doubleValue(), new Bucket.BucketBounds(key.doubleValue(), key.doubleValue()),
            docCount, format, aggregations);
    }

    @Override
    public List<Bucket> getBuckets() {
        return Collections.unmodifiableList(buckets);
    }

    @Override
    public Number getKey(MultiBucketsAggregation.Bucket bucket) {
        return ((Bucket) bucket).centroid;
    }

    @Override
    public Number nextKey(Number key) {
        return nextKey(key.doubleValue());
    }

    /**
     * This method should not be called for this specific subclass of InternalHistogram, since there should not be
     * empty buckets when clustering.
=    */
    private double nextKey(double key){ return key + 1; }

    private static class IteratorAndCurrent {

        private final Iterator<Bucket> iterator;
        private Bucket current;

        IteratorAndCurrent(Iterator<Bucket> iterator) {
            this.iterator = iterator;
            current = iterator.next();
        }

    }

    private List<Bucket> reduceBuckets(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        PriorityQueue<IteratorAndCurrent> pq = new PriorityQueue<IteratorAndCurrent>(aggregations.size()) {
            @Override
            protected boolean lessThan(IteratorAndCurrent a, IteratorAndCurrent b) {
                return Double.compare(a.current.centroid, b.current.centroid) < 0;
            }
        };
        for (InternalAggregation aggregation : aggregations) {
            InternalVariableWidthHistogram histogram = (InternalVariableWidthHistogram) aggregation;
            if (histogram.buckets.isEmpty() == false) {
                pq.add(new IteratorAndCurrent(histogram.buckets.iterator()));
            }
        }

        List<Bucket> reducedBuckets = new ArrayList<>();
        while (pq.size() > 0) {
            IteratorAndCurrent top = pq.top();
            reducedBuckets.add(top.current);

            if (top.iterator.hasNext()) {
                Bucket next = top.iterator.next();
                assert next.compareKey(top.current) >= 0 : "shards must return data sorted by key";
                top.current = next;
                pq.updateTop();
            } else {
                pq.pop();
            }
        }

        mergeBucketsIfNeeded(reducedBuckets, numClusters, reduceContext);

        return reducedBuckets;
    }


    class BucketRange{
        int startIdx;
        int endIdx;

        /**
         * These are optional utility fields
         * They're useful for determining whether buckets should be merged
         */
        double min;
        double max;
        double centroid;
        long docCount;

        public void mergeWith(BucketRange other){
            startIdx = Math.min(startIdx, other.startIdx);
            endIdx = Math.max(endIdx, other.endIdx);

            if(docCount + other.docCount > 0) {
                // Avoids div by 0 error. This condition could be false if the optional docCount field was not set
                centroid = ((centroid * docCount) + (other.centroid * other.docCount)) / (docCount + other.docCount);
                docCount += other.docCount;
            }
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
        }
    }

    /**
     * For each range {startIdx, endIdx} in <code>ranges</code>, all the buckets in that index range
     * from <code>buckets</code> are merged, and this merged bucket replaces the entire range.
     */
    private void mergeBucketsWithPlan(List<Bucket> buckets, List<BucketRange> plan, ReduceContext reduceContext){
        for(int i = plan.size() - 1; i >= 0; i--) {
            BucketRange range = plan.get(i);
            int endIdx = range.endIdx;
            int startIdx = range.startIdx;

            if(startIdx == endIdx) continue;

            List<Bucket> toMerge = new ArrayList<>();
            for(int idx = endIdx; idx > startIdx; idx--){
                toMerge.add(buckets.get(idx));
                buckets.remove(idx);
            }
            toMerge.add(buckets.get(startIdx)); // Don't remove the startIdx bucket because it will be replaced by the merged bucket

            reduceContext.consumeBucketsAndMaybeBreak(-(toMerge.size()-1)); // NOCOMMIT: Is this correct?
            Bucket merged_bucket = toMerge.get(0).reduce(toMerge, reduceContext);

            buckets.set(startIdx, merged_bucket);
        }
    }

    /**
     * Makes a merge plan by simulating the merging of the two closest buckets, until the target number of buckets is reached.
     * Distance is determined by centroid comparison.
     * Then, this plan is actually executed and the underlying buckets are merged.
     *
     * Requires: <code>buckets</code> is sorted by centroid.
     */
    private void mergeBucketsIfNeeded(List<Bucket> buckets, int targetNumBuckets, ReduceContext reduceContext) {
        // Make a plan for getting the target number of buckets
        // Each range represents a set of adjacent bucket indices of buckets that will be merged together
        List<BucketRange> ranges = new ArrayList<>();

        // Initialize each range to represent an individual bucket
        for (int i = 0; i < buckets.size(); i++) {
            // Since buckets is sorted by centroid, ranges will be as well
            BucketRange range = new BucketRange();
            range.centroid = buckets.get(i).centroid;
            range.docCount = buckets.get(i).getDocCount();
            range.startIdx = i;
            range.endIdx = i;
            ranges.add(range);
        }

        // Continually merge the two closest ranges until the target is reached
        while (ranges.size() > targetNumBuckets) {

            // Find two closest ranges (i.e. the two closest buckets after the previous merges are completed)
            // We only have to make one pass through the list because it is sorted by centroid
            int closestIdx = 0; // After this loop, (closestIdx, closestIdx + 1) will be the 2 closest buckets
            double smallest_distance = Double.POSITIVE_INFINITY;
            for (int i = 0; i < ranges.size() - 1; i++) {
                double new_distance = ranges.get(i + 1).centroid - ranges.get(i).centroid; // Positive because buckets is sorted
                if (new_distance < smallest_distance) {
                    closestIdx = i;
                    smallest_distance = new_distance;
                }
            }

            // Merge the two closest ranges
            ranges.get(closestIdx).mergeWith(ranges.get(closestIdx + 1));
            ranges.remove(closestIdx + 1);
        }

        // Execute the plan (merge the underlying buckets)
        mergeBucketsWithPlan(buckets, ranges, reduceContext);
    }

    private void mergeBucketsWithSameMin(List<Bucket> buckets, ReduceContext reduceContext){
        // Create a merge plan
        List<BucketRange> ranges = new ArrayList<>();

        // Initialize each range to represent an individual bucket
        for (int i = 0; i < buckets.size(); i++) {
            BucketRange range = new BucketRange();
            range.min = buckets.get(i).min();
            range.startIdx = i;
            range.endIdx = i;
            ranges.add(range);
        }

        // Merge ranges with same min value
        int i = 0;
        while(i < ranges.size() - 1){
            BucketRange range = ranges.get(i);
            BucketRange nextRange = ranges.get(i+1);

            if(range.min == nextRange.min){
                range.mergeWith(nextRange);
                ranges.remove(i+1);
            } else{
                i++;
            }
        }

        // Execute the plan (merge the underlying buckets)
        mergeBucketsWithPlan(buckets, ranges, reduceContext);
    }

    /**
     * When two adjacent buckets A, B overlap (A.max &gt; B.min) then their boundary is set to
     * the midpoint: (A.max + B.min) / 2.
     *
     * After this adjustment, A will contain more values than indicated and B will have less.
     */
    private void adjustBoundsForOverlappingBuckets(List<Bucket> buckets, ReduceContext reduceContext){
        for(int i = 1; i < buckets.size(); i++){
            Bucket curBucket = buckets.get(i);
            Bucket prevBucket = buckets.get(i-1);
            if(curBucket.bounds.min < prevBucket.bounds.max){
                // Overlap -> there are more values than indicated in prevBucket, less values than indicated in curBucket
                // Choosing midpoint between prevBucket.max and curBucket.min should have more accuracy
                curBucket.bounds.min = (prevBucket.bounds.max + curBucket.bounds.min) / 2;
            }
        }
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        List<Bucket> reducedBuckets = reduceBuckets(aggregations, reduceContext);

        if(reduceContext.isFinalReduce()) {
            buckets.sort(Comparator.comparing(Bucket::min));
            mergeBucketsWithSameMin(reducedBuckets, reduceContext);
            adjustBoundsForOverlappingBuckets(reducedBuckets, reduceContext);
        }
        
        return new InternalVariableWidthHistogram(getName(), reducedBuckets, emptyBucketInfo, numClusters,
            format, pipelineAggregators(), metaData);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (Bucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(buckets, format);
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalVariableWidthHistogram that = (InternalVariableWidthHistogram) obj;
        return Objects.equals(buckets, that.buckets)
                && Objects.equals(format, that.format)
                && Objects.equals(numClusters, that.numClusters);
    }

    @Override
    public String getWriteableName() {
        return VariableWidthHistogramAggregationBuilder.NAME;
    }

    @Override
    public InternalAggregation createAggregation(List<MultiBucketsAggregation.Bucket> buckets) {
        List<Bucket> buckets2 = new ArrayList<>(buckets.size());
        for (Object b : buckets) {
            buckets2.add((Bucket) b);
        }
        buckets2 = Collections.unmodifiableList(buckets2);
        return new InternalVariableWidthHistogram(name, buckets2, emptyBucketInfo, numClusters,
            format, pipelineAggregators(), metaData);
    }
}
