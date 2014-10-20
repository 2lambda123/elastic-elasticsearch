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

package org.elasticsearch.search.reducers.bucket;

import com.google.common.collect.Maps;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.BucketStreams;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class InternalBucketReducerAggregation extends InternalAggregation implements BucketReducerAggregation {

    private List<InternalSelection> selections;
    private Map<String, Selection> selectionMap;

    public static class InternalSelection implements Selection {

        private String key;
        private BytesReference bucketType;
        private List<? extends MultiBucketsAggregation.Bucket> buckets;
        private Map<String, MultiBucketsAggregation.Bucket> bucketMap;
        private InternalAggregations aggregations;
        private BucketStreamContext bucketStreamContext;

        public InternalSelection() {
            // For serialization only
        }

        public InternalSelection(String key, BytesReference bucketType, BucketStreamContext bucketStreamContext, List<? extends MultiBucketsAggregation.Bucket> buckets, InternalAggregations aggregations) {
            this.key = key;
            this.bucketType = bucketType;
            this.bucketStreamContext = bucketStreamContext;
            this.buckets = buckets;
            this.aggregations = aggregations;
        }

        @Override
        public String getName() {
            return getKey();
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public Text getKeyAsText() {
            return new StringText(key);
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }
        
        public void setAggregations(InternalAggregations aggregations) {
            this.aggregations = aggregations;
        }

        public BytesReference getBucketType() {
            return bucketType;
        }

        public BucketStreamContext getBucketStreamContext() {
            return bucketStreamContext;
        }

        @Override
        public List<? extends MultiBucketsAggregation.Bucket> getBuckets() {
            return buckets;
        }

        @Override
        public <B extends MultiBucketsAggregation.Bucket> B getBucketByKey(String key) {
            if (bucketMap == null) {
            bucketMap = Maps.newHashMapWithExpectedSize(buckets.size());
            for (MultiBucketsAggregation.Bucket bucket : buckets) {
                bucketMap.put(bucket.getKey(), bucket);
            }
        }
            return (B) bucketMap.get(key);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("key", key);
            builder.field("bucketType", bucketType.toUtf8());
            builder.startArray("buckets");
            for (MultiBucketsAggregation.Bucket bucket : buckets) {
                bucket.toXContent(builder, params);
            }
            builder.endArray();
            if (aggregations != null) {
                builder.startObject("reductions");
                aggregations.toXContentInternal(builder, params);
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            this.key = in.readString();
            this.bucketType = in.readBytesReference();
            this.bucketStreamContext = new BucketStreamContext();
            this.bucketStreamContext.readFrom(in);
            int size = in.readVInt();
            List<MultiBucketsAggregation.Bucket> buckets = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                MultiBucketsAggregation.Bucket bucket = BucketStreams.stream(bucketType)
                        .readResult(in, bucketStreamContext);
                buckets.add(bucket);
            }
            this.buckets = buckets;
            this.bucketMap = null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(key);
            out.writeBytesReference(bucketType);
            bucketStreamContext.writeTo(out);
            out.writeVInt(buckets.size());
            for (MultiBucketsAggregation.Bucket bucket : buckets) {
                bucket.writeTo(out);
            }
        }
    }

    protected InternalBucketReducerAggregation() {
        // For serialization only
    }

    protected InternalBucketReducerAggregation(String name, List<InternalSelection> selections) {
        super(name);
        this.name = name;
        this.selections = selections;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<? extends Selection> getSelections() {
        return selections;
    }

    @Override
    public <B extends Selection> B getSelectionByKey(String key) {
        if (selectionMap == null) {
            selectionMap = Maps.newHashMapWithExpectedSize(selections.size());
            for (Selection bucket : selections) {
                selectionMap.put(bucket.getKey(), bucket);
            }
        }
        return (B) selectionMap.get(key);
    }

    @Override
    public InternalAggregation reduce(ReduceContext reduceContext) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("selections");
        for (Selection selection : selections) {
            selection.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.name = in.readString();
        int size = in.readVInt();
        List<InternalSelection> selections = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            InternalSelection selection = new InternalSelection();
            selection.readFrom(in);
            selections.add(selection);
        }
        this.selections = selections;
        this.selectionMap = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVInt(selections.size());
        for (Selection selection : selections) {
            selection.writeTo(out);
        }
    }
}
