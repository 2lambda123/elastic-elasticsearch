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

package org.elasticsearch.search.reducers.metric.delta;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.reducers.*;

import java.io.IOException;

public class DeltaReducer extends Reducer {

    public static final ReducerFactoryStreams.Stream STREAM = new ReducerFactoryStreams.Stream() {
        @Override
        public ReducerFactory readResult(StreamInput in) throws IOException {
            Factory factory = new Factory();
            factory.readFrom(in);
            return factory;
        }
    };

    public static void registerStreams() {
        ReducerFactoryStreams.registerStream(STREAM, InternalDelta.TYPE.stream());
    }

    public DeltaReducer(String name, String bucketsPath, ReducerFactories factories, ReducerContext context, Reducer parent) {
        super(name, bucketsPath, factories, context, parent);
    }

    @Override
    public InternalAggregation doReduce(MultiBucketsAggregation aggregation, BytesReference bucketType,
            BucketStreamContext bucketStreamContext) throws ReductionExecutionException {
        //compute diff
        MultiBucketsAggregation.Bucket bucket0 = aggregation.getBuckets().get(0);
        return null;
    }

    public static class Factory extends ReducerFactory {

        private String bucketsPath;

        public Factory() {
            super(InternalDelta.TYPE);
        }

        public Factory(String name, String bucketsPath) {
            super(name, InternalDelta.TYPE);
        }

        @Override
        public Reducer create(ReducerContext context, Reducer parent) {
            return new DeltaReducer(name, bucketsPath, factories, context, parent);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            bucketsPath = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(bucketsPath);
        }
        
    }

}
