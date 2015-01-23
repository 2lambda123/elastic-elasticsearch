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

package org.elasticsearch.search.aggregations.transformer.moving.avg;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;

import java.io.IOException;
import java.util.List;

public interface MovingAvg extends MultiBucketsAggregation {

    public static enum GapPolicy {
        insert_zeros((byte) 0), interpolate((byte) 1), ignore((byte) 2);

        private byte id;

        private GapPolicy(byte id) {
            this.id = id;
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id);
        }

        public static GapPolicy readFrom(StreamInput in) throws IOException {
            byte id = in.readByte();
            for (GapPolicy gapPolicy : values()) {
                if (id == gapPolicy.id) {
                    return gapPolicy;
                }
            }
            throw new IllegalStateException("Unknown GapPolicy with id [" + id + "]");
        }
    }

    public static enum Weighting {
        simple((byte) 0), linear((byte) 1), ewma((byte) 2);

        private byte id;

        private Weighting(byte id) {
            this.id = id;
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id);
        }

        public static Weighting readFrom(StreamInput in) throws IOException {
            byte id = in.readByte();
            for (Weighting weight : values()) {
                if (id == weight.id) {
                    return weight;
                }
            }
            throw new IllegalStateException("Unknown Weighting with id [" + id + "]");
        }
    }

    /**
     * @return The buckets of this aggregation.
     */
    List<? extends Histogram.Bucket> getBuckets();

    /**
     * The bucket that is associated with the given key.
     * 
     * @param key
     *            The key of the requested bucket.
     * @return The bucket
     */
    Histogram.Bucket getBucketByKey(String key);
}
