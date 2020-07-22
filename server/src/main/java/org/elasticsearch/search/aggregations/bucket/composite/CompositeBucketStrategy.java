/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.geo.GeoBoundingBox;

/**
 * This class acts as a bit of syntactic sugar to let us pass in the rounding info for dates or the interval for numeric histograms as one
 * class, to save needing three different interfaces.  Sometimes I miss C-style Union structures.
 */
public class CompositeBucketStrategy {
    public enum Strategy {
        ROUNDING,
        INTERVAL,
        GEOTILE,
        NONE
    }

    private final Strategy strategy;

    private final Rounding rounding;
    private final double interval;

    private final int precision;
    private final GeoBoundingBox boundingBox;


    public CompositeBucketStrategy() {
        this.strategy = Strategy.NONE;
        this.rounding = null;

        this.interval = Double.NaN;
        this.precision = 0;
        this.boundingBox = null;
    }

    public CompositeBucketStrategy(Rounding rounding) {
        this.strategy = Strategy.ROUNDING;
        this.rounding = rounding;

        this.interval = Double.NaN;
        this.precision = 0;
        this.boundingBox = null;
    }

    public CompositeBucketStrategy(double interval) {
        this.strategy = Strategy.INTERVAL;
        this.interval = interval;

        this.rounding = null;
        this.precision = 0;
        this.boundingBox = null;
    }

    public CompositeBucketStrategy(int precision, GeoBoundingBox boundingBox) {
        this.strategy = Strategy.GEOTILE;
        this.precision = precision;
        this.boundingBox = boundingBox;

        this.interval = 0;
        this.rounding = null;
    }

    public Rounding getRounding() {
        assert strategy == Strategy.ROUNDING;
        return rounding;
    }

    public double getInterval() {
        assert strategy == Strategy.INTERVAL;
        return interval;
    }

    public int getPrecision() {
        assert strategy == Strategy.GEOTILE;
        return precision;
    }

    public GeoBoundingBox getBoundingBox() {
        assert strategy == Strategy.GEOTILE;
        return boundingBox;
    }

}
