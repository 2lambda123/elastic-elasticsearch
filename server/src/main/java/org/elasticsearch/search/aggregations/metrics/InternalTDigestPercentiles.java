/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

public class InternalTDigestPercentiles extends AbstractInternalTDigestPercentiles implements Percentiles {
    public static final String NAME = "tdigest_percentiles";

    public InternalTDigestPercentiles(
        String name,
        double[] percents,
        TDigestState state,
        boolean keyed,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        super(name, percents, state, keyed, formatter, metadata);
    }

    /**
     * Read from a stream.
     */
    public InternalTDigestPercentiles(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Iterator<Percentile> iterator() {
        return new Iter(keys, state);
    }

    @Override
    public double percentile(double percent) {
        return state.quantile(percent / 100);
    }

    @Override
    public String percentileAsString(double percent) {
        return valueAsString(String.valueOf(percent));
    }

    @Override
    public double value(double key) {
        return percentile(key);
    }

    @Override
    public double value(String keyName) {
        return Arrays.stream(keys).filter(key -> String.valueOf(key).equals(keyName)).findFirst().orElseGet(() -> {
            if (keys.length != 1) {
                throw new IllegalArgumentException(
                    "percentiles ["
                        + name
                        + "] no percentiles available matching key ["
                        + keyName
                        + "], available metrics ["
                        + String.join(", ", Arrays.stream(keys).mapToObj(String::valueOf).collect(Collectors.joining()))
                        + "]"
                );
            }
            if (!this.iterator().hasNext()) {
                throw new IllegalArgumentException("percentiles [" + name + "] no percentile value available");
            }
                return this.iterator().next().getValue();
        }

        );
    }

    @Override
    protected AbstractInternalTDigestPercentiles createReduced(
        String name,
        double[] keys,
        TDigestState merged,
        boolean keyed,
        Map<String, Object> metadata
    ) {
        return new InternalTDigestPercentiles(name, keys, merged, keyed, format, metadata);
    }

    public static class Iter implements Iterator<Percentile> {

        private final double[] percents;
        private final TDigestState state;
        private int i;

        public Iter(double[] percents, TDigestState state) {
            this.percents = percents;
            this.state = state;
            i = 0;
        }

        @Override
        public boolean hasNext() {
            return i < percents.length;
        }

        @Override
        public Percentile next() {
            final Percentile next = new Percentile(percents[i], state.quantile(percents[i] / 100));
            ++i;
            return next;
        }

        @Override
        public final void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
