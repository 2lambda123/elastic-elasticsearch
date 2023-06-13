/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
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
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class ComparisonTests extends ESTestCase {
    /**
     * This is a demo as well as a test. The scenario is that we have a thing that
     * normally has a moderately long-tailed distribution of response times. Then
     * some small fraction of transactions take 5x longer than normal. We need to
     * detect this by looking at the overall response time distribution.
     */
    public void testLatencyProblem() {
        Random gen = random();
        runSimulation(gen, new TdigestDetector(), 1000);
        runSimulation(gen, new TdigestDetector(), 100);
        runSimulation(gen, new TdigestDetector(), 10);

        runSimulation(gen, new LogHistogramDetector(), 1000);
        runSimulation(gen, new LogHistogramDetector(), 100);
        runSimulation(gen, new LogHistogramDetector(), 10);
    }

    private void runSimulation(Random gen, Detector d, double rate) {
        double dt = 1 / rate;

        double t = 0;
        double currentMinute = 0;

        // compare the distribution each minute against the previous hour
        double failureRate;
        while (t < 2 * 7200) {
            if (t - currentMinute >= 60) {
                currentMinute += 60;
            }
            if (t >= 7200) {
                // after one hour of no failure, we add 0.1% failures, half an hour later we go to 1% failure rate
                if (t >= 7200 + 3600) {
                    failureRate = 0.01;
                } else {
                    failureRate = 0.001;
                }
            } else {
                failureRate = 0;
            }

            d.add(latencySampler(failureRate, gen));
            t += -dt * Math.log(gen.nextDouble());
        }
    }

    private interface Detector {
        boolean isReady();

        void add(double sample);

        void flush();

        double score();

        String name();
    }

    private static class TdigestDetector implements Detector {
        double[] cuts = new double[] { 0.9, 0.99, 0.999, 0.9999 };

        List<TDigest> history = new ArrayList<>();
        TDigest current = new MergingDigest(100);

        @Override
        public boolean isReady() {
            return history.size() >= 60;
        }

        @Override
        public void add(double sample) {
            current.add(sample);
        }

        @Override
        public void flush() {
            history.add(current);
            current = new MergingDigest(100);
        }

        @Override
        public double score() {
            TDigest ref = new MergingDigest(100);
            ref.add(history.subList(history.size() - 60, history.size()));
            return Comparison.compareChi2(ref, current, cuts);
        }

        @Override
        public String name() {
            return "t-digest";
        }
    }

    private static class LogHistogramDetector implements Detector {
        List<Histogram> history = new ArrayList<>();
        LogHistogram current = new LogHistogram(0.1e-3, 1);

        @Override
        public boolean isReady() {
            return history.size() >= 60;
        }

        @Override
        public void add(double sample) {
            current.add(sample);
        }

        @Override
        public void flush() {
            history.add(current);
            current = new LogHistogram(0.1e-3, 1);
        }

        @Override
        public double score() {
            Histogram ref = new LogHistogram(0.1e-3, 1);
            ref.add(history);
            return Comparison.compareChi2(ref, current);
        }

        @Override
        public String name() {
            return "log-histogram";
        }
    }

    private double latencySampler(double failed, Random gen) {
        if (gen.nextDouble() < failed) {
            return 50e-3 * Math.exp(gen.nextGaussian() / 2);
        } else {
            return 10e-3 * Math.exp(gen.nextGaussian() / 2);
        }
    }

    public void testMergingDigests() {
        TDigest d1 = new MergingDigest(100);
        TDigest d2 = new MergingDigest(100);

        d1.add(1);
        d2.add(3);
        assertEquals(2.77, Comparison.compareChi2(d1, d2, new double[] { 1 }), 0.01);

        Random r = random();
        int failed = 0;
        for (int i = 0; i < 1000; i++) {
            d1 = new MergingDigest(100);
            d2 = new MergingDigest(100);
            MergingDigest d3 = new MergingDigest(100);
            for (int j = 0; j < 10000; j++) {
                // these should look the same
                d1.add(r.nextGaussian());
                d2.add(r.nextGaussian());
                // can we see a small difference
                d3.add(r.nextGaussian() + 0.3);
            }

            // 5 degrees of freedom, Pr(llr > 20) < 0.005
            if (Comparison.compareChi2(d1, d2, new double[] { 0.1, 0.3, 0.5, 0.8, 0.9 }) > 25) {
                failed++;
            }

            // 1 degree of freedom, Pr(llr > 10) < 0.005
            if (Comparison.compareChi2(d1, d2, new double[] { 0.1 }) > 20) {
                failed++;
            }

            // 1 degree of freedom, Pr(llr > 10) < 0.005
            if (Comparison.compareChi2(d1, d2, new double[] { 0.5 }) > 20) {
                failed++;
            }

            if (Comparison.compareChi2(d1, d3, new double[] { 0.1, 0.5, 0.9 }) < 90) {
                failed++;
            }
        }
        assertEquals(0, failed, 5);
    }

    public void testKsFunction() {
        Random r = random();
        for (int i = 0; i < 10; i++) {
            MergingDigest d1 = new MergingDigest(100);
            MergingDigest d2 = new MergingDigest(100);
            MergingDigest d3 = new MergingDigest(100);
            for (int j = 0; j < 1000000; j++) {
                d1.add(r.nextGaussian());
                d2.add(r.nextGaussian() + 1);
                d3.add(r.nextGaussian());
            }
            double ks = Comparison.ks(d1, d2);
            // this value is slightly lower than it should be (by about 0.9)
            assertEquals(270.0, ks, 3.5);
            assertEquals(0, Comparison.ks(d1, d3), 3.5);
        }
    }

    public void testLogHistograms() {
        Random r = random();
        int failed = 0;

        try {
            Comparison.compareChi2(new LogHistogram(10e-6, 10), new LogHistogram(1e-6, 1));
            fail("Should have detected incompatible histograms (lower bound)");
        } catch (IllegalArgumentException e) {
            assertEquals("Incompatible histograms in terms of size or bounds", e.getMessage());
        }
        try {
            Comparison.compareChi2(new LogHistogram(10e-6, 10), new LogHistogram(10e-6, 1));
            fail("Should have detected incompatible histograms (size)");
        } catch (IllegalArgumentException e) {
            assertEquals("Incompatible histograms in terms of size or bounds", e.getMessage());
        }

        for (int i = 0; i < 1000; i++) {
            LogHistogram d1 = new LogHistogram(10e-6, 10);
            LogHistogram d2 = new LogHistogram(10e-6, 10);
            LogHistogram d3 = new LogHistogram(10e-6, 10);
            for (int j = 0; j < 10000; j++) {
                // these should look the same
                d1.add(Math.exp(r.nextGaussian()));
                d2.add(Math.exp(r.nextGaussian()));
                // can we see a small difference
                d3.add(Math.exp(r.nextGaussian() + 0.5));
            }

            // 144 degrees of freedom, Pr(llr > 250) < 1e-6
            if (Comparison.compareChi2(d1, d2) > 250) {
                failed++;
            }

            if (Comparison.compareChi2(d1, d3) < 1000) {
                failed++;
            }
        }
        assertEquals(0, failed, 5);
    }

    public void TestLlrFunction() {
        double[][] count = new double[2][2];
        count[0][0] = 1;
        count[1][1] = 1;
        assertEquals(2.77, Comparison.llr(count), 0.01);

        count[0][0] = 3;
        count[0][1] = 1;
        count[1][0] = 1;
        count[1][1] = 3;
        assertEquals(2.09, Comparison.llr(count), 0.01);

        count[1][1] = 5;
        assertEquals(3.55, Comparison.llr(count), 0.01);
    }

    public void testRandomDenseDistribution() {
        final int SAMPLE_COUNT = 1_000_000;
        final int COMPRESSION = 100;

        TDigest avlTreeDigest = TDigest.createAvlTreeDigest(COMPRESSION);
        TDigest mergingDigest = TDigest.createMergingDigest(COMPRESSION);
        double[] samples = new double[SAMPLE_COUNT];

        var rand = random();
        for (int i = 0; i < SAMPLE_COUNT; i++) {
            samples[i] = rand.nextDouble();
            avlTreeDigest.add(samples[i]);
            mergingDigest.add(samples[i]);
        }
        Arrays.sort(samples);

        for (double percentile : new double[] { 0, 0.01, 0.1, 1, 5, 10, 25, 50, 75, 90, 99, 99.9, 99.99, 100.0 }) {
            double q = percentile / 100.0;
            double expected = Dist.quantile(q, samples);
            double accuracy = percentile > 1 ? Math.abs(expected / 10) : Math.abs(expected);
            assertEquals(String.valueOf(percentile), expected, avlTreeDigest.quantile(q), accuracy);
            assertEquals(String.valueOf(percentile), expected, mergingDigest.quantile(q), accuracy);
        }
    }

    public void testRandomSparseDistribution() {
        final int SAMPLE_COUNT = 1_000_000;
        final int COMPRESSION = 100;

        TDigest avlTreeDigest = TDigest.createAvlTreeDigest(COMPRESSION);
        TDigest mergingDigest = TDigest.createMergingDigest(COMPRESSION);
        double[] samples = new double[SAMPLE_COUNT];

        var rand = random();
        for (int i = 0; i < SAMPLE_COUNT; i++) {
            samples[i] = rand.nextDouble() * SAMPLE_COUNT * SAMPLE_COUNT + SAMPLE_COUNT;
            avlTreeDigest.add(samples[i]);
            mergingDigest.add(samples[i]);
        }
        Arrays.sort(samples);

        for (double percentile : new double[] { 0, 0.01, 0.1, 1, 5, 10, 25, 50, 75, 90, 99, 99.9, 99.99, 100.0 }) {
            double q = percentile / 100.0;
            double expected = Dist.quantile(q, samples);
            double accuracy = percentile > 1 ? Math.abs(expected / 10) : Math.abs(expected);
            assertEquals(String.valueOf(percentile), expected, avlTreeDigest.quantile(q), accuracy);
            assertEquals(String.valueOf(percentile), expected, mergingDigest.quantile(q), accuracy);
        }
    }

    public void testDenseGaussianDistribution() {
        final int SAMPLE_COUNT = 1_000_000;
        final int COMPRESSION = 100;

        TDigest avlTreeDigest = TDigest.createAvlTreeDigest(COMPRESSION);
        TDigest mergingDigest = TDigest.createMergingDigest(COMPRESSION);
        double[] samples = new double[SAMPLE_COUNT];

        var rand = random();
        for (int i = 0; i < SAMPLE_COUNT; i++) {
            samples[i] = rand.nextGaussian();
            avlTreeDigest.add(samples[i]);
            mergingDigest.add(samples[i]);
        }
        Arrays.sort(samples);

        for (double percentile : new double[] { 0, 0.01, 0.1, 1, 5, 10, 25, 75, 90, 99, 99.9, 99.99, 100.0 }) {
            double q = percentile / 100.0;
            double expected = Dist.quantile(q, samples);
            double accuracy = percentile > 1 ? Math.abs(expected / 10) : Math.abs(expected);
            assertEquals(String.valueOf(percentile), expected, avlTreeDigest.quantile(q), accuracy);
            assertEquals(String.valueOf(percentile), expected, mergingDigest.quantile(q), accuracy);
        }

        double expectedMedian = Dist.quantile(0.5, samples);
        assertEquals(expectedMedian, avlTreeDigest.quantile(0.5), 0.01);
        assertEquals(expectedMedian, mergingDigest.quantile(0.5), 0.01);
    }

    public void testSparseGaussianDistribution() {
        final int SAMPLE_COUNT = 1_000_000;
        final int COMPRESSION = 100;

        TDigest avlTreeDigest = TDigest.createAvlTreeDigest(COMPRESSION);
        TDigest mergingDigest = TDigest.createMergingDigest(COMPRESSION);
        double[] samples = new double[SAMPLE_COUNT];
        var rand = random();

        for (int i = 0; i < SAMPLE_COUNT; i++) {
            samples[i] = rand.nextGaussian() * SAMPLE_COUNT;
            avlTreeDigest.add(samples[i]);
            mergingDigest.add(samples[i]);
        }
        Arrays.sort(samples);

        for (double percentile : new double[] { 0, 0.01, 0.1, 1, 5, 10, 25, 75, 90, 99, 99.9, 99.99, 100.0 }) {
            double q = percentile / 100.0;
            double expected = Dist.quantile(q, samples);
            double accuracy = percentile > 1 ? Math.abs(expected / 10) : Math.abs(expected);
            assertEquals(String.valueOf(percentile), expected, avlTreeDigest.quantile(q), accuracy);
            assertEquals(String.valueOf(percentile), expected, mergingDigest.quantile(q), accuracy);
        }

        // The absolute value of median is within [0,5000], which is deemed close enough to 0 compared to the max value.
        double expectedMedian = Dist.quantile(0.5, samples);
        assertEquals(expectedMedian, avlTreeDigest.quantile(0.5), 5000);
        assertEquals(expectedMedian, mergingDigest.quantile(0.5), 5000);
    }
}
