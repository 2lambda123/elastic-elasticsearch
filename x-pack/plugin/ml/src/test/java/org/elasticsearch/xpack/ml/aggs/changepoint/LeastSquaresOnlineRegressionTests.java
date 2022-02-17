/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;

public class LeastSquaresOnlineRegressionTests extends ESTestCase {

    public void testAddRemove() {
        LeastSquaresOnlineRegression lsLess = new LeastSquaresOnlineRegression(2);
        LeastSquaresOnlineRegression lsAll = new LeastSquaresOnlineRegression(2);
        double[] x = new double[] { 1.0, 4.0, 2.0, 15.0, 20.0 };
        double[] xLess = new double[] { 1.0, 4.0, 2.0, 15.0 };
        double[] y = new double[] { 2.0, 8.0, 7.0, 22.0, 23.0 };
        double[] yLess = new double[] { 2.0, 8.0, 7.0, 22.0 };
        for (int i = 0; i < y.length; i++) {
            lsAll.add(x[i], y[i]);
        }
        for (int i = 0; i < yLess.length; i++) {
            lsLess.add(xLess[i], yLess[i]);
        }
        double rsAll = lsAll.squareResidual(x, y, lsAll.parameters());
        double rsLess = lsLess.squareResidual(xLess, yLess, lsLess.parameters());

        lsAll.remove(x[x.length - 1], y[y.length - 1]);
        lsLess.add(x[x.length - 1], y[y.length - 1]);

        assertThat(rsAll, closeTo(lsLess.squareResidual(x, y, lsLess.parameters()), 1e-12));
        assertThat(rsLess, closeTo(lsAll.squareResidual(xLess, yLess, lsAll.parameters()), 1e-12));
    }

    public void testOnlineRegression() {
        LeastSquaresOnlineRegression ls = new LeastSquaresOnlineRegression(2);
        OLSMultipleLinearRegression linearRegression = new OLSMultipleLinearRegression(0);
        double[] x = new double[] { 1.0, 4.0, 2.0, 15.0, 20.0 };
        double[] y = new double[] { 2.0, 8.0, 7.0, 22.0, 23.0 };
        double[][] xs = new double[y.length][];
        for (int i = 0; i < y.length; i++) {
            xs[i] = new double[] { x[i], x[i] * x[i] };
            ls.add(x[i], y[i]);
        }
        linearRegression.newSampleData(y, xs);
        double slowRSquared = linearRegression.calculateRSquared();
        double[] params = ls.parameters();
        double rs = ls.squareResidual(x, y, params);
        assertThat(rs, closeTo(slowRSquared, 1e-15));

        // Test removing the last value
        xs = new double[y.length - 1][];
        for (int i = 0; i < y.length - 1; i++) {
            xs[i] = new double[] { x[i], x[i] * x[i] };
        }
        linearRegression.newSampleData(new double[] { 2.0, 8.0, 7.0, 22.0 }, xs);
        slowRSquared = linearRegression.calculateRSquared();
        ls.remove(x[x.length - 1], y[y.length - 1]);
        params = ls.parameters();
        rs = ls.squareResidual(new double[] { 1.0, 4.0, 2.0, 15.0 }, new double[] { 2.0, 8.0, 7.0, 22.0 }, params);
        assertThat(rs, closeTo(slowRSquared, 1e-15));

    }

}
