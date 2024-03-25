/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk.vec;

import org.elasticsearch.nativeaccess.VectorScorer;

abstract sealed class AbstractScalarQuantizedVectorScorer implements VectorScorer permits DotProduct, Euclidean, MaximumInnerProduct {

    static final VectorDistance DISTANCE_FUNCS = VectorDistanceProvider.lookup();

    protected final int dims;
    protected final int maxOrd;
    protected final float scoreCorrectionConstant;
    protected final VectorDataInput data;

    protected AbstractScalarQuantizedVectorScorer(int dims, int maxOrd, float scoreCorrectionConstant, VectorDataInput data) {
        this.dims = dims;
        this.maxOrd = maxOrd;
        this.scoreCorrectionConstant = scoreCorrectionConstant;
        this.data = data;
    }

    @Override
    public final int dims() {
        return dims;
    }

    @Override
    public final int maxOrd() {
        return maxOrd;
    }

    @Override
    public final void close() {
        data.close();
    }

    protected final void checkOrdinal(int ord) {
        if (ord < 0 || ord > maxOrd) {
            throw new IllegalArgumentException("illegal ordinal: " + ord);
        }
    }
}
