/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.util.VectorUtil;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class KnnDenseVector implements DenseVector {
    protected final float[] vector;

    public KnnDenseVector(float[] vector) {
        this.vector = vector;
    }

    @Override
    public float getMagnitude() {
        double magnitude = 0.0f;
        for (float elem : vector) {
            magnitude += elem * elem;
        }
        return (float) Math.sqrt(magnitude);
    }

    @Override
    public double dotProduct(QueryVector queryVector) {
        return VectorUtil.dotProduct(vector, queryVector.asFloatArray());
    }

    @Override
    public double l1Norm(QueryVector queryVector) {
        double result = 0.0;
        for (int i = 0; i < queryVector.size(); i++) {
            result += Math.abs(vector[i] - queryVector.get(i));
        }
        return result;
    }

    @Override
    public double l2Norm(QueryVector queryVector) {
        return Math.sqrt(VectorUtil.squareDistance(vector, queryVector.asFloatArray()));
    }

    @Override
    public float[] getVector() {
        // we need to copy the value, since {@link VectorValues} can reuse
        // the underlying array across documents
        return Arrays.copyOf(vector, vector.length);
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public int getDims() {
        return vector.length;
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public Iterator<Float> iterator() {
        return new Iterator<>() {
            int index = 0;

            @Override
            public Float next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                return vector[index++];
            }

            @Override
            public boolean hasNext() {
                return index < vector.length;
            }
        };
    }
}
