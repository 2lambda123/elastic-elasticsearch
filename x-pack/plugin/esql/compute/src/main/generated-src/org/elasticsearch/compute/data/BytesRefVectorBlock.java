/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Releasables;

/**
 * Block view of a BytesRefVector.
 * This class is generated. Do not edit it.
 */
public sealed class BytesRefVectorBlock extends AbstractVectorBlock implements BytesRefBlock permits BytesRefVectorBlock.ShallowCopy {

    final class ShallowCopy extends BytesRefVectorBlock {
        /**
         * The vector must be a shallow copy of the original vector.
         */
        ShallowCopy(BytesRefVector vector) {
            super(vector);
        }

        @Override
        public void close() {
            BytesRefVectorBlock.this.close();
        }
    }

    private final BytesRefVector vector;

    BytesRefVectorBlock(BytesRefVector vector) {
        super(vector.getPositionCount(), vector.blockFactory());
        this.vector = vector;
    }

    @Override
    public BytesRefVector asVector() {
        return vector;
    }

    @Override
    public BytesRef getBytesRef(int valueIndex, BytesRef dest) {
        return vector.getBytesRef(valueIndex, dest);
    }

    @Override
    public int getTotalValueCount() {
        return vector.getPositionCount();
    }

    @Override
    public ElementType elementType() {
        return vector.elementType();
    }

    @Override
    public BytesRefBlock filter(int... positions) {
        return new ShallowCopy(new FilterBytesRefVector(vector, positions));
    }

    @Override
    public long ramBytesUsed() {
        return vector.ramBytesUsed();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BytesRefBlock that) {
            return BytesRefBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return BytesRefBlock.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[vector=" + vector + "]";
    }

    @Override
    public void close() {
        if (released) {
            throw new IllegalStateException("can't release already released block [" + this + "]");
        }
        released = true;
        Releasables.closeExpectNoException(vector);
    }
}
