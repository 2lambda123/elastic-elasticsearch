/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.core.Releasables;

/**
 * Block view of a IntVector.
 * This class is generated. Do not edit it.
 */
public sealed class IntVectorBlock extends AbstractVectorBlock implements IntBlock permits IntVectorBlock.ShallowCopy {

    final class ShallowCopy extends IntVectorBlock {
        /**
         * The vector must be a shallow copy of the original vector.
         */
        ShallowCopy(IntVector vector) {
            super(vector);
        }

        @Override
        public void close() {
            IntVectorBlock.this.close();
        }
    }

    private final IntVector vector;

    IntVectorBlock(IntVector vector) {
        super(vector.getPositionCount(), vector.blockFactory());
        this.vector = vector;
    }

    @Override
    public IntVector asVector() {
        return vector;
    }

    @Override
    public int getInt(int valueIndex) {
        return vector.getInt(valueIndex);
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
    public IntBlock filter(int... positions) {
        return new ShallowCopy(new FilterIntVector(vector, positions));
    }

    @Override
    public long ramBytesUsed() {
        return vector.ramBytesUsed();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IntBlock that) {
            return IntBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return IntBlock.hash(this);
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
