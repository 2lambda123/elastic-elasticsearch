/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;

@Aggregator({ @IntermediateState(name = "values", type = "LONG_BLOCK") })
@GroupingAggregator
class ValuesLongAggregator {

    public static SingleState initSingle(BigArrays bigArrays) {
        return new SingleState(bigArrays);
    }

    public static void combine(SingleState state, long v) {
        state.values.add(v);
    }

    public static void combineIntermediate(SingleState state, LongBlock block) {
        int start = block.getFirstValueIndex(0);
        int end = start + block.getValueCount(0);
        for (int i = start; i < end; i++) {
            combine(state, block.getLong(i));
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(BigArrays bigArrays) {
        return new GroupingState(bigArrays);
    }

    public static void combine(GroupingState state, int groupId, long v) {
        state.values.add(groupId, v);
    }

    public static void combineIntermediate(GroupingState state, int groupId, LongBlock values, int valuesPosition) {
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        for (int i = start; i < end; i++) {
            combine(state, groupId, values.getLong(i));
        }
    }

    public static void combineStates(GroupingState current, int currentGroupId, GroupingState state, int statePosition) {
        throw new UnsupportedOperationException();
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory(), selected);
    }

    public static class SingleState implements Releasable {
        private final LongHash values;

        private SingleState(BigArrays bigArrays) {
            values = new LongHash(1, bigArrays);
        }

        void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory());
        }

        Block toBlock(BlockFactory blockFactory) {
            if (values.size() == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
            LongBlock.Builder builder = blockFactory.newLongBlockBuilder((int) values.size());
            builder.beginPositionEntry();
            for (int id = 0; id < values.size(); id++) {
                builder.appendLong(values.get(id));
            }
            builder.endPositionEntry();
            return builder.build();
        }

        @Override
        public void close() {
            values.close();
        }
    }

    public static class GroupingState implements Releasable {
        private final LongLongHash values;

        private GroupingState(BigArrays bigArrays) {
            values = new LongLongHash(1, bigArrays);
        }

        void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory(), selected);
        }

        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            if (values.size() == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
            LongBlock.Builder builder = blockFactory.newLongBlockBuilder(selected.getPositionCount());
            for (int s = 0; s < selected.getPositionCount(); s++) {
                int selectedGroup = selected.getInt(s);
                /*
                 * Count can effectively be in three states - 0, 1, many. We use those
                 * states to buffer the first value, so we can avoid calling
                 * beginPositionEntry on single valued fields.
                 */
                int count = 0;
                long first = 0;
                for (int id = 0; id < values.size(); id++) {
                    if (values.getKey1(id) == selectedGroup) {
                        long value = values.getKey2(id);
                        switch (count) {
                            case 0 -> first = value;
                            case 1 -> {
                                builder.beginPositionEntry();
                                builder.appendLong(first);
                                builder.appendLong(value);
                            }
                            default -> builder.appendLong(value);
                        }
                        count++;
                    }
                }
                switch (count) {
                    case 0 -> builder.appendNull();
                    case 1 -> builder.appendLong(first);
                    default -> builder.endPositionEntry();
                }
            }
            LongBlock b = builder.build();
            return b;
        }

        void enableGroupIdTracking(SeenGroupIds seen) {
            // we figure out seen values from their
        }

        @Override
        public void close() {
            values.close();
        }
    }
}
