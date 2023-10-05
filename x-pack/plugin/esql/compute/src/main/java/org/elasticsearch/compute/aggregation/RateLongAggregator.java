/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;

@Aggregator({ @IntermediateState(name = "rate", type = "BYTES_REF") })
@GroupingAggregator
class RateLongAggregator {

    public static RateStates.SingleState initSingle() {
        return new RateStates.SingleState();
    }

    public static void combine(RateStates.SingleState current, long v) {
        current.add(v);
    }

    public static void combineStates(RateStates.SingleState current, RateStates.SingleState state) {
        current.add(state);
    }

    public static void combineIntermediate(RateStates.SingleState state, BytesRef inValue) {
        state.add(inValue);
    }

    public static Block evaluateFinal(RateStates.SingleState state, DriverContext driverContext) {
        return state.evaluateDelta(driverContext);
    }

    public static RateStates.GroupingState initGrouping(BigArrays bigArrays) {
        return new RateStates.GroupingState(bigArrays);
    }

    public static void combine(RateStates.GroupingState state, int groupId, long v) {
        state.add(groupId, v);
    }

    public static void combineIntermediate(RateStates.GroupingState state, int groupId, BytesRef inValue) {
        state.add(groupId, inValue);
    }

    public static void combineStates(
        RateStates.GroupingState current,
        int currentGroupId,
        RateStates.GroupingState state,
        int statePosition
    ) {
        current.add(currentGroupId, state.getOrNull(statePosition));
    }

    public static Block evaluateFinal(RateStates.GroupingState state, IntVector selectedGroups, DriverContext driverContext) {
        return state.evaluateDelta(selectedGroups, driverContext);
    }
}
