// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import java.util.List;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link CountDistinctLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class CountDistinctLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final DriverContext driverContext;

  private final List<Integer> channels;

  private final int precision;

  public CountDistinctLongAggregatorFunctionSupplier(DriverContext driverContext,
      List<Integer> channels, int precision) {
    this.driverContext = driverContext;
    this.channels = channels;
    this.precision = precision;
  }

  @Override
  public CountDistinctLongAggregatorFunction aggregator() {
    return CountDistinctLongAggregatorFunction.create(channels, driverContext, precision);
  }

  @Override
  public CountDistinctLongGroupingAggregatorFunction groupingAggregator() {
    return CountDistinctLongGroupingAggregatorFunction.create(channels, driverContext, precision);
  }

  @Override
  public String describe() {
    return "count_distinct of longs";
  }
}
