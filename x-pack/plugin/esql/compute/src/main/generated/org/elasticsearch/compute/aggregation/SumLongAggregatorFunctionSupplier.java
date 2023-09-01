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
 * {@link AggregatorFunctionSupplier} implementation for {@link SumLongAggregator}.
 * This class is generated. Do not edit it.
 */
public final class SumLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final DriverContext driverContext;

  private final List<Integer> channels;

  public SumLongAggregatorFunctionSupplier(DriverContext driverContext, List<Integer> channels) {
    this.driverContext = driverContext;
    this.channels = channels;
  }

  @Override
  public SumLongAggregatorFunction aggregator() {
    return SumLongAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public SumLongGroupingAggregatorFunction groupingAggregator() {
    return SumLongGroupingAggregatorFunction.create(channels, driverContext);
  }

  @Override
  public String describe() {
    return "sum of longs";
  }
}
