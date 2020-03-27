/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.aggregatemetric.aggregations.metrics;

import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MetricAggregatorSupplier;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xpack.aggregatemetric.aggregations.support.AggregateMetricsValuesSource;
import org.elasticsearch.xpack.aggregatemetric.aggregations.support.AggregateMetricsValuesSourceType;

public class AggregateMetricsAggregatorsRegistrar {

    public static void registerSumAggregator(ValuesSourceRegistry valuesSourceRegistry) {
        valuesSourceRegistry.register(SumAggregationBuilder.NAME,
            AggregateMetricsValuesSourceType.AGGREGATE_METRIC,
            (MetricAggregatorSupplier) (name, valuesSource, formatter, context, parent, pipelineAggregators, metaData) ->
                new AggregateMetricBackedSumAggregator(name, (AggregateMetricsValuesSource.AggregateDoubleMetric) valuesSource,
                    formatter, context, parent, pipelineAggregators, metaData));
    }


    public static void registerAvgAggregator(ValuesSourceRegistry valuesSourceRegistry) {
        valuesSourceRegistry.register(AvgAggregationBuilder.NAME,
            AggregateMetricsValuesSourceType.AGGREGATE_METRIC,
            (MetricAggregatorSupplier) (name, valuesSource, formatter, context, parent, pipelineAggregators, metaData) ->
                new AggregateMetricBackedAvgAggregator(name, (AggregateMetricsValuesSource.AggregateDoubleMetric) valuesSource,
                    formatter, context, parent, pipelineAggregators, metaData));
    }

    public static void registerMinAggregator(ValuesSourceRegistry valuesSourceRegistry) {
        valuesSourceRegistry.register(MinAggregationBuilder.NAME,
            AggregateMetricsValuesSourceType.AGGREGATE_METRIC,
            (MetricAggregatorSupplier) (name, valuesSource, formatter, context, parent, pipelineAggregators, metaData) ->
                new AggregateMetricBackedAvgAggregator(name, (AggregateMetricsValuesSource.AggregateDoubleMetric) valuesSource,
                    formatter, context, parent, pipelineAggregators, metaData));
    }

    public static void registerMaxAggregator(ValuesSourceRegistry valuesSourceRegistry) {
        valuesSourceRegistry.register(MaxAggregationBuilder.NAME,
            AggregateMetricsValuesSourceType.AGGREGATE_METRIC,
            (MetricAggregatorSupplier) (name, valuesSource, formatter, context, parent, pipelineAggregators, metaData) ->
                new AggregateMetricBackedAvgAggregator(name, (AggregateMetricsValuesSource.AggregateDoubleMetric) valuesSource,
                    formatter, context, parent, pipelineAggregators, metaData));
    }
}
