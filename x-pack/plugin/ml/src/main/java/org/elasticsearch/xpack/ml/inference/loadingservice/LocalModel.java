/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.loadingservice;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceStats;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;
import org.elasticsearch.xpack.core.ml.utils.MapHelper;
import org.elasticsearch.xpack.ml.inference.TrainedModelStatsService;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ml.job.messages.Messages.INFERENCE_WARNING_ALL_FIELDS_MISSING;

public class LocalModel implements Model {

    private static final long MIN_PERSISTENCE_INTERVAL = TimeValue.timeValueMinutes(1).getMillis();

    private final TrainedModelDefinition trainedModelDefinition;
    private final String modelId;
    private final Set<String> fieldNames;
    private final Map<String, String> defaultFieldMap;
    private final InferenceStats.Accumulator statsAccumulator;
    private final TrainedModelStatsService trainedModelStatsService;
    private final Supplier<Long> nanoTimeSupplier;
    private volatile long lastStatsQueue;
    private volatile long persistenceQuotient = 100;
    private final LongAdder currentInferenceCount;

    public LocalModel(String modelId,
                      TrainedModelDefinition trainedModelDefinition,
                      TrainedModelInput input,
                      Map<String, String> defaultFieldMap,
                      InferenceStats previousStats,
                      TrainedModelStatsService trainedModelStatsService) {
        this(modelId, trainedModelDefinition, input, defaultFieldMap, previousStats, trainedModelStatsService, System::nanoTime);
    }

    LocalModel(String modelId,
               TrainedModelDefinition trainedModelDefinition,
               TrainedModelInput input,
               Map<String, String> defaultFieldMap,
               InferenceStats previousStats,
               TrainedModelStatsService trainedModelStatsService,
               Supplier<Long> nanoTimeSupplier) {
        this.trainedModelDefinition = trainedModelDefinition;
        this.modelId = modelId;
        this.fieldNames = new HashSet<>(input.getFieldNames());
        this.statsAccumulator = new InferenceStats.Accumulator(previousStats);
        this.trainedModelStatsService = trainedModelStatsService;
        this.defaultFieldMap = defaultFieldMap == null ? null : new HashMap<>(defaultFieldMap);
        this.currentInferenceCount = new LongAdder();
        this.nanoTimeSupplier = nanoTimeSupplier;
        this.lastStatsQueue = nanoTimeSupplier.get();
    }

    long ramBytesUsed() {
        return trainedModelDefinition.ramBytesUsed();
    }

    @Override
    public String getModelId() {
        return modelId;
    }

    @Override
    public InferenceStats getLatestStats() {
        return statsAccumulator.currentStats();
    }

    @Override
    public String getResultsType() {
        switch (trainedModelDefinition.getTrainedModel().targetType()) {
            case CLASSIFICATION:
                return ClassificationInferenceResults.NAME;
            case REGRESSION:
                return RegressionInferenceResults.NAME;
            default:
                throw ExceptionsHelper.badRequestException("Model [{}] has unsupported target type [{}]",
                    modelId,
                    trainedModelDefinition.getTrainedModel().targetType());
        }
    }

    void persistStats() {
        trainedModelStatsService.queueStats(getLatestStats());
        lastStatsQueue = nanoTimeSupplier.get();
        if (persistenceQuotient < 1000 && currentInferenceCount.sum() > 1000) {
            persistenceQuotient = 1000;
        }
        if (persistenceQuotient < 10_000 && currentInferenceCount.sum() > 10_000) {
            persistenceQuotient = 10_000;
        }
    }

    @Override
    public void infer(Map<String, Object> fields, InferenceConfig config, ActionListener<InferenceResults> listener) {
        try {
            statsAccumulator.incInference();
            currentInferenceCount.increment();

            Model.mapFieldsIfNecessary(fields, defaultFieldMap);

            long startTimeInNanos = nanoTimeSupplier.get();
            boolean shouldPersistStats =
                (TimeUnit.NANOSECONDS.toMillis(startTimeInNanos - lastStatsQueue) > MIN_PERSISTENCE_INTERVAL)
                || ((currentInferenceCount.sum() + 1) % persistenceQuotient == 0);
            if (fieldNames.stream().allMatch(f -> MapHelper.dig(f, fields) == null)) {
                statsAccumulator.incMissingFields();
                if (shouldPersistStats) {
                    persistStats();
                }
                listener.onResponse(new WarningInferenceResults(Messages.getMessage(INFERENCE_WARNING_ALL_FIELDS_MISSING, modelId)));
                return;
            }
            InferenceResults inferenceResults = trainedModelDefinition.infer(fields, config);
            statsAccumulator.timeSpent(nanoTimeSupplier.get() - startTimeInNanos);
            if (shouldPersistStats) {
                persistStats();
            }
            listener.onResponse(inferenceResults);
        } catch (Exception e) {
            statsAccumulator.incFailure();
            listener.onFailure(e);
        }
    }

}
