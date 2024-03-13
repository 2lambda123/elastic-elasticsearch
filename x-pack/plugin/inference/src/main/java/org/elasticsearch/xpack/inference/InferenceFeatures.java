/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.xpack.inference.retriever.TextSimilarityRankRetrieverBuilder;

import java.util.Set;

/**
 * A set of features specifically for the inference plugin.
 */
public class InferenceFeatures implements FeatureSpecification {

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(TextSimilarityRankRetrieverBuilder.NODE_FEATURE);
    }
}
