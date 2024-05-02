/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FloatEmbedding extends Embedding<Float> {

    public FloatEmbedding(StreamInput in) throws IOException {
        this(in.readCollectionAsImmutableList(StreamInput::readFloat));
    }

    public FloatEmbedding(List<Float> embedding) {
        super(embedding);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(embedding, StreamOutput::writeFloat);
    }

    public static FloatEmbedding of(org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults embeddingResult) {
        List<Float> embeddingAsList = new ArrayList<>();
        float[] embeddingAsArray = embeddingResult.getInferenceAsFloat();
        for (float dim : embeddingAsArray) {
            embeddingAsList.add(dim);
        }
        return new FloatEmbedding(embeddingAsList);
    }
}
