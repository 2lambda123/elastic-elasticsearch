/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfig;
import org.elasticsearch.xpack.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.NlpTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;

import java.util.List;

/**
 * A NLP processor that returns a single double[] output from the model. Assumes that only one tensor is returned via inference
 **/
public class TextEmbeddingProcessor implements NlpTask.Processor {

    private final NlpTask.RequestBuilder requestBuilder;

    TextEmbeddingProcessor(NlpTokenizer tokenizer, TextEmbeddingConfig config) {
        this.requestBuilder = tokenizer.requestBuilder();
    }

    @Override
    public void validateInputs(List<String> inputs) {
        // nothing to validate
    }

    @Override
    public NlpTask.RequestBuilder getRequestBuilder() {
        return requestBuilder;
    }

    @Override
    public NlpTask.ResultProcessor getResultProcessor() {
        return TextEmbeddingProcessor::processResult;
    }

    private static InferenceResults processResult(TokenizationResult tokenization, PyTorchResult pyTorchResult) {
        // TODO - process all results in the batch
        return new TextEmbeddingResults(pyTorchResult.getInferenceResult()[0][0]);
    }
}
