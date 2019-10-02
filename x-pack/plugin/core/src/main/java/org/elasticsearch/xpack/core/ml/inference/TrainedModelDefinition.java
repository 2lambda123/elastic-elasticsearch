/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.LenientlyParsedPreProcessor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.PreProcessor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.StrictlyParsedPreProcessor;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TrainedModelDefinition implements ToXContentObject, Writeable {

    public static final String NAME = "trained_model_doc";

    public static final ParseField TRAINED_MODEL = new ParseField("trained_model");
    public static final ParseField PREPROCESSORS = new ParseField("preprocessors");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ObjectParser<TrainedModelDefinition.Builder, Void> LENIENT_PARSER = createParser(true);
    public static final ObjectParser<TrainedModelDefinition.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<TrainedModelDefinition.Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<TrainedModelDefinition.Builder, Void> parser = new ObjectParser<>(NAME,
            ignoreUnknownFields,
            TrainedModelDefinition.Builder::new);
        parser.declareNamedObjects(TrainedModelDefinition.Builder::setTrainedModel,
            (p, c, n) -> ignoreUnknownFields ?
                p.namedObject(LenientlyParsedTrainedModel.class, n, null) :
                p.namedObject(StrictlyParsedTrainedModel.class, n, null),
            (modelDocBuilder) -> { /* Noop does not matter as we will throw if more than one is defined */ },
            TRAINED_MODEL);
        parser.declareNamedObjects(TrainedModelDefinition.Builder::setPreProcessors,
            (p, c, n) -> ignoreUnknownFields ?
                p.namedObject(LenientlyParsedPreProcessor.class, n, null) :
                p.namedObject(StrictlyParsedPreProcessor.class, n, null),
            (trainedModelDefBuilder) -> trainedModelDefBuilder.setProcessorsInOrder(true),
            PREPROCESSORS);
        return parser;
    }

    public static TrainedModelDefinition.Builder fromXContent(XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.parse(parser, null) : STRICT_PARSER.parse(parser, null);
    }

    private final TrainedModel trainedModel;
    private final List<PreProcessor> preProcessors;

    TrainedModelDefinition(TrainedModel trainedModel, List<PreProcessor> preProcessors) {
        this.trainedModel = trainedModel;
        this.preProcessors = preProcessors == null ? Collections.emptyList() : Collections.unmodifiableList(preProcessors);
    }

    public TrainedModelDefinition(StreamInput in) throws IOException {
        this.trainedModel = in.readNamedWriteable(TrainedModel.class);
        this.preProcessors = in.readNamedWriteableList(PreProcessor.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(trainedModel);
        out.writeNamedWriteableList(preProcessors);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        NamedXContentObjectHelper.writeNamedObjects(builder,
            params,
            false,
            TRAINED_MODEL.getPreferredName(),
            Collections.singletonList(trainedModel));
        NamedXContentObjectHelper.writeNamedObjects(builder,
            params,
            true,
            PREPROCESSORS.getPreferredName(),
            preProcessors);
        builder.endObject();
        return builder;
    }

    public TrainedModel getTrainedModel() {
        return trainedModel;
    }

    public List<PreProcessor> getPreProcessors() {
        return preProcessors;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelDefinition that = (TrainedModelDefinition) o;
        return Objects.equals(trainedModel, that.trainedModel) &&
            Objects.equals(preProcessors, that.preProcessors) ;
    }

    @Override
    public int hashCode() {
        return Objects.hash(trainedModel, preProcessors);
    }

    public static class Builder {

        private List<PreProcessor> preProcessors;
        private TrainedModel trainedModel;
        private boolean processorsInOrder;

        private static Builder builderForParser() {
            return new Builder(false);
        }

        private Builder(boolean processorsInOrder) {
            this.processorsInOrder = processorsInOrder;
        }

        public Builder() {
            this(true);
        }

        public Builder setPreProcessors(List<PreProcessor> preProcessors) {
            this.preProcessors = preProcessors;
            return this;
        }

        public Builder setTrainedModel(TrainedModel trainedModel) {
            this.trainedModel = trainedModel;
            return this;
        }

        private Builder setTrainedModel(List<TrainedModel> trainedModel) {
            if (trainedModel.size() != 1) {
                throw ExceptionsHelper.badRequestException("[{}] must have exactly one trained model defined.",
                    TRAINED_MODEL.getPreferredName());
            }
            return setTrainedModel(trainedModel.get(0));
        }

        private void setProcessorsInOrder(boolean value) {
            this.processorsInOrder = value;
        }

        public TrainedModelDefinition build() {
            if (preProcessors != null && preProcessors.size() > 1 && processorsInOrder == false) {
                throw new IllegalArgumentException("preprocessors must be an array of preprocessor objects");
            }
            return new TrainedModelDefinition(this.trainedModel, this.preProcessors);
        }
    }

}
