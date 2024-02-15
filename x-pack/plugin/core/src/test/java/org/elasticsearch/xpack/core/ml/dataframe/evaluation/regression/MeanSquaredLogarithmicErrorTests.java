/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.regression;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.xpack.core.ml.dataframe.evaluation.MockAggregations.mockSingleValue;
import static org.hamcrest.Matchers.equalTo;

public class MeanSquaredLogarithmicErrorTests extends AbstractXContentSerializingTestCase<MeanSquaredLogarithmicError> {

    @Override
    protected MeanSquaredLogarithmicError doParseInstance(XContentParser parser) throws IOException {
        return MeanSquaredLogarithmicError.fromXContent(parser);
    }

    @Override
    protected MeanSquaredLogarithmicError createTestInstance() {
        return createRandom();
    }

    @Override
    protected MeanSquaredLogarithmicError mutateInstance(MeanSquaredLogarithmicError instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<MeanSquaredLogarithmicError> instanceReader() {
        return MeanSquaredLogarithmicError::new;
    }

    public static MeanSquaredLogarithmicError createRandom() {
        return new MeanSquaredLogarithmicError(randomBoolean() ? randomDoubleBetween(0.0, 1000.0, false) : null);
    }

    public void testEvaluate() {
        InternalAggregations aggs = InternalAggregations.from(
            Arrays.asList(mockSingleValue("regression_msle", 0.8123), mockSingleValue("some_other_single_metric_agg", 0.2377))
        );

        MeanSquaredLogarithmicError msle = new MeanSquaredLogarithmicError((Double) null);
        msle.process(aggs);

        EvaluationMetricResult result = msle.getResult().get();
        String expected = "{\"value\":0.8123}";
        assertThat(Strings.toString(result), equalTo(expected));
    }

    public void testEvaluate_GivenMissingAggs() {
        InternalAggregations aggs = InternalAggregations.from(
            Collections.singletonList(mockSingleValue("some_other_single_metric_agg", 0.2377))
        );

        MeanSquaredLogarithmicError msle = new MeanSquaredLogarithmicError((Double) null);
        msle.process(aggs);

        EvaluationMetricResult result = msle.getResult().get();
        assertThat(result, equalTo(new MeanSquaredLogarithmicError.Result(0.0)));
    }
}
