/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationFields;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationParameters;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClassificationTests extends AbstractSerializingTestCase<Classification> {

    private static final EvaluationParameters EVALUATION_PARAMETERS = new EvaluationParameters(100);

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(MlEvaluationNamedXContentProvider.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlEvaluationNamedXContentProvider().getNamedXContentParsers());
    }

    public static Classification createRandom() {
        List<EvaluationMetric> metrics =
            randomSubsetOf(
                Arrays.asList(
                    AccuracyTests.createRandom(),
                    AucRocTests.createRandom(),
                    PrecisionTests.createRandom(),
                    RecallTests.createRandom(),
                    MulticlassConfusionMatrixTests.createRandom()));
        boolean usesAucRoc = metrics.stream().map(EvaluationMetric::getName).anyMatch(n -> AucRoc.NAME.getPreferredName().equals(n));
        boolean specifiesNestedFields = usesAucRoc || randomBoolean();
        String resultsNestedField = specifiesNestedFields ? randomAlphaOfLength(10) : null;
        return new Classification(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            resultsNestedField,
            specifiesNestedFields ? resultsNestedField + "." + randomAlphaOfLength(10) : null,
            specifiesNestedFields ? resultsNestedField + "." + randomAlphaOfLength(10) : null,
            metrics.isEmpty() ? null : metrics);
    }

    @Override
    protected Classification doParseInstance(XContentParser parser) throws IOException {
        return Classification.fromXContent(parser);
    }

    @Override
    protected Classification createTestInstance() {
        return createRandom();
    }

    @Override
    protected Writeable.Reader<Classification> instanceReader() {
        return Classification::new;
    }

    public void testConstructor_GivenMissingField() {
        String expectedErrorMessage =
            "Either all or none of [results_nested_field, predicted_class_name_field, predicted_probability_field] must be specified";
        {
            ElasticsearchStatusException e =
                expectThrows(ElasticsearchStatusException.class, () -> new Classification("foo", "bar", "baz", null, null, null));
            assertThat(e.getMessage(), is(equalTo(expectedErrorMessage)));
        }
        {
            ElasticsearchStatusException e =
                expectThrows(ElasticsearchStatusException.class, () -> new Classification("foo", "bar", null, "baz", null, null));
            assertThat(e.getMessage(), is(equalTo(expectedErrorMessage)));
        }
        {
            ElasticsearchStatusException e =
                expectThrows(ElasticsearchStatusException.class, () -> new Classification("foo", "bar", null, null, "baz", null));
            assertThat(e.getMessage(), is(equalTo(expectedErrorMessage)));
        }
    }

    public void testConstructor_GivenBadField() {
        {
            ElasticsearchStatusException e =
                expectThrows(
                    ElasticsearchStatusException.class,
                    () -> new Classification("foo", "bar", "results", "class_name", "results.class_probability", null));
            assertThat(
                e.getMessage(),
                is(equalTo(
                    "The value of [predicted_class_name_field] must start with the value of [results_nested_field] "
                    + "but it didn't ([class_name] is not a prefix of [results])")));
        }
        {
            ElasticsearchStatusException e =
                expectThrows(
                    ElasticsearchStatusException.class,
                    () -> new Classification("foo", "bar", "results", "results.class_name", "class_probability", null));
            assertThat(
                e.getMessage(),
                is(equalTo(
                    "The value of [predicted_probability_field] must start with the value of [results_nested_field] "
                    + "but it didn't ([class_probability] is not a prefix of [results])")));
        }
    }

    public void testConstructor_GivenEmptyMetrics() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> new Classification("foo", "bar", "baz", "baz2", "baz3", Collections.emptyList()));
        assertThat(e.getMessage(), equalTo("[classification] must have one or more metrics"));
    }

    public void testBuildSearch() {
        QueryBuilder userProvidedQuery =
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("field_A", "some-value"))
                .filter(QueryBuilders.termQuery("field_B", "some-other-value"));
        QueryBuilder expectedSearchQuery =
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.existsQuery("act"))
                .filter(QueryBuilders.existsQuery("pred"))
                .filter(QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("field_A", "some-value"))
                    .filter(QueryBuilders.termQuery("field_B", "some-other-value")));

        Classification evaluation = new Classification("act", "pred", null, null, null, Arrays.asList(new MulticlassConfusionMatrix()));

        SearchSourceBuilder searchSourceBuilder = evaluation.buildSearch(EVALUATION_PARAMETERS, userProvidedQuery);
        assertThat(searchSourceBuilder.query(), equalTo(expectedSearchQuery));
        assertThat(searchSourceBuilder.aggregations().count(), greaterThan(0));
    }

    public void testBuildSearch_WithNestedFields() {
        QueryBuilder userProvidedQuery =
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("field_A", "some-value"))
                .filter(QueryBuilders.termQuery("field_B", "some-other-value"));
        QueryBuilder expectedSearchQuery =
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.existsQuery("act"))
                .filter(QueryBuilders.existsQuery("pred"))
                .filter(QueryBuilders.nestedQuery("results", QueryBuilders.existsQuery("results"), ScoreMode.None).ignoreUnmapped(true))
                .filter(
                    QueryBuilders.nestedQuery("results", QueryBuilders.existsQuery("results.pred_class"), ScoreMode.None)
                        .ignoreUnmapped(true))
                .filter(
                    QueryBuilders.nestedQuery("results", QueryBuilders.existsQuery("results.pred_prob"), ScoreMode.None)
                        .ignoreUnmapped(true))
                .filter(QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("field_A", "some-value"))
                    .filter(QueryBuilders.termQuery("field_B", "some-other-value")));

        Classification evaluation =
            new Classification(
                "act", "pred", "results", "results.pred_class", "results.pred_prob", Arrays.asList(new MulticlassConfusionMatrix()));

        SearchSourceBuilder searchSourceBuilder = evaluation.buildSearch(EVALUATION_PARAMETERS, userProvidedQuery);
        assertThat(searchSourceBuilder.query(), equalTo(expectedSearchQuery));
        assertThat(searchSourceBuilder.aggregations().count(), greaterThan(0));
    }

    public void testProcess_MultipleMetricsWithDifferentNumberOfSteps() {
        EvaluationMetric metric1 = new FakeClassificationMetric("fake_metric_1", 2);
        EvaluationMetric metric2 = new FakeClassificationMetric("fake_metric_2", 3);
        EvaluationMetric metric3 = new FakeClassificationMetric("fake_metric_3", 4);
        EvaluationMetric metric4 = new FakeClassificationMetric("fake_metric_4", 5);

        Classification evaluation = new Classification("act", "pred", null, null, null, Arrays.asList(metric1, metric2, metric3, metric4));
        assertThat(metric1.getResult(), isEmpty());
        assertThat(metric2.getResult(), isEmpty());
        assertThat(metric3.getResult(), isEmpty());
        assertThat(metric4.getResult(), isEmpty());
        assertThat(evaluation.hasAllResults(), is(false));

        evaluation.process(mockSearchResponseWithNonZeroTotalHits());
        assertThat(metric1.getResult(), isEmpty());
        assertThat(metric2.getResult(), isEmpty());
        assertThat(metric3.getResult(), isEmpty());
        assertThat(metric4.getResult(), isEmpty());
        assertThat(evaluation.hasAllResults(), is(false));

        evaluation.process(mockSearchResponseWithNonZeroTotalHits());
        assertThat(metric1.getResult(), isPresent());
        assertThat(metric2.getResult(), isEmpty());
        assertThat(metric3.getResult(), isEmpty());
        assertThat(metric4.getResult(), isEmpty());
        assertThat(evaluation.hasAllResults(), is(false));

        evaluation.process(mockSearchResponseWithNonZeroTotalHits());
        assertThat(metric1.getResult(), isPresent());
        assertThat(metric2.getResult(), isPresent());
        assertThat(metric3.getResult(), isEmpty());
        assertThat(metric4.getResult(), isEmpty());
        assertThat(evaluation.hasAllResults(), is(false));

        evaluation.process(mockSearchResponseWithNonZeroTotalHits());
        assertThat(metric1.getResult(), isPresent());
        assertThat(metric2.getResult(), isPresent());
        assertThat(metric3.getResult(), isPresent());
        assertThat(metric4.getResult(), isEmpty());
        assertThat(evaluation.hasAllResults(), is(false));

        evaluation.process(mockSearchResponseWithNonZeroTotalHits());
        assertThat(metric1.getResult(), isPresent());
        assertThat(metric2.getResult(), isPresent());
        assertThat(metric3.getResult(), isPresent());
        assertThat(metric4.getResult(), isPresent());
        assertThat(evaluation.hasAllResults(), is(true));

        evaluation.process(mockSearchResponseWithNonZeroTotalHits());
        assertThat(metric1.getResult(), isPresent());
        assertThat(metric2.getResult(), isPresent());
        assertThat(metric3.getResult(), isPresent());
        assertThat(metric4.getResult(), isPresent());
        assertThat(evaluation.hasAllResults(), is(true));
    }

    private static SearchResponse mockSearchResponseWithNonZeroTotalHits() {
        SearchResponse searchResponse = mock(SearchResponse.class);
        SearchHits hits = new SearchHits(SearchHits.EMPTY, new TotalHits(10, TotalHits.Relation.EQUAL_TO), 0);
        when(searchResponse.getHits()).thenReturn(hits);
        return searchResponse;
    }

    /**
     * Metric which iterates through its steps in {@link #process} method.
     * Number of steps is configurable.
     * Upon reaching the last step, the result is produced.
     */
    private static class FakeClassificationMetric implements EvaluationMetric {

        private final String name;
        private final int numSteps;
        private int currentStepIndex;
        private EvaluationMetricResult result;

        FakeClassificationMetric(String name, int numSteps) {
            this.name = name;
            this.numSteps = numSteps;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getWriteableName() {
            return name;
        }

        @Override
        public Set<String> getRequiredFields() {
            return Set.of(EvaluationFields.ACTUAL_FIELD.getPreferredName(), EvaluationFields.PREDICTED_FIELD.getPreferredName());
        }

        @Override
        public Tuple<List<AggregationBuilder>, List<PipelineAggregationBuilder>> aggs(EvaluationParameters parameters,
                                                                                      EvaluationFields fields) {
            return Tuple.tuple(List.of(), List.of());
        }

        @Override
        public void process(Aggregations aggs) {
            if (result != null) {
                return;
            }
            currentStepIndex++;
            if (currentStepIndex == numSteps) {
                // This is the last step, time to write evaluation result
                result = mock(EvaluationMetricResult.class);
            }
        }

        @Override
        public Optional<EvaluationMetricResult> getResult() {
            return Optional.ofNullable(result);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) {
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) {
        }
    }
}
