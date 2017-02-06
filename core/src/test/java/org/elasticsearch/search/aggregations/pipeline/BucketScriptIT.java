/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateRange;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders.bucketScript;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class BucketScriptIT extends ESIntegTestCase {

    private static final String FIELD_1_NAME = "field1";
    private static final String FIELD_2_NAME = "field2";
    private static final String FIELD_3_NAME = "field3";
    private static final String FIELD_4_NAME = "field4";
    private static final String FIELD_5_NAME = "field5";

    private static int interval;
    private static int numDocs;
    private static int minNumber;
    private static int maxNumber;
    private static long date;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("_value0 + _value1 + _value2", vars -> {
                double value0 = (double) vars.get("_value0");
                double value1 = (double) vars.get("_value1");
                double value2 = (double) vars.get("_value2");
                return value0 + value1 + value2;
            });

            scripts.put("_value0 + _value1 / _value2", vars -> {
                double value0 = (double) vars.get("_value0");
                double value1 = (double) vars.get("_value1");
                double value2 = (double) vars.get("_value2");
                return value0 + value1 / value2;
            });

            scripts.put("_value0", vars -> vars.get("_value0"));

            scripts.put("foo + bar + baz", vars -> {
                double foo = (double) vars.get("foo");
                double bar = (double) vars.get("bar");
                double baz = (double) vars.get("baz");
                return foo + bar + baz;
            });

            scripts.put("(_value0 + _value1 + _value2) * factor", vars -> {
                double value0 = (double) vars.get("_value0");
                double value1 = (double) vars.get("_value1");
                double value2 = (double) vars.get("_value2");
                return (value0 + value1 + value2) * (int) vars.get("factor");
            });

            scripts.put("my_script", vars -> {
                double value0 = (double) vars.get("_value0");
                double value1 = (double) vars.get("_value1");
                double value2 = (double) vars.get("_value2");
                return value0 + value1 + value2;
            });

            return scripts;
        }
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");

        interval = randomIntBetween(1, 50);
        numDocs = randomIntBetween(10, 500);
        minNumber = -200;
        maxNumber = 200;
        date = randomLong();

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int docs = 0; docs < numDocs; docs++) {
            builders.add(client().prepareIndex("idx", "type").setSource(newDocBuilder()));
        }

        indexRandom(true, builders);
        ensureSearchable();
    }

    private XContentBuilder newDocBuilder() throws IOException {
        XContentBuilder jsonBuilder = jsonBuilder();
        jsonBuilder.startObject();
        jsonBuilder.field(FIELD_1_NAME, randomIntBetween(minNumber, maxNumber));
        jsonBuilder.field(FIELD_2_NAME, randomIntBetween(minNumber, maxNumber));
        jsonBuilder.field(FIELD_3_NAME, randomIntBetween(minNumber, maxNumber));
        jsonBuilder.field(FIELD_4_NAME, randomIntBetween(minNumber, maxNumber));
        jsonBuilder.field(FIELD_5_NAME, date);
        jsonBuilder.endObject();
        return jsonBuilder;
    }

    public void testInlineScript() {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                                .subAggregation(
                                        bucketScript("seriesArithmetic",
                                                new Script(ScriptType.INLINE,
                                                    CustomScriptPlugin.NAME, "_value0 + _value1 + _value2", Collections.emptyMap()),
                                            "field2Sum", "field3Sum", "field4Sum")))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
            }
        }
    }

    public void testInlineScript2() {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                                .subAggregation(
                                        bucketScript("seriesArithmetic",
                                                new Script(ScriptType.INLINE,
                                                    CustomScriptPlugin.NAME, "_value0 + _value1 / _value2", Collections.emptyMap()),
                                                "field2Sum", "field3Sum", "field4Sum")))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue / field4SumValue));
            }
        }
    }

    public void testInlineScriptWithDateRange() {
        SearchResponse response = client()
            .prepareSearch("idx")
            .addAggregation(
                dateRange("range")
                    .field(FIELD_5_NAME)
                    .addUnboundedFrom(date)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(
                        bucketScript("seriesArithmetic",
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value0 + _value1 + _value2", Collections.emptyMap())
                            , "field2Sum", "field3Sum", "field4Sum")))
            .execute().actionGet();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Range.Bucket> buckets = range.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Range.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
            }
        }
    }

    public void testInlineScriptSingleVariable() {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(
                                        bucketScript("seriesArithmetic",
                                                new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value0", Collections.emptyMap()),
                                                "field2Sum")))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue));
            }
        }
    }

    public void testInlineScriptNamedVars() {
        Map<String, String> bucketsPathsMap = new HashMap<>();
        bucketsPathsMap.put("foo", "field2Sum");
        bucketsPathsMap.put("bar", "field3Sum");
        bucketsPathsMap.put("baz", "field4Sum");
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                                .subAggregation(
                                        bucketScript("seriesArithmetic", bucketsPathsMap,
                                                new Script(ScriptType.INLINE,
                                                    CustomScriptPlugin.NAME, "foo + bar + baz", Collections.emptyMap()))))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
            }
        }
    }

    public void testInlineScriptWithParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("factor", 3);

        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "(_value0 + _value1 + _value2) * factor", params);

        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                                .subAggregation(bucketScript("seriesArithmetic", script, "field2Sum", "field3Sum", "field4Sum")))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo((field2SumValue + field3SumValue + field4SumValue) * 3));
            }
        }
    }

    public void testInlineScriptInsertZeros() {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                                .subAggregation(
                                        bucketScript("seriesArithmetic",
                                                new Script(ScriptType.INLINE,
                                                    CustomScriptPlugin.NAME, "_value0 + _value1 + _value2", Collections.emptyMap()),
                                                "field2Sum", "field3Sum", "field4Sum").gapPolicy(GapPolicy.INSERT_ZEROS)))
                .execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(0.0));
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
            }
        }
    }

    public void testStoredScript() {
        assertAcked(client().admin().cluster().preparePutStoredScript()
                .setId("my_script")
                .setLang(CustomScriptPlugin.NAME)
                // Script source is not interpreted but it references a pre-defined script from CustomScriptPlugin
                .setContent(new BytesArray("{ \"script\": \"my_script\" }"), XContentType.JSON));

        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                                .subAggregation(
                                        bucketScript("seriesArithmetic",
                                                new Script(ScriptType.STORED, CustomScriptPlugin.NAME, "my_script", Collections.emptyMap()),
                                                "field2Sum", "field3Sum", "field4Sum"))).execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
            }
        }
    }

    public void testUnmapped() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx_unmapped")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                                .subAggregation(
                                        bucketScript("seriesArithmetic",
                                                new Script(ScriptType.INLINE,
                                                    CustomScriptPlugin.NAME, "_value0 + _value1 + _value2", Collections.emptyMap()),
                                                "field2Sum", "field3Sum", "field4Sum")))
                                .execute().actionGet();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("histo");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        assertThat(deriv.getBuckets().size(), equalTo(0));
    }

    public void testPartiallyUnmapped() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx", "idx_unmapped")
                .addAggregation(
                        histogram("histo")
                                .field(FIELD_1_NAME)
                                .interval(interval)
                                .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                                .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                                .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                                .subAggregation(
                                        bucketScript("seriesArithmetic",
                                                new Script(ScriptType.INLINE,
                                                    CustomScriptPlugin.NAME, "_value0 + _value1 + _value2", Collections.emptyMap()),
                                                "field2Sum", "field3Sum", "field4Sum"))).execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
            }
        }
    }
}
