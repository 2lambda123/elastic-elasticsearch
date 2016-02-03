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
package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngineRegistry;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class SumIT extends AbstractNumericTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ExtractFieldScriptPlugin.class, FieldValueScriptPlugin.class);
    }

    @Override
    public void testEmptyAggregation() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1L).minDocCount(0).subAggregation(sum("sum").field("value")))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2L));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, notNullValue());

        Sum sum = bucket.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo(0.0));
    }

    @Override
    public void testUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx_unmapped")
                .setQuery(matchAllQuery())
                .addAggregation(sum("sum").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0L));

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo(0.0));
    }

    @Override
    public void testSingleValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(sum("sum").field("value"))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 1+2+3+4+5+6+7+8+9+10));
    }

    public void testSingleValuedFieldWithFormatter() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(sum("sum").format("0000.0").field("value")).execute().actionGet();

        assertHitCount(searchResponse, 10);

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10));
        assertThat(sum.getValueAsString(), equalTo("0055.0"));
    }

    @Override
    public void testSingleValuedFieldGetProperty() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(global("global").subAggregation(sum("sum").field("value"))).execute().actionGet();

        assertHitCount(searchResponse, 10);

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo(10L));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        Sum sum = global.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        double expectedSumValue = (double) 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10;
        assertThat(sum.getValue(), equalTo(expectedSumValue));
        assertThat((Sum) global.getProperty("sum"), equalTo(sum));
        assertThat((double) global.getProperty("sum.value"), equalTo(expectedSumValue));
        assertThat((double) sum.getProperty("value"), equalTo(expectedSumValue));
    }

    @Override
    public void testSingleValuedFieldPartiallyUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx", "idx_unmapped")
                .setQuery(matchAllQuery())
                .addAggregation(sum("sum").field("value"))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 1+2+3+4+5+6+7+8+9+10));
    }

    @Override
    public void testSingleValuedFieldWithValueScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(sum("sum").field("value").script(new Script("", ScriptType.INLINE, FieldValueScriptEngine.NAME, null)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 1+2+3+4+5+6+7+8+9+10));
    }

    @Override
    public void testSingleValuedFieldWithValueScriptWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("increment", 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(sum("sum").field("value").script(new Script("", ScriptType.INLINE, FieldValueScriptEngine.NAME, params)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 1+2+3+4+5+6+7+8+9+10));
    }

    @Override
    public void testScriptSingleValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(sum("sum").script(new Script("value", ScriptType.INLINE, ExtractFieldScriptEngine.NAME, null)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 1+2+3+4+5+6+7+8+9+10));
    }

    @Override
    public void testScriptSingleValuedWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("inc", 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(sum("sum").script(new Script("value", ScriptType.INLINE, ExtractFieldScriptEngine.NAME, params)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 2+3+4+5+6+7+8+9+10+11));
    }

    @Override
    public void testScriptMultiValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(sum("sum").script(new Script("values", ScriptType.INLINE, ExtractFieldScriptEngine.NAME, null)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 2+3+3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11+11+12));
    }

    @Override
    public void testScriptMultiValuedWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("inc", 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        sum("sum").script(new Script("values", ScriptType.INLINE, ExtractFieldScriptEngine.NAME, params)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11+11+12+12+13));
    }

    @Override
    public void testMultiValuedField() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(sum("sum").field("values"))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 2+3+3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11+11+12));
    }

    @Override
    public void testMultiValuedFieldWithValueScript() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(sum("sum").field("values").script(new Script("", ScriptType.INLINE, FieldValueScriptEngine.NAME, null)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 2 + 3 + 3 + 4 + 4 + 5 + 5 + 6 + 6 + 7 + 7 + 8 + 8 + 9 + 9 + 10 + 10 + 11 + 11 + 12));
    }

    @Override
    public void testMultiValuedFieldWithValueScriptWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("increment", 1);
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(sum("sum").field("values").script(new Script("", ScriptType.INLINE, FieldValueScriptEngine.NAME, params)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Sum sum = searchResponse.getAggregations().get("sum");
        assertThat(sum, notNullValue());
        assertThat(sum.getName(), equalTo("sum"));
        assertThat(sum.getValue(), equalTo((double) 2 + 3 + 3 + 4 + 4 + 5 + 5 + 6 + 6 + 7 + 7 + 8 + 8 + 9 + 9 + 10 + 10 + 11 + 11 + 12));
    }

    /**
     * Mock plugin for the {@link ExtractFieldScriptEngine}
     */
    public static class ExtractFieldScriptPlugin extends Plugin {

        @Override
        public String name() {
            return ExtractFieldScriptEngine.NAME;
        }

        @Override
        public String description() {
            return "Mock script engine for " + SumIT.class;
        }

        public void onModule(ScriptModule module) {
            module.addScriptEngine(new ScriptEngineRegistry.ScriptEngineRegistration(ExtractFieldScriptEngine.class, ExtractFieldScriptEngine.TYPES));
        }

    }

    /**
     * This mock script returns the field that is specified by name in the
     * script body
     */
    public static class ExtractFieldScriptEngine implements ScriptEngineService {

        public static final String NAME = "extract_field";

        public static final List<String> TYPES = Collections.singletonList(NAME);

        @Override
        public void close() throws IOException {
        }

        @Override
        public List<String> getTypes() {
            return TYPES;
        }

        @Override
        public List<String> getExtensions() {
            return TYPES;
        }

        @Override
        public boolean isSandboxed() {
            return true;
        }

        @Override
        public Object compile(String script, Map<String, String> params) {
            return script;
        }

        @Override
        public ExecutableScript executable(CompiledScript compiledScript, Map<String, Object> params) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, Map<String, Object> vars) {
            final long inc;
            if (vars == null || vars.containsKey("inc") == false) {
                inc = 0;
            } else {
                inc = ((Number) vars.get("inc")).longValue();
            }
            return new SearchScript() {

                @Override
                public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {

                    final LeafSearchLookup leafLookup = lookup.getLeafSearchLookup(context);

                    return new LeafSearchScript() {
                        @Override
                        public void setNextVar(String name, Object value) {
                        }

                        @Override
                        public Object run() {
                            String fieldName = (String) compiledScript.compiled();
                            List<Long> values = new ArrayList<>();
                            for (Object v : (List<?>) leafLookup.doc().get(fieldName)) {
                                values.add(((Number) v).longValue() + inc);
                            }
                            return values;
                        }

                        @Override
                        public void setScorer(Scorer scorer) {
                        }

                        @Override
                        public void setSource(Map<String, Object> source) {
                        }

                        @Override
                        public void setDocument(int doc) {
                            if (leafLookup != null) {
                                leafLookup.setDocument(doc);
                            }
                        }

                        @Override
                        public long runAsLong() {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public float runAsFloat() {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public double runAsDouble() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }

                @Override
                public boolean needsScores() {
                    return false;
                }
            };
        }

        @Override
        public void scriptRemoved(CompiledScript script) {
        }
    }

    /**
     * Mock plugin for the {@link FieldValueScriptEngine}
     */
    public static class FieldValueScriptPlugin extends Plugin {

        @Override
        public String name() {
            return FieldValueScriptEngine.NAME;
        }

        @Override
        public String description() {
            return "Mock script engine for " + SumIT.class;
        }

        public void onModule(ScriptModule module) {
            module.addScriptEngine(new ScriptEngineRegistry.ScriptEngineRegistration(FieldValueScriptEngine.class, FieldValueScriptEngine.TYPES));
        }

    }

    /**
     * This mock script returns the field value and adds one to the returned
     * value
     */
    public static class FieldValueScriptEngine implements ScriptEngineService {

        public static final String NAME = "field_value";

        public static final List<String> TYPES = Collections.singletonList(NAME);

        @Override
        public void close() throws IOException {
        }

        @Override
        public List<String> getTypes() {
            return TYPES;
        }

        @Override
        public List<String> getExtensions() {
            return TYPES;
        }

        @Override
        public boolean isSandboxed() {
            return true;
        }

        @Override
        public Object compile(String script, Map<String, String> params) {
            return script;
        }

        @Override
        public ExecutableScript executable(CompiledScript compiledScript, Map<String, Object> params) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, Map<String, Object> vars) {
            final long inc;
            if (vars == null || vars.containsKey("inc") == false) {
                inc = 0;
            } else {
                inc = ((Number) vars.get("inc")).longValue();
            }
            return new SearchScript() {

                private Map<String, Object> vars = new HashMap<>(2);

                @Override
                public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {

                    final LeafSearchLookup leafLookup = lookup.getLeafSearchLookup(context);

                    return new LeafSearchScript() {

                        @Override
                        public Object unwrap(Object value) {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public void setNextVar(String name, Object value) {
                            vars.put(name, value);
                        }

                        @Override
                        public Object run() {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public void setScorer(Scorer scorer) {
                        }

                        @Override
                        public void setSource(Map<String, Object> source) {
                        }

                        @Override
                        public void setDocument(int doc) {
                            if (leafLookup != null) {
                                leafLookup.setDocument(doc);
                            }
                        }

                        @Override
                        public long runAsLong() {
                            return ((Number) vars.get("_value")).longValue() + inc;
                        }

                        @Override
                        public float runAsFloat() {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public double runAsDouble() {
                            return ((Number) vars.get("_value")).doubleValue() + inc;
                        }
                    };
                }

                @Override
                public boolean needsScores() {
                    return false;
                }
            };
        }

        @Override
        public void scriptRemoved(CompiledScript script) {
        }
    }
}
