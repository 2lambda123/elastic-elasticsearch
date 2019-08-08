/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.enrich.EnrichProcessorFactory.EnrichSpecification;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class EnrichProcessorFactoryTests extends ESTestCase {

    public void testCreateProcessorInstance() throws Exception {
        List<String> enrichValues = List.of("globalRank", "tldRank", "tld");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, List.of("source_index"), "my_key",
            enrichValues);
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null);
        factory.policies = Map.of("majestic", policy);

        Map<String, Object> config = new HashMap<>();
        config.put("policy_name", "majestic");
        config.put("enrich_key", "host");
        boolean keyIgnoreMissing = randomBoolean();
        if (keyIgnoreMissing || randomBoolean()) {
            config.put("ignore_missing", keyIgnoreMissing);
        }

        Boolean overrideEnabled = randomBoolean() ? null : randomBoolean();
        if (overrideEnabled != null) {
            config.put("override", overrideEnabled);
        }

        int numRandomValues = randomIntBetween(1, 8);
        List<Tuple<String, String>> randomValues = new ArrayList<>(numRandomValues);
        for (int i = 0; i < numRandomValues; i++) {
            randomValues.add(new Tuple<>(randomFrom(enrichValues), randomAlphaOfLength(4)));
        }

        List<Map<String, Object>> valuesConfig = new ArrayList<>(numRandomValues);
        for (Tuple<String, String> tuple : randomValues) {
            valuesConfig.add(Map.of("source", tuple.v1(), "target", tuple.v2()));
        }
        config.put("enrich_values", valuesConfig);

        ExactMatchProcessor result = (ExactMatchProcessor) factory.create(Collections.emptyMap(), "_tag", config);
        assertThat(result, notNullValue());
        assertThat(result.getPolicyName(), equalTo("majestic"));
        assertThat(result.getEnrichKey(), equalTo("host"));
        assertThat(result.isIgnoreMissing(), is(keyIgnoreMissing));
        if (overrideEnabled != null) {
            assertThat(result.isOverrideEnabled(), is(overrideEnabled));
        } else {
            assertThat(result.isOverrideEnabled(), is(true));
        }
        assertThat(result.getSpecifications().size(), equalTo(numRandomValues));
        for (int i = 0; i < numRandomValues; i++) {
            EnrichSpecification actual = result.getSpecifications().get(i);
            Tuple<String, String> expected = randomValues.get(i);
            assertThat(actual.sourceField, equalTo(expected.v1()));
            assertThat(actual.targetField, equalTo(expected.v2()));
        }
    }

    public void testPolicyDoesNotExist() {
        List<String> enrichValues = List.of("globalRank", "tldRank", "tld");
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null);

        Map<String, Object> config = new HashMap<>();
        config.put("policy_name", "majestic");
        config.put("enrich_key", "host");
        boolean keyIgnoreMissing = randomBoolean();
        if (keyIgnoreMissing || randomBoolean()) {
            config.put("ignore_missing", keyIgnoreMissing);
        }

        int numRandomValues = randomIntBetween(1, 8);
        List<Tuple<String, String>> randomValues = new ArrayList<>(numRandomValues);
        for (int i = 0; i < numRandomValues; i++) {
            randomValues.add(new Tuple<>(randomFrom(enrichValues), randomAlphaOfLength(4)));
        }

        List<Map<String, Object>> valuesConfig = new ArrayList<>(numRandomValues);
        for (Tuple<String, String> tuple : randomValues) {
            valuesConfig.add(Map.of("source", tuple.v1(), "target", tuple.v2()));
        }
        config.put("enrich_values", valuesConfig);

        Exception e = expectThrows(IllegalArgumentException.class, () -> factory.create(Collections.emptyMap(), "_tag", config));
        assertThat(e.getMessage(), equalTo("policy [majestic] does not exists"));
    }

    public void testPolicyNameMissing() {
        List<String> enrichValues = List.of("globalRank", "tldRank", "tld");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, List.of("source_index"), "my_key",
            enrichValues);
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null);
        factory.policies = Map.of("_name", policy);

        Map<String, Object> config = new HashMap<>();
        config.put("enrich_key", "host");
        boolean keyIgnoreMissing = randomBoolean();
        if (keyIgnoreMissing || randomBoolean()) {
            config.put("ignore_missing", keyIgnoreMissing);
        }

        int numRandomValues = randomIntBetween(1, 8);
        List<Tuple<String, String>> randomValues = new ArrayList<>(numRandomValues);
        for (int i = 0; i < numRandomValues; i++) {
            randomValues.add(new Tuple<>(randomFrom(enrichValues), randomAlphaOfLength(4)));
        }

        List<Map<String, Object>> valuesConfig = new ArrayList<>(numRandomValues);
        for (Tuple<String, String> tuple : randomValues) {
            valuesConfig.add(Map.of("source", tuple.v1(), "target", tuple.v2()));
        }
        config.put("enrich_values", valuesConfig);

        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(Collections.emptyMap(), "_tag", config));
        assertThat(e.getMessage(), equalTo("[policy_name] required property is missing"));
    }

    public void testUnsupportedPolicy() {
        List<String> enrichValues = List.of("globalRank", "tldRank", "tld");
        EnrichPolicy policy = new EnrichPolicy("unsupported", null, List.of("source_index"), "my_key", enrichValues);
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null);
        factory.policies = Map.of("majestic", policy);

        Map<String, Object> config = new HashMap<>();
        config.put("policy_name", "majestic");
        config.put("enrich_key", "host");
        boolean keyIgnoreMissing = randomBoolean();
        if (keyIgnoreMissing || randomBoolean()) {
            config.put("ignore_missing", keyIgnoreMissing);
        }

        int numRandomValues = randomIntBetween(1, 8);
        List<Tuple<String, String>> randomValues = new ArrayList<>(numRandomValues);
        for (int i = 0; i < numRandomValues; i++) {
            randomValues.add(new Tuple<>(randomFrom(enrichValues), randomAlphaOfLength(4)));
        }

        List<Map<String, Object>> valuesConfig = new ArrayList<>(numRandomValues);
        for (Tuple<String, String> tuple : randomValues) {
            valuesConfig.add(Map.of("source", tuple.v1(), "target", tuple.v2()));
        }
        config.put("enrich_values", valuesConfig);

        Exception e = expectThrows(IllegalArgumentException.class, () -> factory.create(Collections.emptyMap(), "_tag", config));
        assertThat(e.getMessage(), equalTo("unsupported policy type [unsupported]"));
    }

    public void testNonExistingDecorateField() {
        List<String> enrichValues = List.of("globalRank", "tldRank", "tld");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, List.of("source_index"), "my_key",
            enrichValues);
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null);
        factory.policies = Map.of("majestic", policy);

        Map<String, Object> config = new HashMap<>();
        config.put("policy_name", "majestic");
        config.put("enrich_key", "host");
        List<Map<String, Object>> valuesConfig = List.of(Map.of("source", "rank", "target", "rank"));
        config.put("enrich_values", valuesConfig);

        Exception e = expectThrows(IllegalArgumentException.class, () -> factory.create(Collections.emptyMap(), "_tag", config));
        assertThat(e.getMessage(), equalTo("source field [rank] does not exist in policy [majestic]"));
    }

    public void testCompactEnrichValuesFormat() throws Exception {
        List<String> enrichValues = List.of("globalRank", "tldRank", "tld");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, List.of("source_index"), "host",
            enrichValues);
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null);
        factory.policies = Map.of("majestic", policy);

        Map<String, Object> config = new HashMap<>();
        config.put("policy_name", "majestic");
        config.put("enrich_values", enrichValues);

        ExactMatchProcessor result = (ExactMatchProcessor) factory.create(Collections.emptyMap(), "_tag", config);
        assertThat(result, notNullValue());
        assertThat(result.getPolicyName(), equalTo("majestic"));
        assertThat(result.getEnrichKey(), equalTo("host"));
        assertThat(result.getSpecifications().size(), equalTo(enrichValues.size()));
        for (int i = 0; i < enrichValues.size(); i++) {
            EnrichSpecification actual = result.getSpecifications().get(i);
            String expected = enrichValues.get(i);
            assertThat(actual.sourceField, equalTo(expected));
            assertThat(actual.targetField, equalTo(expected));
        }
    }

    public void testNoEnrichValues() throws Exception {
        List<String> enrichValues = List.of("globalRank", "tldRank", "tld");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, List.of("source_index"), "host",
            enrichValues);
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null);
        factory.policies = Map.of("majestic", policy);

        Map<String, Object> config = new HashMap<>();
        config.put("policy_name", "majestic");
        config.put("enrich_values", List.of());

        Exception e = expectThrows(IllegalArgumentException.class, () -> factory.create(Collections.emptyMap(), "_tag", config));
        assertThat(e.getMessage(), equalTo("provided enrich_values is empty"));
    }

    public void testIllegalEnrichValues() throws Exception {
        List<String> enrichValues = List.of("globalRank", "tldRank", "tld");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, List.of("source_index"), "host",
            enrichValues);
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null);
        factory.policies = Map.of("majestic", policy);

        Map<String, Object> config1 = new HashMap<>();
        config1.put("policy_name", "majestic");
        config1.put("enrich_values", List.of(1L));

        Exception e = expectThrows(IllegalArgumentException.class, () -> factory.create(Collections.emptyMap(), "_tag", config1));
        assertThat(e.getMessage(), equalTo("unsupported enrich values type [class java.lang.Long]"));

        Map<String, Object> config2 = new HashMap<>();
        config2.put("policy_name", "majestic");
        config2.put("enrich_values", List.of("tld", true));
        e = expectThrows(IllegalArgumentException.class, () -> factory.create(Collections.emptyMap(), "_tag", config2));
        assertThat(e.getMessage(), equalTo("unsupported enrich values type [class java.lang.Boolean]"));
    }

}
