/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.test.mapped;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.test.rest.yaml.section.ApiCallSection;
import org.elasticsearch.xpack.runtimefields.test.CoreTestTranslater;

import java.util.HashMap;
import java.util.Map;

/**
 * Runs elasticsearch's core rest tests replacing all field mappings with runtime fields
 * that load from {@code _source}. Tests that configure the field in a way that are not
 * supported by runtime fields are skipped.
 */
public class CoreWithMappedRuntimeFieldsIT extends ESClientYamlSuiteTestCase {
    public CoreWithMappedRuntimeFieldsIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return new MappingRuntimeFieldTranslater().parameters();
    }

    private static class MappingRuntimeFieldTranslater extends CoreTestTranslater {
        @Override
        protected Map<String, Object> indexTemplate() {
            return indexTemplateToAddRuntimeFieldsToMappings();
        }

        @Override
        protected Suite suite(ClientYamlTestCandidate candidate) {
            return new Suite(candidate) {
                @Override
                protected boolean modifyMapping(String index, Map<String, Object> mapping) {
                    Object properties = mapping.get("properties");
                    if (properties == null || false == (properties instanceof Map)) {
                        return true;
                    }
                    @SuppressWarnings("unchecked")
                    Map<String, Object> propertiesMap = (Map<String, Object>) properties;
                    Map<String, Object> newProperties = new HashMap<>(propertiesMap.size());
                    Map<String, Map<String, Object>> runtimeProperties = new HashMap<>(propertiesMap.size());
                    if (false == runtimeifyMappingProperties(propertiesMap, newProperties, runtimeProperties)) {
                        return false;
                    }
                    for (Map.Entry<String, Map<String, Object>> runtimeProperty : runtimeProperties.entrySet()) {
                        runtimeProperty.getValue().put("runtime_type", runtimeProperty.getValue().get("type"));
                        runtimeProperty.getValue().put("type", "runtime");
                        newProperties.put(runtimeProperty.getKey(), runtimeProperty.getValue());
                    }
                    mapping.put("properties", newProperties);
                    return true;
                }

                @Override
                protected boolean modifySearch(ApiCallSection search) {
                    // We don't need to modify the search request if the mappings are in the index
                    return true;
                }
            };
        }

    }
}
