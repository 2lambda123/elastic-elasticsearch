/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.test.rest.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

public class ReplaceKeyValueTests extends GradleUnitTestCase {

    private static final YAMLFactory YAML_FACTORY = new YAMLFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper(YAML_FACTORY);
    private static final ObjectReader READER = MAPPER.readerFor(ObjectNode.class);
    private static JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(false);



    private static final boolean humanDebug = true; // useful for humans trying to debug these tests

    /**
     * test file does not have setup: block
     */
    @Test
    public void testReplaceTypes() throws Exception {
        String testName = "/rest/replace_match/replace_types.yml";
        File testFile = new File(getClass().getResource(testName).toURI());
        YAMLParser yamlParser = YAML_FACTORY.createParser(testFile);
        List<ObjectNode> tests = READER.<ObjectNode>readValues(yamlParser).readAll();
        RestTestTransformer transformer = new RestTestTransformer();
        Pattern matchValue = Pattern.compile("\\{.*_type.*\\}");

        ObjectNode replacement = new ObjectNode(jsonNodeFactory);
        replacement.set("_type", TextNode.valueOf("_doc"));


        List<ObjectNode> transformedTests = transformer.transformRestTests(
            new LinkedList<>(tests),
            Collections.singletonList(new ReplaceKeyValue("match", matchValue, replacement))
        );
        printTest(testName, transformedTests);
//        // ensure setup is correct
//        assertThat(transformedTests.stream().filter(node -> node.get("setup") != null).count(), CoreMatchers.equalTo(1L));
//        transformedTests.stream().filter(node -> node.get("setup") != null).forEach(this::assertSetup);
//        transformedTests.stream().filter(node -> node.get("teardown") != null).forEach(this::assertTeardown);
//        // ensure do body is correct
//        transformedTests.forEach(test -> {
//            Iterator<Map.Entry<String, JsonNode>> testsIterator = test.fields();
//            while (testsIterator.hasNext()) {
//                Map.Entry<String, JsonNode> testObject = testsIterator.next();
//                assertThat(testObject.getValue(), CoreMatchers.instanceOf(ArrayNode.class));
//                ArrayNode testBody = (ArrayNode) testObject.getValue();
//                assertTestBodyForHeaders(testBody, headers);
//            }
//        });
    }



//    private void assertTestBodyForHeaders(ArrayNode testBody, Map<String, String> headers) {
//        testBody.forEach(arrayObject -> {
//            assertThat(arrayObject, CoreMatchers.instanceOf(ObjectNode.class));
//            ObjectNode testSection = (ObjectNode) arrayObject;
//            if (testSection.get("do") != null) {
//                ObjectNode doSection = (ObjectNode) testSection.get("do");
//                assertThat(doSection.get("headers"), CoreMatchers.notNullValue());
//                ObjectNode headersNode = (ObjectNode) doSection.get("headers");
//                LongAdder assertions = new LongAdder();
//                headers.forEach((k, v) -> {
//                    assertThat(headersNode.get(k), CoreMatchers.notNullValue());
//                    TextNode textNode = (TextNode) headersNode.get(k);
//                    assertThat(textNode.asText(), CoreMatchers.equalTo(v));
//                    assertions.increment();
//                });
//                assertThat(assertions.intValue(), CoreMatchers.equalTo(headers.size()));
//            }
//        });
//    }




    // only to help manually debug
    private void printTest(String testName, List<ObjectNode> tests) {
        if (humanDebug) {
            System.out.println("\n************* " + testName + " *************");
            try (SequenceWriter sequenceWriter = MAPPER.writer().writeValues(System.out)) {
                for (ObjectNode transformedTest : tests) {
                    sequenceWriter.write(transformedTest);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
