/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.test.rest.transform.match;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.elasticsearch.gradle.test.rest.transform.RestTestTransformByParentArray;

import java.util.Objects;

public class AddMatch implements RestTestTransformByParentArray {
    private static JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(false);
    private final String matchKey;
    private final String testName;
    private final JsonNode matchValue;

    public AddMatch(String matchKey, JsonNode matchValue, String testName) {
        this.matchKey = matchKey;
        this.matchValue = matchValue;
        this.testName = Objects.requireNonNull(testName, "adding matches is only supported for named tests");
    }

    @Override
    public String getTestName() {
        return testName;
    }

    @Override
    public void transformTest(ArrayNode matchParent) {
        ObjectNode matchObject = new ObjectNode(jsonNodeFactory);
        ObjectNode matchContent = new ObjectNode(jsonNodeFactory);
        matchContent.set(matchKey, matchValue);
        matchObject.set("match", matchContent);
        matchParent.add(matchObject);
    }

    @Override
    public String getKeyOfArrayToFind() {
        // match objects are always in the array that is the direct child of the test name, i.e.
        // "my test name" : [ {"do" : ... }, { "match" : .... }, {..}, {..}, ... ]
        return testName;
    }
}
