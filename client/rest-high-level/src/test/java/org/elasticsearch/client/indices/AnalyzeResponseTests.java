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

package org.elasticsearch.client.indices;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AnalyzeResponseTests extends AbstractResponseTestCase<AnalyzeAction.Response, AnalyzeResponse> {

    @Override
    protected AnalyzeAction.Response createServerTestInstance() {
        int tokenCount = randomIntBetween(1, 30);
        AnalyzeAction.AnalyzeToken[] tokens = new AnalyzeAction.AnalyzeToken[tokenCount];
        for (int i = 0; i < tokenCount; i++) {
            tokens[i] = randomToken();
        }
        if (randomBoolean()) {
            AnalyzeAction.CharFilteredText[] charfilters = null;
            AnalyzeAction.AnalyzeTokenList[] tokenfilters = null;
            if (randomBoolean()) {
                charfilters = new AnalyzeAction.CharFilteredText[]{
                    new AnalyzeAction.CharFilteredText("my_charfilter", new String[]{"one two"})
                };
            }
            if (randomBoolean()) {
                tokenfilters = new AnalyzeAction.AnalyzeTokenList[]{
                    new AnalyzeAction.AnalyzeTokenList("my_tokenfilter_1", tokens),
                    new AnalyzeAction.AnalyzeTokenList("my_tokenfilter_2", tokens)
                };
            }
            AnalyzeAction.DetailAnalyzeResponse dar = new AnalyzeAction.DetailAnalyzeResponse(
                charfilters,
                new AnalyzeAction.AnalyzeTokenList("my_tokenizer", tokens),
                tokenfilters);
            return new AnalyzeAction.Response(null, dar);
        }
        return new AnalyzeAction.Response(Arrays.asList(tokens), null);
    }

    private AnalyzeAction.AnalyzeToken randomToken() {
        String token = randomAlphaOfLengthBetween(1, 20);
        int position = randomIntBetween(0, 1000);
        int startOffset = randomIntBetween(0, 1000);
        int endOffset = randomIntBetween(0, 1000);
        int posLength = randomIntBetween(1, 5);
        String type = randomAlphaOfLengthBetween(1, 20);
        Map<String, Object> extras = new HashMap<>();
        if (randomBoolean()) {
            int entryCount = randomInt(6);
            for (int i = 0; i < entryCount; i++) {
                switch (randomInt(6)) {
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                        String key = randomAlphaOfLength(5);
                        String value = randomAlphaOfLength(10);
                        extras.put(key, value);
                        break;
                    case 4:
                        String objkey = randomAlphaOfLength(5);
                        Map<String, String> obj = new HashMap<>();
                        obj.put(randomAlphaOfLength(5), randomAlphaOfLength(10));
                        extras.put(objkey, obj);
                        break;
                    case 5:
                        String listkey = randomAlphaOfLength(5);
                        List<String> list = new ArrayList<>();
                        list.add(randomAlphaOfLength(4));
                        list.add(randomAlphaOfLength(6));
                        extras.put(listkey, list);
                        break;
                }
            }
        }
        return new AnalyzeAction.AnalyzeToken(token, position, startOffset, endOffset, posLength, type, extras);
    }

    @Override
    protected AnalyzeResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return AnalyzeResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(AnalyzeAction.Response serverTestInstance, AnalyzeResponse clientInstance) {
        if (serverTestInstance.detail() != null) {
            assertNotNull(clientInstance.detail());
            assertInstances(serverTestInstance.detail(), clientInstance.detail());
        }
        else {
            assertEquals(serverTestInstance.getTokens().size(), clientInstance.getTokens().size());
            for (int i = 0; i < serverTestInstance.getTokens().size(); i++) {
                assertEqualTokens(serverTestInstance.getTokens().get(0), clientInstance.getTokens().get(0));
            }
        }
    }

    private static void assertEqualTokens(AnalyzeAction.AnalyzeToken serverToken, AnalyzeResponse.AnalyzeToken clientToken) {
        assertEquals(serverToken.getTerm(), clientToken.getTerm());
        assertEquals(serverToken.getPosition(), clientToken.getPosition());
        assertEquals(serverToken.getPositionLength(), clientToken.getPositionLength());
        assertEquals(serverToken.getStartOffset(), clientToken.getStartOffset());
        assertEquals(serverToken.getEndOffset(), clientToken.getEndOffset());
        assertEquals(serverToken.getType(), clientToken.getType());
        assertEquals(serverToken.getAttributes(), clientToken.getAttributes());
    }

    private static void assertInstances(AnalyzeAction.DetailAnalyzeResponse serverResponse, DetailAnalyzeResponse clientResponse) {
        assertInstances(serverResponse.analyzer(), clientResponse.analyzer());
        assertInstances(serverResponse.tokenizer(), clientResponse.tokenizer());
        if (serverResponse.tokenfilters() == null) {
            assertNull(clientResponse.tokenfilters());
        }
        else {
            assertEquals(serverResponse.tokenfilters().length, clientResponse.tokenfilters().length);
            for (int i = 0; i < serverResponse.tokenfilters().length; i++) {
                assertInstances(serverResponse.tokenfilters()[i], clientResponse.tokenfilters()[i]);
            }
        }
        if (serverResponse.charfilters() == null) {
            assertNull(clientResponse.charfilters());
        }
        else {
            assertEquals(serverResponse.charfilters().length, clientResponse.charfilters().length);
            for (int i = 0; i < serverResponse.charfilters().length; i++) {
                assertInstances(serverResponse.charfilters()[i], clientResponse.charfilters()[i]);
            }
        }
    }

    private static void assertInstances(AnalyzeAction.AnalyzeTokenList serverTokens,
                                        DetailAnalyzeResponse.AnalyzeTokenList clientTokens) {
        if (serverTokens == null) {
            assertNull(clientTokens);
        }
        else {
            assertEquals(serverTokens.getName(), clientTokens.getName());
            assertEquals(serverTokens.getTokens().length, clientTokens.getTokens().length);
            for (int i = 0; i < serverTokens.getTokens().length; i++) {
                assertEqualTokens(serverTokens.getTokens()[i], clientTokens.getTokens()[i]);
            }
        }
    }

    private static void assertInstances(AnalyzeAction.CharFilteredText serverText, DetailAnalyzeResponse.CharFilteredText clientText) {
        assertEquals(serverText.getName(), clientText.getName());
        assertArrayEquals(serverText.getTexts(), clientText.getTexts());
    }
}
