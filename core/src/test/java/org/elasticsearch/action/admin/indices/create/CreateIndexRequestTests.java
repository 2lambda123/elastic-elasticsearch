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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class CreateIndexRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("foo");
        String mapping = JsonXContent.contentBuilder().startObject().startObject("type").endObject().endObject().string();
        request.mapping("my_type", mapping, XContentType.JSON);

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            request.writeTo(output);

            try (StreamInput in = output.bytes().streamInput()) {
                CreateIndexRequest serialized = new CreateIndexRequest();
                serialized.readFrom(in);
                assertEquals(request.index(), serialized.index());
                assertEquals(mapping, serialized.mappings().get("my_type"));
            }
        }
    }
    
    public void testTopLevelKeys() throws IOException {
        String createIndex =
                "{\n"
                + "  \"FOO_SHOULD_BE_ILLEGAL_HERE\": {\n"
                + "    \"BAR_IS_THE_SAME\": 42\n"
                + "  },\n"
                + "  \"mappings\": {\n"
                + "    \"test\": {\n"
                + "      \"properties\": {\n"
                + "        \"field1\": {\n"
                + "          \"type\": \"text\"\n"
                + "       }\n"
                + "     }\n"
                + "    }\n"
                + "  }\n"
                + "}";

        CreateIndexRequest request = new CreateIndexRequest();
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, 
                () -> {request.source(createIndex, XContentType.JSON);});
        assertThat(e.toString(), containsString(
                "unknown key [FOO_SHOULD_BE_ILLEGAL_HERE] for a [START_OBJECT], "
                + "expected [settings], [mappings] or [aliases]"));
    }
}
