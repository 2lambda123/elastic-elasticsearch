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

package org.elasticsearch.client;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasIndex;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasProperty;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasType;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class BulkRequestWithGlobalParametersIT extends ESRestHighLevelClientTestCase {

    public void testGlobalPipelineOnBulkRequest() throws IOException {
        createFieldAddingPipleine("xyz", "fieldNameXYZ", "valueXYZ");

        BulkRequest request = new BulkRequest();
        request.pipeline("xyz");
        request.add(new IndexRequest("test", "doc", "1")
            .source(XContentType.JSON, "field", "bulk1"));
        request.add(new IndexRequest("test", "doc", "2")
            .source(XContentType.JSON, "field", "bulk2"));

        bulk(request);

        Iterable<SearchHit> hits = searchAll("test");
        assertThat(hits, containsInAnyOrder(hasId("1"), hasId("2")));
        assertThat(hits, everyItem(hasProperty(fieldFromSource("fieldNameXYZ"), equalTo("valueXYZ"))));
    }

    public void testPipelineOnRequestOverridesGlobalPipeline() throws IOException {
        createFieldAddingPipleine("globalId", "fieldXYZ", "valueXYZ");
        createFieldAddingPipleine("perIndexId", "someNewField", "someValue");

        BulkRequest request = new BulkRequest();
        request.pipeline("globalId");
        request.add(new IndexRequest("test", "doc", "1")
            .source(XContentType.JSON, "field", "bulk1")
            .setPipeline("perIndexId"));
        request.add(new IndexRequest("test", "doc", "2")
            .source(XContentType.JSON, "field", "bulk2")
            .setPipeline("perIndexId"));

        bulk(request);

        Iterable<SearchHit> hits = searchAll("test");
        assertThat(hits, everyItem(hasProperty(fieldFromSource("someNewField"), equalTo("someValue"))));
        // global pipeline was not applied
        assertThat(hits, everyItem(hasProperty(fieldFromSource("fieldXYZ"), nullValue())));
    }

    public void testMixPipelineOnRequestAndGlobal() throws IOException {
        createFieldAddingPipleine("globalId", "fieldXYZ", "valueXYZ");
        createFieldAddingPipleine("perIndexId", "someNewField", "someValue");

        BulkRequest request = new BulkRequest();
        request.pipeline("globalId");

        request.add(new IndexRequest("test", "doc", "1")
            .source(XContentType.JSON, "field", "bulk1")
            .setPipeline("perIndexId"));

        request.add(new IndexRequest("test", "doc", "2")
            .source(XContentType.JSON, "field", "bulk2"));

        bulk(request);

        Iterable<SearchHit> hits = searchAll("test");
        assertThat(hits, containsInAnyOrder(
            both(hasId("1"))
                .and(hasProperty(fieldFromSource("someNewField"), equalTo("someValue"))),
            both(hasId("2"))
                .and(hasProperty(fieldFromSource("fieldXYZ"), equalTo("valueXYZ")))));
    }

    public void testGlobalIndex() throws IOException {
        BulkRequest request = new BulkRequest("global_index", null);
        request.add(new IndexRequest().type("doc").id("1")
            .source(XContentType.JSON, "field", "bulk1"));
        request.add(new IndexRequest().type("doc").id("2")
            .source(XContentType.JSON, "field", "bulk2"));

        bulk(request);

        Iterable<SearchHit> hits = searchAll("global_index");
        assertThat(hits, everyItem(hasIndex("global_index")));
    }

    public void testIndexGlobalAndPerRequest() throws IOException {
        BulkRequest request = new BulkRequest("global_index", null);
        request.add(new IndexRequest("local_index", "doc", "1")
            .source(XContentType.JSON, "field", "bulk1"));
        request.add(new IndexRequest().type("doc").id("2") // will take global index
            .source(XContentType.JSON, "field", "bulk2"));

        bulk(request);

        Iterable<SearchHit> hits = searchAll("local_index", "global_index");
        assertThat(hits, containsInAnyOrder(
            both(hasId("1"))
                .and(hasIndex("local_index")),
            both(hasId("2"))
                .and(hasIndex("global_index"))));
    }

    public void testGlobalType() throws IOException {
        BulkRequest request = new BulkRequest(null, "global_type");
        request.add(new IndexRequest("index").id("1")
            .source(XContentType.JSON, "field", "bulk1"));
        request.add(new IndexRequest("index").id("2")
            .source(XContentType.JSON, "field", "bulk2"));

        bulk(request);

        Iterable<SearchHit> hits = searchAll("index");
        assertThat(hits, everyItem(hasType("global_type")));
    }

    public void testTypeGlobalAndPerRequest() throws IOException {
        BulkRequest request = new BulkRequest(null, "global_type");
        request.add(new IndexRequest("index1", "local_type", "1")
            .source(XContentType.JSON, "field", "bulk1"));
        request.add(new IndexRequest("index2").id("2") // will take global type
            .source(XContentType.JSON, "field", "bulk2"));

        bulk(request);

        Iterable<SearchHit> hits = searchAll("index1", "index2");
        assertThat(hits, containsInAnyOrder(
            both(hasId("1"))
                .and(hasType("local_type")),
            both(hasId("2"))
                .and(hasType("global_type"))));
    }

    public void testGlobalRouting() throws IOException {
        createIndexWithTwoShards();

        BulkRequest request = new BulkRequest(null, null);
        request.routing("routing");
        request.add(new IndexRequest("index", "type", "1")
            .source(XContentType.JSON, "field", "bulk1"));
        request.add(new IndexRequest("index", "type", "2")
            .source(XContentType.JSON, "field", "bulk1"));

        bulk(request);
        
        Iterable<SearchHit> emptyHits = searchAll(new SearchRequest("index").routing("wrongRouting"));
        assertThat(emptyHits, is(emptyIterable()));

        Iterable<SearchHit> hits = searchAll(new SearchRequest("index").routing("routing"));
        assertThat(hits, containsInAnyOrder(hasId("1"), hasId("2")));
    }

    public void testMixLocalAndGlobalRouting() throws IOException {
        BulkRequest request = new BulkRequest(null, null);
        request.routing("globalRouting");
        request.add(new IndexRequest("index", "type", "1")
            .source(XContentType.JSON, "field", "bulk1"));
        request.add(new IndexRequest("index", "type", "2")
            .routing("localRouting")
            .source(XContentType.JSON, "field", "bulk1"));

        bulk(request);

        Iterable<SearchHit> hits = searchAll(new SearchRequest("index").routing("globalRouting", "localRouting"));
        assertThat(hits, containsInAnyOrder(hasId("1"), hasId("2")));
    }

    private BulkResponse bulk(BulkRequest request) throws IOException {
        BulkResponse bulkResponse = execute(request, highLevelClient()::bulk, highLevelClient()::bulkAsync);
        assertFalse(bulkResponse.hasFailures());
        return bulkResponse;
    }

    @SuppressWarnings("unchecked")
    private static <T> Function<SearchHit, T> fieldFromSource(String fieldName) {
        return (response) -> (T) response.getSourceAsMap().get(fieldName);
    }

    private void createIndexWithTwoShards() throws IOException {
        CreateIndexRequest indexRequest = new CreateIndexRequest("index");
        indexRequest.settings(Settings.builder()
            .put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 1)
        );
        highLevelClient().indices().create(indexRequest, RequestOptions.DEFAULT);
    }
}
