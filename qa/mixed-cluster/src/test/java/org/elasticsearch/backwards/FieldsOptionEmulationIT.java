/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.backwards;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.backwards.IndexingIT.Node;
import org.elasticsearch.backwards.IndexingIT.Nodes;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * In mixed cluster scenarios on 7.x we try to emulate the "fields" option introduced in 7.10
 * by running a request with "source" enabled for the requested patterns on older nodes and convert
 * the resulting source entries back into the "fields" section. These tests check this in mixed cluster
 * scenarios.
 */
public class FieldsOptionEmulationIT extends ESRestTestCase {

    private static String index = "test_field_newversion";
    private static String index_old = "test_field_oldversion";
    private static Nodes nodes;
    private static List<Node> bwcNodes;
    private static List<Node> newNodes;
    private static String oldNodeName;
    private static String newNodeName;

    @Before
    public void prepareTestData() throws IOException {
        nodes = IndexingIT.buildNodeAndVersions(client());
        bwcNodes = new ArrayList<>(nodes.getBWCNodes());
        newNodes = new ArrayList<>(nodes.getNewNodes());
        oldNodeName = bwcNodes.get(0).getNodeName();
        newNodeName = newNodes.get(0).getNodeName();
        createIndexOnNode(index, newNodeName);
        createIndexOnNode(index_old, oldNodeName);
        refreshAllIndices();
    }

    private void createIndexOnNode(String indexName, String nodeName) throws IOException {
        if (indexExists(indexName) == false) {
            createIndex(indexName, Settings.builder()
                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", nodeName).build());
            for (int i = 0; i < 5; i++) {
                Request request = new Request("PUT", indexName + "/_doc/" + i);
                request.setJsonEntity(
                    "{\"test\": \"test_" + randomAlphaOfLength(2) + "\"," + "\"obj\" : { \"foo\" : \"value_" + i + "\"} }"
                );
                assertOK(client().performRequest(request));
            }
            ensureGreen(indexName);
            flush(indexName, true);
        }
    }

    @SuppressWarnings("unchecked")
    public void testFieldOptionAdapter() throws Exception {
        Request matchAllRequest = new Request("POST",
            "test_field_*/_search");
        matchAllRequest.setJsonEntity("{\"_source\":false,\"fields\":[\"*\"]}");
        try (
            RestClient client = buildClient(restClientSettings(), newNodes.stream().map(Node::getPublishAddress).toArray(HttpHost[]::new))
        ) {
            Response response = client.performRequest(matchAllRequest);
            ObjectPath responseObject = ObjectPath.createFromResponse(response);
            System.out.println(Strings.toString(responseObject.toXContentBuilder(XContentType.JSON.xContent())));
            List<Map<String, Object>> hits = responseObject.evaluate("hits.hits");
            assertEquals(10, hits.size());
            for (Map<String, Object> hit : hits) {
                Map<String, Object> fieldsMap = (Map<String, Object>) hit.get("fields");
                assertNotNull(fieldsMap);
                assertNotNull(fieldsMap.get("test"));
                assertTrue(((List<?>) fieldsMap.get("test")).get(0).toString().startsWith("test_"));
                assertNotNull(fieldsMap.get("obj.foo"));
                assertTrue(((List<?>) fieldsMap.get("obj.foo")).get(0).toString().startsWith("value_"));
                if (bwcNodes.get(0).getVersion().onOrAfter(Version.V_7_10_0)) {
                    // if all nodes are > 7.10 we should get full "fields" output even for subfields
                    assertTrue(((List<?>) fieldsMap.get("test.keyword")).get(0).toString().startsWith("test_"));
                }
            }
        }
    }
}
