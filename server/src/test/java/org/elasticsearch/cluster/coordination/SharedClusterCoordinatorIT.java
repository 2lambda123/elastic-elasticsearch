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
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.discovery.TestZenDiscovery;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, minNumDataNodes = 2)
public class SharedClusterCoordinatorIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put(TestZenDiscovery.USE_ZEN2.getKey(), true)
            .build();
    }

    /** checks auto-provisioning of the Zen2 cluster */
    public void testSimpleClusterFormation() throws Exception {
        String[] indexNames = { "test1", "test2" };
        for (String indexName : indexNames) {
            assertAcked(client()
                .admin()
                .indices()
                .prepareCreate(indexName));

            indexRandom(true, client().prepareIndex(indexName, "type1", indexName + "1").setSource("field1", "value1"));
        }

        for (String indexName : indexNames) {
            SearchResponse request = client().prepareSearch(indexName).get();
            SearchResponse searchResponse = assertSearchResponse(request);
            assertHitCount(searchResponse, 1L);
            assertSearchHits(searchResponse, indexName + "1");
        }
    }
}
