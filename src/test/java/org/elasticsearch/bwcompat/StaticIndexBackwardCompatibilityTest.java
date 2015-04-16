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

package org.elasticsearch.bwcompat;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import java.nio.file.Paths;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * These tests are against static indexes, built from versions of ES that cannot be upgraded without
 * a full cluster restart (ie no wire format compatibility).
 */
@LuceneTestCase.SuppressCodecs({"Lucene3x", "MockFixedIntBlock", "MockVariableIntBlock", "MockSep", "MockRandom", "Lucene40", "Lucene41", "Appending", "Lucene42", "Lucene45", "Lucene46", "Lucene49"})
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0, minNumDataNodes = 0, maxNumDataNodes = 0)
public class StaticIndexBackwardCompatibilityTest extends ElasticsearchIntegrationTest {

    public void loadIndex(String index, Object... settings) throws Exception {
        logger.info("Checking static index " + index);
        Settings nodeSettings = prepareBackwardsDataDir(Paths.get(getClass().getResource(index + ".zip").toURI()), settings);
        internalCluster().startNode(nodeSettings);
        ensureGreen(index);
        assertIndexSanity(index);
    }

    private void assertIndexSanity(String index) {
        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().get();
        assertEquals(1, getIndexResponse.indices().length);
        assertEquals(index, getIndexResponse.indices()[0]);
        ensureYellow(index);
        SearchResponse test = client().prepareSearch(index).get();
        assertThat(test.getHits().getTotalHits(), greaterThanOrEqualTo(1l));
    }

}
