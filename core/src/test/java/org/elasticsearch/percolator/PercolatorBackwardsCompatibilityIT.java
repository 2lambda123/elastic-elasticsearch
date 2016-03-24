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
package org.elasticsearch.percolator;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.Version;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.percolate.PercolateSourceBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.env.Environment;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class PercolatorBackwardsCompatibilityIT extends ESIntegTestCase {

    private final static String INDEX_NAME = "percolator_index";

    public void testOldPercolatorIndex() throws Exception {
        setupNode();

        // verify cluster state:
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertThat(state.metaData().indices().size(), equalTo(1));
        assertThat(state.metaData().indices().get(INDEX_NAME), notNullValue());
        assertThat(state.metaData().indices().get(INDEX_NAME).getCreationVersion(), equalTo(Version.V_2_0_0));
        assertThat(state.metaData().indices().get(INDEX_NAME).getUpgradedVersion(), equalTo(Version.CURRENT));
        assertThat(state.metaData().indices().get(INDEX_NAME).getMappings().size(), equalTo(2));
        assertThat(state.metaData().indices().get(INDEX_NAME).getMappings().get(".percolator"), notNullValue());
        // important: verify that the query field in the .percolator mapping is of type object (from 3.0.0 this is of type percolator)
        MappingMetaData mappingMetaData = state.metaData().indices().get(INDEX_NAME).getMappings().get(".percolator");
        assertThat(XContentMapValues.extractValue("properties.query.type", mappingMetaData.sourceAsMap()), equalTo("object"));
        assertThat(state.metaData().indices().get(INDEX_NAME).getMappings().get("message"), notNullValue());

        // verify existing percolator queries:
        SearchResponse searchResponse = client().prepareSearch(INDEX_NAME)
            .setTypes(".percolator")
            .addSort("_uid", SortOrder.ASC)
            .get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("3"));

        // verify percolate response
        PercolateResponse percolateResponse = client().preparePercolate()
            .setIndices(INDEX_NAME)
            .setDocumentType("message")
            .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("message", "the quick brown fox jumps over the lazy dog"))
            .get();

        assertThat(percolateResponse.getCount(), equalTo(2L));
        assertThat(percolateResponse.getMatches().length, equalTo(2));
        assertThat(percolateResponse.getMatches()[0].getId().string(), equalTo("1"));
        assertThat(percolateResponse.getMatches()[1].getId().string(), equalTo("2"));

        // add an extra query and verify the results
        client().prepareIndex(INDEX_NAME, ".percolator", "4")
            .setSource(jsonBuilder().startObject().field("query", matchQuery("message", "fox jumps")).endObject())
            .get();
        refresh();

        percolateResponse = client().preparePercolate()
            .setIndices(INDEX_NAME)
            .setDocumentType("message")
            .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("message", "the quick brown fox jumps over the lazy dog"))
            .get();

        assertThat(percolateResponse.getCount(), equalTo(3L));
        assertThat(percolateResponse.getMatches().length, equalTo(3));
        assertThat(percolateResponse.getMatches()[0].getId().string(), equalTo("1"));
        assertThat(percolateResponse.getMatches()[1].getId().string(), equalTo("2"));
        assertThat(percolateResponse.getMatches()[2].getId().string(), equalTo("4"));
    }

    private void setupNode() throws Exception {
        Path dataDir = createTempDir();
        Path clusterDir = Files.createDirectory(dataDir.resolve(cluster().getClusterName()));
        try (InputStream stream = PercolatorBackwardsCompatibilityIT.class.getResourceAsStream("/indices/percolator/bwc_index_2.0.0.zip")) {
            TestUtil.unzip(stream, clusterDir);
        }

        Settings.Builder nodeSettings = Settings.builder()
            .put(Environment.PATH_DATA_SETTING.getKey(), dataDir);
        internalCluster().startNode(nodeSettings.build());
        ensureGreen(INDEX_NAME);
    }

}
