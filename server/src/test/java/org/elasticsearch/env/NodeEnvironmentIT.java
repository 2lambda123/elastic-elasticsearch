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

package org.elasticsearch.env;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.hamcrest.Matchers.containsString;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class NodeEnvironmentIT extends ESIntegTestCase {
    public void testStartFailureOnDataForNonDataNode() throws Exception {
        final String indexName = "test-fail-on-data";

        logger.info("--> starting one node");
        internalCluster().startNodes(1);

        logger.info("--> creating index");
        prepareCreate(indexName, Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
        ).get();
        final String indexUUID = resolveIndex(indexName).getUUID();

        logger.info("--> indexing a simple document");
        client().prepareIndex(indexName, "type1", "1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();

        logger.info("--> restarting the node with node.data=true");
        internalCluster().restartRandomDataNode();

        logger.info("--> restarting the node with node.data=false");
        IllegalStateException ex = expectThrows(IllegalStateException.class,
            "Node started with node.data=false and existing shard data must fail",
            () ->
                internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
                    @Override
                    public Settings onNodeStopped(String nodeName) {
                        return Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false).build();
                    }
                }));
        assertThat(ex.getMessage(), containsString(indexUUID));
    }
}
