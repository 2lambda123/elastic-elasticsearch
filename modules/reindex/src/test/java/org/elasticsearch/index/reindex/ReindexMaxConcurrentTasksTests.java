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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class ReindexMaxConcurrentTasksTests extends ReindexTestCase {

    public void testMaxConcurrentTasks() throws Exception {
        List<IndexRequestBuilder> docs = new ArrayList<>();
        int max = between(150, 500);
        for (int i = 0; i < max; i++) {
            docs.add(client().prepareIndex("source").setId(Integer.toString(i)).setSource("foo", "a"));
        }

        indexRandom(true, docs);
        assertHitCount(client().prepareSearch("source").setSize(0).get(), max);

        try {
            ClusterUpdateSettingsRequest settingsUpdate = new ClusterUpdateSettingsRequest();
            Settings settings = Settings.builder().put(TransportStartReindexTaskAction.MAX_CONCURRENT_REINDEX_TASKS.getKey(), 3).build();
            settingsUpdate.persistentSettings(settings);
            client().admin().cluster().updateSettings(settingsUpdate).get();

            List<String> tasksToWaitOn = new ArrayList<>();

            // Since we are only using validation on the data nodes and this test is inherently racy, we
            //submit an excess of requests to ensure that the max concurrent exception triggers
            expectThrows(IllegalStateException.class, () -> {
                for (int i = 0; i < 10; ++i) {
                    // Copy all the docs
                    ReindexRequestBuilder copy = reindex().source("source").destination("dest_" + i).refresh(true);
                    StartReindexTaskAction.Request request = new StartReindexTaskAction.Request(copy.request(), false);
                    // Use a small batch size so we have to use more than one batch
                    copy.source().setSize(5);
                    tasksToWaitOn.add(client().execute(StartReindexTaskAction.INSTANCE, request).actionGet().getTaskId());
                }
            });

            for (String task : tasksToWaitOn) {
                client().admin().cluster().prepareGetTask(task).setWaitForCompletion(true).get();
            }
        } finally {
            ClusterUpdateSettingsRequest settingsUpdate = new ClusterUpdateSettingsRequest();
            Settings settings = Settings.builder()
                .put(TransportStartReindexTaskAction.MAX_CONCURRENT_REINDEX_TASKS.getKey(), (String) null)
                .build();
            settingsUpdate.persistentSettings(settings);
            client().admin().cluster().updateSettings(settingsUpdate).get();
        }
    }
}
