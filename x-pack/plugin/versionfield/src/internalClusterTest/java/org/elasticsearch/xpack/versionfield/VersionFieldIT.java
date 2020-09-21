/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.versionfield;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class VersionFieldIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return org.elasticsearch.common.collect.List.of(VersionFieldPlugin.class, LocalStateCompositeXPackPlugin.class);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/62705")
    public void testTermsAggregation() throws Exception {
        String indexName = "test";
        createIndex(indexName);

        client().admin()
            .indices()
            .preparePutMapping(indexName)
            .setType("_doc")
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("version")
                    .field("type", "version")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();
        ensureGreen();

        client().prepareIndex(indexName, "_doc")
            .setId("1")
            .setSource(jsonBuilder().startObject().field("version", "1.0").endObject())
            .get();
        client().prepareIndex(indexName, "_doc")
            .setId("2")
            .setSource(jsonBuilder().startObject().field("version", "1.3.0").endObject())
            .get();
        client().prepareIndex(indexName, "_doc")
            .setId("3")
            .setSource(jsonBuilder().startObject().field("version", "2.1.0-alpha").endObject())
            .get();
        client().prepareIndex(indexName, "_doc")
            .setId("4")
            .setSource(jsonBuilder().startObject().field("version", "2.1.0").endObject())
            .get();
        client().prepareIndex(indexName, "_doc")
            .setId("5")
            .setSource(jsonBuilder().startObject().field("version", "3.11.5").endObject())
            .get();
        refresh();

        // terms aggs
        SearchResponse response = client().prepareSearch(indexName)
            .addAggregation(AggregationBuilders.terms("myterms").field("version"))
            .get();
        Terms terms = response.getAggregations().get("myterms");
        List<? extends Bucket> buckets = terms.getBuckets();

        assertEquals(5, buckets.size());
        assertEquals("1.0", buckets.get(0).getKey());
        assertEquals("1.3.0", buckets.get(1).getKey());
        assertEquals("2.1.0-alpha", buckets.get(2).getKey());
        assertEquals("2.1.0", buckets.get(3).getKey());
        assertEquals("3.11.5", buckets.get(4).getKey());
    }
}
