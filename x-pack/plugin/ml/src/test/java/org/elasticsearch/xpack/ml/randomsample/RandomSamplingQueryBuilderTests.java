/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.randomsample;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.TestGeoShapeFieldMapperPlugin;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class RandomSamplingQueryBuilderTests extends AbstractQueryTestCase<RandomSamplingQueryBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(MachineLearning.class, TestGeoShapeFieldMapperPlugin.class);
    }

    private boolean isCacheable = false;

    @Override
    protected boolean builderGeneratesCacheableQueries() {
        return isCacheable;
    }

    @Override
    protected RandomSamplingQueryBuilder doCreateTestQueryBuilder() {
        double p = randomDoubleBetween(0.00001, 0.999999, true);
        RandomSamplingQueryBuilder builder = new RandomSamplingQueryBuilder().setProbability(p);
        if (randomBoolean()) {
            builder.setSeed(123);
            isCacheable = true;
        } else {
            isCacheable = false;
        }
        if (randomBoolean()) {
            builder.setQuery(QueryBuilders.matchAllQuery());
        }
        return builder;
    }

    @Override
    protected void doAssertLuceneQuery(RandomSamplingQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        assertThat(query, instanceOf(RandomSamplingQuery.class));
    }

    public void testIllegalArguments() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RandomSamplingQueryBuilder().setProbability(0.0)
        );
        assertEquals("[probability] cannot be less than or equal to 0.", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new RandomSamplingQueryBuilder().setProbability(-5.0));
        assertEquals("[probability] cannot be less than or equal to 0.", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new RandomSamplingQueryBuilder().setProbability(1.0));
        assertEquals("[probability] cannot be greater than or equal to 1.", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new RandomSamplingQueryBuilder().setProbability(5.0));
        assertEquals("[probability] cannot be greater than or equal to 1.", e.getMessage());
    }

    public void testFromJson() throws IOException {
        String json = "{ \"random_sample\": {\"boost\":1.0,\"probability\": 0.5}}";
        RandomSamplingQueryBuilder parsed = (RandomSamplingQueryBuilder) parseQuery(json);
        assertThat(parsed.getProbability(), equalTo(0.5));

        // try with seed
        json = "{ \"random_sample\": {\"boost\":1.0,\"probability\": 0.5,\"seed\":123}}";
        parsed = (RandomSamplingQueryBuilder) parseQuery(json);
        assertThat(parsed.getProbability(), equalTo(0.5));
        assertThat(parsed.getSeed(), equalTo(123));
    }

    @Override
    public void testCacheability() throws IOException {
        RandomSamplingQueryBuilder queryBuilder = createTestQueryBuilder();
        SearchExecutionContext context = createSearchExecutionContext();
        QueryBuilder rewriteQuery = rewriteQuery(queryBuilder, new SearchExecutionContext(context));
        assertNotNull(rewriteQuery.toQuery(context));
        assertThat(
            "query should " + (isCacheable ? "" : "not") + " be cacheable: " + queryBuilder.toString(),
            context.isCacheable(),
            is(isCacheable)
        );
    }

}
