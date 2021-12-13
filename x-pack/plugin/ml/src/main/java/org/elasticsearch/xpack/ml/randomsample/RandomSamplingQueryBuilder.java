/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.randomsample;

import com.carrotsearch.hppc.BitMixer;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.InnerHitContextBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * A query that randomly matches documents with a user-provided probability.  May
 * optionally include a seed so that matches are deterministic
 */
public class RandomSamplingQueryBuilder extends AbstractQueryBuilder<RandomSamplingQueryBuilder> {
    public static final String NAME = "random_sample";
    private static final ParseField PROBABILITY = new ParseField("probability");
    private static final ParseField SEED = new ParseField("seed");
    private static final ParseField FILTER = new ParseField("filter");

    private double p = 0.1;
    private boolean setSeed = false;
    private int seed = Randomness.get().nextInt();
    private QueryBuilder queryBuilder;

    public RandomSamplingQueryBuilder() {}

    /**
     * Read from a stream.
     */
    public RandomSamplingQueryBuilder(StreamInput in) throws IOException {
        super(in);
        p = in.readDouble();
        seed = in.readInt();
        setSeed = in.readBoolean();
        queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(p);
        out.writeInt(seed);
        out.writeBoolean(setSeed);
        out.writeOptionalNamedWriteable(queryBuilder);
    }

    public double getProbability() {
        return p;
    }

    public RandomSamplingQueryBuilder setProbability(double p) {
        this.p = validateProbability(p);
        return this;
    }

    public int getSeed() {
        return seed;
    }

    public RandomSamplingQueryBuilder setSeed(int seed) {
        this.seed = seed;
        this.setSeed = true;
        return this;
    }

    private void setGeneratedSeed(int seed) {
        if (setSeed == false) {
            this.seed = seed;
        }
    }

    public RandomSamplingQueryBuilder setQuery(QueryBuilder query) {
        this.queryBuilder = query;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        printBoostAndQueryName(builder);
        builder.field(PROBABILITY.getPreferredName(), p);
        if (setSeed) {
            builder.field(SEED.getPreferredName(), seed);
        }
        if (queryBuilder != null) {
            builder.field(FILTER.getPreferredName());
            queryBuilder.toXContent(builder, params);
        }
        builder.endObject();
    }

    public static final ObjectParser<RandomSamplingQueryBuilder, Void> PARSER = new ObjectParser<>(NAME, RandomSamplingQueryBuilder::new);

    static {
        declareStandardFields(PARSER);
        PARSER.declareDouble(RandomSamplingQueryBuilder::setProbability, PROBABILITY);
        PARSER.declareInt(RandomSamplingQueryBuilder::setSeed, SEED);
        PARSER.declareObject(RandomSamplingQueryBuilder::setQuery, (p, c) -> AbstractQueryBuilder.parseInnerQueryBuilder(p), FILTER);
    }

    @Override
    protected final Query doToQuery(SearchExecutionContext context) throws IOException {
        long hash = BitMixer.mix(context.index().getUUID(), context.getShardId());
        if (this.setSeed == false) {
            context.disableCache();
        }
        return new RandomSamplingQuery(p, seed, hash, this.setSeed, queryBuilder == null ? null : queryBuilder.toQuery(context));
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (queryBuilder != null) {
            QueryBuilder rewrite = queryBuilder.rewrite(queryRewriteContext);
            if (rewrite instanceof MatchNoneQueryBuilder) {
                return rewrite; // we won't match anyway
            }
            if (rewrite != queryBuilder) {
                RandomSamplingQueryBuilder builder = new RandomSamplingQueryBuilder().setQuery(rewrite).setProbability(p);
                if (setSeed) {
                    builder.setSeed(seed);
                } else {
                    builder.setGeneratedSeed(seed);
                }
                return builder;
            }
        }
        return this;
    }

    @Override
    protected void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
        if (queryBuilder != null) {
            InnerHitContextBuilder.extractInnerHits(queryBuilder, innerHits);
        }
    }

    @Override
    protected boolean doEquals(RandomSamplingQueryBuilder other) {
        return Objects.equals(p, other.p)
            && (setSeed == false || Objects.equals(seed, other.seed))
            && Objects.equals(queryBuilder, other.queryBuilder);
    }

    @Override
    protected int doHashCode() {
        if (setSeed) {
            return Objects.hash(p, seed, queryBuilder);
        } else {
            return Objects.hash(p, queryBuilder);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    private double validateProbability(double p) {
        if (p <= 0.0) {
            throw new IllegalArgumentException("[" + PROBABILITY.getPreferredName() + "] cannot be less than or equal to 0.");
        }
        if (p >= 1.0) {
            throw new IllegalArgumentException("[" + PROBABILITY.getPreferredName() + "] cannot be greater than or equal to 1.");
        }
        return p;
    }
}
