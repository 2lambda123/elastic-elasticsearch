/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.query;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Same as {@link MatchQueryBuilder} but supports multiple fields.
 */
public class MultiMatchQueryBuilder extends BaseQueryBuilder implements BoostableQueryBuilder<MultiMatchQueryBuilder> {

    private final Object text;

    private final List<String> fields;

    private MatchQueryBuilder.Type type;

    private MatchQueryBuilder.Operator operator;

    private String analyzer;

    private Float boost;

    private Integer slop;

    private String fuzziness;

    private Integer prefixLength;

    private Integer maxExpansions;

    private String minimumShouldMatch;

    private String rewrite = null;

    private String fuzzyRewrite = null;

    private Boolean useDisMax;

    private Integer tieBreaker;

    private Boolean lenient;

    /**
     * Constructs a new text query.
     */
    public MultiMatchQueryBuilder(Object text, String... fields) {
        this.fields = Arrays.asList(fields);
        this.text = text;
    }

    /**
     * Sets the type of the text query.
     */
    public MultiMatchQueryBuilder type(MatchQueryBuilder.Type type) {
        this.type = type;
        return this;
    }

    /**
     * Sets the operator to use when using a boolean query. Defaults to <tt>OR</tt>.
     */
    public MultiMatchQueryBuilder operator(MatchQueryBuilder.Operator operator) {
        this.operator = operator;
        return this;
    }

    /**
     * Explicitly set the analyzer to use. Defaults to use explicit mapping config for the field, or, if not
     * set, the default search analyzer.
     */
    public MultiMatchQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /**
     * Set the boost to apply to the query.
     */
    public MultiMatchQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Set the phrase slop if evaluated to a phrase query type.
     */
    public MultiMatchQueryBuilder slop(int slop) {
        this.slop = slop;
        return this;
    }

    /**
     * Sets the minimum similarity used when evaluated to a fuzzy query type. Defaults to "0.5".
     */
    public MultiMatchQueryBuilder fuzziness(Object fuzziness) {
        this.fuzziness = fuzziness.toString();
        return this;
    }

    public MultiMatchQueryBuilder prefixLength(int prefixLength) {
        this.prefixLength = prefixLength;
        return this;
    }

    /**
     * When using fuzzy or prefix type query, the number of term expansions to use. Defaults to unbounded
     * so its recommended to set it to a reasonable value for faster execution.
     */
    public MultiMatchQueryBuilder maxExpansions(int maxExpansions) {
        this.maxExpansions = maxExpansions;
        return this;
    }

    public MultiMatchQueryBuilder minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    public MultiMatchQueryBuilder rewrite(String rewrite) {
        this.rewrite = rewrite;
        return this;
    }

    public MultiMatchQueryBuilder fuzzyRewrite(String fuzzyRewrite) {
        this.fuzzyRewrite = fuzzyRewrite;
        return this;
    }

    public MultiMatchQueryBuilder useDisMax(Boolean useDisMax) {
        this.useDisMax = useDisMax;
        return this;
    }

    public MultiMatchQueryBuilder tieBreaker(Integer tieBreaker) {
        this.tieBreaker = tieBreaker;
        return this;
    }

    /**
     * Sets whether format based failures will be ignored.
     */
    public MultiMatchQueryBuilder lenient(boolean lenient) {
        this.lenient = lenient;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(MultiMatchQueryParser.NAME);

        builder.field("query", text);
        builder.field("fields", fields);

        if (type != null) {
            builder.field("type", type.toString().toLowerCase(Locale.ENGLISH));
        }
        if (operator != null) {
            builder.field("operator", operator.toString());
        }
        if (analyzer != null) {
            builder.field("analyzer", analyzer);
        }
        if (boost != null) {
            builder.field("boost", boost);
        }
        if (slop != null) {
            builder.field("slop", slop);
        }
        if (fuzziness != null) {
            builder.field("fuzziness", fuzziness);
        }
        if (prefixLength != null) {
            builder.field("prefix_length", prefixLength);
        }
        if (maxExpansions != null) {
            builder.field("max_expansions", maxExpansions);
        }
        if (minimumShouldMatch != null) {
            builder.field("minimum_should_match", minimumShouldMatch);
        }
        if (rewrite != null) {
            builder.field("rewrite", rewrite);
        }
        if (fuzzyRewrite != null) {
            builder.field("fuzzy_rewrite", fuzzyRewrite);
        }

        if (useDisMax != null) {
            builder.field("use_dis_max", useDisMax);
        }

        if (tieBreaker != null) {
            builder.field("tie_breaker", tieBreaker);
        }

        if (lenient != null) {
            builder.field("lenient", lenient);
        }

        builder.endObject();
    }
}