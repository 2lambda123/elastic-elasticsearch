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

package org.elasticsearch.search.internal;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.AliasFilterParsingException;
import org.elasticsearch.indices.InvalidAliasNameException;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.function.Function;

/**
 * Shard level request that represents a search.
 * It provides all the methods that the {@link org.elasticsearch.search.internal.SearchContext} needs.
 * Provides a cache key based on its content that can be used to cache shard level response.
 */
public interface ShardSearchRequest {

    ShardId shardId();

    String[] types();

    SearchSourceBuilder source();

    void source(SearchSourceBuilder source);

    int numberOfShards();

    SearchType searchType();

    QueryBuilder filteringAliases();

    long nowInMillis();

    Boolean requestCache();

    Scroll scroll();

    /**
     * Sets if this shard search needs to be profiled or not
     * @param profile True if the shard should be profiled
     */
    void setProfile(boolean profile);

    /**
     * Returns true if this shard search is being profiled or not
     */
    boolean isProfile();

    /**
     * Returns the cache key for this shard search request, based on its content
     */
    BytesReference cacheKey() throws IOException;

    /**
     * Rewrites this request into its primitive form. e.g. by rewriting the
     * QueryBuilder.
     */
    void rewrite(QueryShardContext context) throws IOException;

    /**
     * Returns the filter associated with listed filtering aliases.
     * <p>
     * The list of filtering aliases should be obtained by calling MetaData.filteringAliases.
     * Returns <tt>null</tt> if no filtering is required.</p>
     */
    static QueryBuilder parseAliasFilter(Function<XContentParser, QueryParseContext> contextFactory,
                                         IndexMetaData metaData, String... aliasNames) {
        if (aliasNames == null || aliasNames.length == 0) {
            return null;
        }
        Index index = metaData.getIndex();
        ImmutableOpenMap<String, AliasMetaData> aliases = metaData.getAliases();
        Function<AliasMetaData, QueryBuilder> parserFunction = (alias) -> {
            if (alias.filter() == null) {
                return null;
            }
            try {
                byte[] filterSource = alias.filter().uncompressed();
                try (XContentParser parser = XContentFactory.xContent(filterSource).createParser(filterSource)) {
                    return contextFactory.apply(parser).parseInnerQueryBuilder();
                }
            } catch (IOException ex) {
                throw new AliasFilterParsingException(index, alias.getAlias(), "Invalid alias filter", ex);
            }
        };
        if (aliasNames.length == 1) {
            AliasMetaData alias = aliases.get(aliasNames[0]);
            if (alias == null) {
                // This shouldn't happen unless alias disappeared after filteringAliases was called.
                throw new InvalidAliasNameException(index, aliasNames[0], "Unknown alias name was passed to alias Filter");
            }
            return parserFunction.apply(alias);
        } else {
            // we need to bench here a bit, to see maybe it makes sense to use OrFilter
            BoolQueryBuilder combined = new BoolQueryBuilder();
            for (String aliasName : aliasNames) {
            AliasMetaData alias = aliases.get(aliasName);
            if (alias == null) {
                // This shouldn't happen unless alias disappeared after filteringAliases was called.
                throw new InvalidAliasNameException(index, aliasNames[0],
                    "Unknown alias name was passed to alias Filter");
            }
            QueryBuilder parsedFilter = parserFunction.apply(alias);
            if (parsedFilter != null) {
                combined.should(parsedFilter);
            } else {
                // The filter might be null only if filter was removed after filteringAliases was called
                return null;
            }
            }
            return combined;
        }
    }

}
