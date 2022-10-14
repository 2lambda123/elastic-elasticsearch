/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.xsearch.action.rest;

import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.relevancesearch.query.RelevanceMatchQueryBuilder;
import org.elasticsearch.xpack.relevancesearch.query.RelevanceMatchQueryRewriter;
import org.elasticsearch.xpack.relevancesearch.xsearch.action.XSearchSearchAction;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestXSearchSearchAction extends BaseRestHandler {

    public static final String REST_BASE_PATH = "/{index}/_xsearch";

    private final RelevanceMatchQueryRewriter relevanceMatchQueryRewriter;

    @Inject
    public RestXSearchSearchAction(RelevanceMatchQueryRewriter relevanceMatchQueryRewriter) {
        super();
        this.relevanceMatchQueryRewriter = relevanceMatchQueryRewriter;
    }
    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, REST_BASE_PATH), new Route(POST, REST_BASE_PATH));
    }

    @Override
    public String getName() {
        return "xsearch_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String index = request.param("index");
        XContentParser parser = request.contentOrSourceParamParser();
        XSearchSearchAction.Request xsearchRequest = XSearchSearchAction.Request.parseRequest(index, parser);
        xsearchRequest.indicesOptions(IndicesOptions.fromRequest(request, xsearchRequest.indicesOptions()));

        RelevanceMatchQueryBuilder queryBuilder = createQueryBuilder(xsearchRequest);
        return channel -> doXSearch(index, queryBuilder, client, channel);
    }

    private RelevanceMatchQueryBuilder createQueryBuilder(XSearchSearchAction.Request request) {
        return new RelevanceMatchQueryBuilder(relevanceMatchQueryRewriter, request.getQuery());
    }

    /**** POC CODE BEGINS HERE *****/

    private static void doXSearch(String index, RelevanceMatchQueryBuilder queryBuilder, NodeClient client, RestChannel channel) {

        SearchRequest searchRequest = client.prepareSearch(index).setQuery(queryBuilder).setSize(1000).setFetchSource(true).request();

        client.execute(SearchAction.INSTANCE, searchRequest, new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(SearchResponse searchResponse, XContentBuilder builder) throws Exception {
                searchResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
                return new RestResponse(RestStatus.OK, builder);
            }
        });
    }

    @Override
    protected Set<String> responseParams() {
        return Collections.emptySet();
    }
}
