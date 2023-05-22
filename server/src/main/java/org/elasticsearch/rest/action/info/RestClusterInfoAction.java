/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.info;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.ChunkedRestResponseBody;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;

public class RestClusterInfoAction extends BaseRestHandler {

    static final Map<String, Function<NodesStatsResponse, ChunkedToXContent>> RESPONSE_MAPPER = Map.of(
        NodesInfoRequest.Metric.HTTP.metricName(),
        nodesStatsResponse -> nodesStatsResponse.getNodes().stream().map(NodeStats::getHttp).reduce(HttpStats.IDENTITY, HttpStats::merge)
    );
    static final Set<String> AVAILABLE_METRICS = RESPONSE_MAPPER.keySet();

    @Override
    public String getName() {
        return "cluster_info_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_info/{metric}"));
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        var nodesStatsRequest = new NodesStatsRequest().clear();
        var metrics = Strings.tokenizeByCommaToSet(request.param("metric"));

        if (metrics.size() == 1 && metrics.contains("_all")) {
            metrics.clear();
            AVAILABLE_METRICS.forEach(m -> {
                nodesStatsRequest.addMetric(m);
                metrics.add(m);
            });

        } else if (metrics.contains("_all")) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "request [%s] contains _all and individual metrics [%s]",
                    request.path(),
                    request.param("metric")
                )
            );

        } else {
            var invalidMetricParams = metrics.stream()
                .filter(Predicate.not(AVAILABLE_METRICS::contains))
                .collect(Collectors.toCollection(TreeSet::new));

            if (invalidMetricParams.isEmpty() == false) {
                throw new IllegalArgumentException(unrecognized(request, invalidMetricParams, AVAILABLE_METRICS, "metric"));
            }

            metrics.forEach(nodesStatsRequest::addMetric);
        }

        return channel -> client.admin().cluster().nodesStats(nodesStatsRequest, new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(NodesStatsResponse response) throws Exception {
                var chunkedResponses = metrics.stream().map(RESPONSE_MAPPER::get).map(mapper -> mapper.apply(response)).iterator();

                return new RestResponse(
                    RestStatus.OK,
                    ChunkedRestResponseBody.fromXContent(
                        outerParams -> Iterators.concat(
                            ChunkedToXContentHelper.startObject(),
                            Iterators.single((builder, params) -> builder.field("cluster_name", response.getClusterName().value())),
                            Iterators.flatMap(chunkedResponses, chunk -> chunk.toXContentChunked(outerParams)),
                            ChunkedToXContentHelper.endObject()
                        ),
                        EMPTY_PARAMS,
                        channel
                    )
                );
            }
        });
    }
}
