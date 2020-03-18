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

package org.elasticsearch.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.asyncsearch.SubmitAsyncSearchRequest;
import org.elasticsearch.client.asyncsearch.AsyncSearchResponse;

import java.io.IOException;

import static java.util.Collections.emptySet;

public class AsyncSearchClient {
    private final RestHighLevelClient restHighLevelClient;

    AsyncSearchClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    // TODO add docs url
    /**
     * Submit a new async search request.
     * See <a href="todo"> the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AsyncSearchResponse submitAsyncSearch(SubmitAsyncSearchRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, AsyncSearchRequestConverters::submitAsyncSearch, options,
                AsyncSearchResponse::fromXContent, emptySet());
    }

    // TODO add docs url
    /**
     * Asynchronously submit a new async search request.
     * See <a href="todo"> the docs</a> for more.
     * <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-ilm-ilm-get-lifecycle-policy.html">
     * the docs</a> for more.
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable submitAsyncSearchAsync(SubmitAsyncSearchRequest request, RequestOptions options,
                                               ActionListener<AsyncSearchResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, AsyncSearchRequestConverters::submitAsyncSearch, options,
                AsyncSearchResponse::fromXContent, listener, emptySet());
    }

}
