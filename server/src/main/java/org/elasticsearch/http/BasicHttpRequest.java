/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.rest.RestRequest;

import java.util.List;
import java.util.Map;

public interface BasicHttpRequest {

    /**
     * Returns the HTTP method used in the HTTP request.
     *
     * @return the {@link RestRequest.Method} used in the REST request
     * @throws IllegalArgumentException if the HTTP method is invalid
     */
    RestRequest.Method method();

    /**
     * The uri of the rest request, with the query string.
     */
    String uri();

    /**
     * Get all of the headers and values associated with the headers. Modifications of this map are not supported.
     */
    Map<String, List<String>> getHeaders();

    default String header(String name) {
        List<String> values = getHeaders().get(name);
        if (values != null && values.isEmpty() == false) {
            return values.get(0);
        }
        return null;
    }

}
