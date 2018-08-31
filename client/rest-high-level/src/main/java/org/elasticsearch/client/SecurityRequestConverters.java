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

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.security.ChangePasswordRequest;

import java.io.IOException;

import static org.elasticsearch.client.RequestConverters.REQUEST_BODY_CONTENT_TYPE;
import static org.elasticsearch.client.RequestConverters.createEntity;

public final class SecurityRequestConverters {

    private SecurityRequestConverters() {}

    static Request changePassword(ChangePasswordRequest changePasswordRequest) throws IOException {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_xpack/security")
            .addPathPart(changePasswordRequest.getUsername())
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(changePasswordRequest, REQUEST_BODY_CONTENT_TYPE));
        RequestConverters.Params params = new RequestConverters.Params(request);
        params.withRefreshPolicy(changePasswordRequest.getRefreshPolicy());
        return request;
    }


}
