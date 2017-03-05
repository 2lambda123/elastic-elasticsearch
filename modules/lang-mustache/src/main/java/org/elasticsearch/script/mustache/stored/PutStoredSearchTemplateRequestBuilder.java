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

package org.elasticsearch.script.mustache.stored;

import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;

public class PutStoredSearchTemplateRequestBuilder extends AcknowledgedRequestBuilder<
            PutStoredSearchTemplateRequest,
            PutStoredSearchTemplateResponse,
            PutStoredSearchTemplateRequestBuilder> {

    public PutStoredSearchTemplateRequestBuilder(ElasticsearchClient client, PutStoredSearchTemplateAction action) {
        super(client, action, new PutStoredSearchTemplateRequest());
    }

    public PutStoredSearchTemplateRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    public PutStoredSearchTemplateRequestBuilder setContent(BytesReference content, XContentType xContentType) {
        request.content(content, xContentType);
        return this;
    }

}
