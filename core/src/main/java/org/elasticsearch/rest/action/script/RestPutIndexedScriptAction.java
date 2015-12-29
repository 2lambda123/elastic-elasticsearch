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
package org.elasticsearch.rest.action.script;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.BaseMultiMethodRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestGlobalContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.rest.RestStatus.CREATED;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 *
 */
public class RestPutIndexedScriptAction extends BaseMultiMethodRestHandler {
    public RestPutIndexedScriptAction(RestGlobalContext context) {
        this(context, "/_scripts/{lang}/{id}");
    }

    protected RestPutIndexedScriptAction(RestGlobalContext context, String path) {
        super(context, new Method[] {POST, PUT}, path, path + "/_create");
    }

    protected String getScriptLang(RestRequest request) {
        return request.param("lang");
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, Client client) {
        if (request.path().endsWith("/_create")) {
            request.params().put("op_type", "create");
        }
        PutIndexedScriptRequest putRequest = new PutIndexedScriptRequest(getScriptLang(request), request.param("id"));
        putRequest.version(request.paramAsLong("version", putRequest.version()));
        putRequest.versionType(VersionType.fromString(request.param("version_type"), putRequest.versionType()));
        putRequest.source(request.content());
        String sOpType = request.param("op_type");
        if (sOpType != null) {
            try {
                putRequest.opType(IndexRequest.OpType.fromString(sOpType));
            } catch (IllegalArgumentException eia){
                try {
                    XContentBuilder builder = channel.newErrorBuilder();
                    channel.sendResponse(new BytesRestResponse(BAD_REQUEST, builder.startObject().field("error", eia.getMessage()).endObject()));
                    return;
                } catch (IOException e1) {
                    logger.warn("Failed to send response", e1);
                    return;
                }
            }
        }

        client.putIndexedScript(putRequest, new RestBuilderListener<PutIndexedScriptResponse>(channel) {
            @Override
            public RestResponse buildResponse(PutIndexedScriptResponse response, XContentBuilder builder) throws Exception {
                builder.startObject()
                        .field(Fields._ID, response.getId())
                        .field(Fields._VERSION, response.getVersion())
                        .field(Fields.CREATED, response.isCreated());
                builder.endObject();
                RestStatus status = OK;
                if (response.isCreated()) {
                    status = CREATED;
                }
                return new BytesRestResponse(status, builder);
            }
        });
    }

    static final class Fields {
        static final XContentBuilderString _VERSION = new XContentBuilderString("_version");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString CREATED = new XContentBuilderString("created");
    }
}
