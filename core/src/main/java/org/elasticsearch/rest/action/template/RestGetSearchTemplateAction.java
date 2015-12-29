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
package org.elasticsearch.rest.action.template;

import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.RestGlobalContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.script.RestGetIndexedScriptAction;
import org.elasticsearch.script.Template;

/**
 *
 */
public class RestGetSearchTemplateAction extends RestGetIndexedScriptAction {
    public RestGetSearchTemplateAction(RestGlobalContext context) {
        super(context, "/_search/template/{id}");
    }

    @Override
    protected String getScriptLang(RestRequest request) {
        return Template.DEFAULT_LANG;
    }

    @Override
    protected XContentBuilderString getScriptFieldName() {
        return TEMPLATE;
    }

    private static final XContentBuilderString TEMPLATE = new XContentBuilderString("template");
}
