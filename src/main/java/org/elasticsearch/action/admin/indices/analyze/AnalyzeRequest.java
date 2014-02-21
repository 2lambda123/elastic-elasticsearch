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
package org.elasticsearch.action.admin.indices.analyze;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.single.custom.SingleCustomOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to analyze a text associated with a specific index. Allow to provide
 * the actual analyzer name to perform the analysis with.
 */
public class AnalyzeRequest extends SingleCustomOperationRequest<AnalyzeRequest> {

    private String index;

    private String text;

    private String analyzer;

    private String tokenizer;

    private String[] tokenFilters = Strings.EMPTY_ARRAY;

    private String[] charFilters = Strings.EMPTY_ARRAY;

    private String field;

    AnalyzeRequest() {

    }

    /**
     * Constructs a new analyzer request for the provided text.
     *
     * @param text The text to analyze
     */
    public AnalyzeRequest(String text) {
        this.text = text;
    }

    /**
     * Constructs a new analyzer request for the provided index and text.
     *
     * @param index The index name
     * @param text  The text to analyze
     */
    public AnalyzeRequest(@Nullable String index, String text) {
        this.index = index;
        this.text = text;
    }

    public String text() {
        return this.text;
    }

    public AnalyzeRequest index(String index) {
        this.index = index;
        return this;
    }

    public String index() {
        return this.index;
    }

    public AnalyzeRequest analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public String analyzer() {
        return this.analyzer;
    }

    public AnalyzeRequest tokenizer(String tokenizer) {
        this.tokenizer = tokenizer;
        return this;
    }

    public String tokenizer() {
        return this.tokenizer;
    }

    public AnalyzeRequest tokenFilters(String... tokenFilters) {
        if (tokenFilters == null) throw new ElasticsearchIllegalArgumentException("token filters must not be null");
        this.tokenFilters = tokenFilters;
        return this;
    }

    public String[] tokenFilters() {
        return this.tokenFilters;
    }

    public AnalyzeRequest charFilters(String... charFilters) {
        if (charFilters == null) throw new ElasticsearchIllegalArgumentException("char filters must not be null");
        this.charFilters = charFilters;
        return this;
    }

    public String[] charFilters() {
        return this.charFilters;
    }

    public AnalyzeRequest field(String field) {
        this.field = field;
        return this;
    }

    public String field() {
        return this.field;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (text == null) {
            validationException = addValidationError("text is missing", validationException);
        }
        if (tokenFilters == null) {
            validationException = addValidationError("tokenFilters is null", validationException);
        }
        if (charFilters == null) {
            validationException = addValidationError("charFilters is null", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readOptionalString();
        text = in.readString();
        analyzer = in.readOptionalString();
        tokenizer = in.readOptionalString();
        tokenFilters = in.readStringArray();
        if (in.getVersion().onOrAfter(Version.V_1_1_0)) {
            charFilters = in.readStringArray();
        }
        field = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(index);
        out.writeString(text);
        out.writeOptionalString(analyzer);
        out.writeOptionalString(tokenizer);
        out.writeStringArrayNullable(tokenFilters);
        if (out.getVersion().onOrAfter(Version.V_1_1_0)) {
            out.writeStringArrayNullable(charFilters);
        }
        out.writeOptionalString(field);
    }
}
