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

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class ClearReaderRequest extends ActionRequest implements ToXContentObject {
    private static final ParseField ID = new ParseField("id");

    private final String id;

    public ClearReaderRequest(StreamInput in) throws IOException {
        super(in);
        this.id = in.readString();
    }

    public ClearReaderRequest(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (Strings.isEmpty(id)) {
            throw new IllegalArgumentException("reader id must be specified");
        }
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        builder.endObject();
        return builder;
    }

    public static ClearReaderRequest fromXContent(XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Malformed content, must start with an object");
        } else {
            XContentParser.Token token;
            String id = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME && parser.currentName().equals(ID.getPreferredName())) {
                    token = parser.nextToken();
                    if (token.isValue() == false) {
                        throw new IllegalArgumentException("the request must contain only [" + ID.getPreferredName() + " field");
                    }
                    id = parser.text();
                } else {
                    throw new IllegalArgumentException("Unknown parameter [" + parser.currentName() +
                        "] in request body or parameter is of the wrong type[" + token + "] ");
                }
            }
            if (Strings.isNullOrEmpty(id)) {
                throw new IllegalArgumentException("reader context is is not provided");
            }
            return new ClearReaderRequest(id);
        }
    }
}
