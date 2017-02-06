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

package org.elasticsearch.action.admin.indices.fieldcaps;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class FieldCapabilitiesIndexRequest extends SingleShardRequest<FieldCapabilitiesIndexRequest> {
    private String[] fields;

    FieldCapabilitiesIndexRequest() {}

    FieldCapabilitiesIndexRequest(FieldCapabilitiesRequest request, String index) {
        super(index);
        Set<String> fields = new HashSet<>();
        fields.addAll(Arrays.asList(request.fields()));
        this.fields = fields.toArray(new String[fields.size()]);
    }

    public String[] fields() {
        return fields;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        fields = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(fields);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
