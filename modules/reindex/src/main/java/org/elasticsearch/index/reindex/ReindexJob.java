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

package org.elasticsearch.index.reindex;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.PersistentTaskParams;

import java.io.IOException;

public class ReindexJob implements PersistentTaskParams {

    // TODO: Name
    public static final String NAME = ReindexTask.NAME;

    public static final ConstructingObjectParser<ReindexJob, Void> PARSER
        = new ConstructingObjectParser<>(NAME, a -> new ReindexJob((Boolean) a[0]));

    private static String STORE_RESULT = "store_result";

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), new ParseField(STORE_RESULT));
    }

    private final boolean storeResult;

    ReindexJob(boolean storeResult) {
        this.storeResult = storeResult;
    }

    ReindexJob(StreamInput in) throws IOException {
        storeResult = in.readBoolean();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        // TODO: version
        return Version.V_8_0_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(storeResult);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(STORE_RESULT, storeResult);
        return builder.endObject();
    }

    public boolean shouldStoreResult() {
        return storeResult;
    }

    public static ReindexJob fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
