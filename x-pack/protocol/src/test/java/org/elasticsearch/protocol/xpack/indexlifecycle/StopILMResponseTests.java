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

package org.elasticsearch.protocol.xpack.indexlifecycle;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;

public class StopILMResponseTests extends AbstractStreamableXContentTestCase<StopILMResponse> {

    @Override
    protected StopILMResponse createBlankInstance() {
        return new StopILMResponse();
    }

    @Override
    protected StopILMResponse createTestInstance() {
        return new StopILMResponse(randomBoolean());
    }

    @Override
    protected StopILMResponse mutateInstance(StopILMResponse instance) throws IOException {
        return new StopILMResponse(instance.isAcknowledged() == false);
    }

    @Override
    protected StopILMResponse doParseInstance(XContentParser parser) throws IOException {
        return StopILMResponse.fromXContent(parser);
    }

}
