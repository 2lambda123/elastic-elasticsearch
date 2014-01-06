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

package org.elasticsearch.index.query;

import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

public class GeoShapeQueryBuilderTests extends ElasticsearchTestCase {

    @Test // see #3878
    public void testThatXContentSerializationInsideOfArrayWorks() throws Exception {
        EnvelopeBuilder envelopeBuilder = ShapeBuilder.newEnvelope().topLeft(0, 0).bottomRight(10, 10);
        GeoShapeQueryBuilder geoQuery = QueryBuilders.geoShapeQuery("searchGeometry", envelopeBuilder);
        JsonXContent.contentBuilder().startArray().value(geoQuery).endArray();
    }
}
