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

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder.Range;
import org.elasticsearch.test.geo.RandomShapeGenerator;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.*;

public class GeoDistanceRangeTests extends BaseAggregationTestCase<GeoDistanceAggregationBuilder> {

    @Override
    protected GeoDistanceAggregationBuilder createTestAggregatorBuilder() {
        int numRanges = randomIntBetween(1, 10);
        GeoPoint origin = RandomShapeGenerator.randomPoint(random());
        GeoDistanceAggregationBuilder factory = new GeoDistanceAggregationBuilder(randomAlphaOfLengthBetween(3, 10), origin);
        for (int i = 0; i < numRanges; i++) {
            String key = null;
            if (randomBoolean()) {
                key = randomAlphaOfLengthBetween(1, 20);
            }
            double from = randomBoolean() ? 0 : randomIntBetween(0, Integer.MAX_VALUE - 1000);
            double to = randomBoolean() ? Double.POSITIVE_INFINITY
                    : (Double.compare(from, 0) == 0 ? randomIntBetween(0, Integer.MAX_VALUE)
                            : randomIntBetween((int) from, Integer.MAX_VALUE));
            factory.addRange(new Range(key, from, to));
        }
        factory.field(randomAlphaOfLengthBetween(1, 20));
        if (randomBoolean()) {
            factory.keyed(randomBoolean());
        }
        if (randomBoolean()) {
            factory.missing("0, 0");
        }
        if (randomBoolean()) {
            factory.unit(randomFrom(DistanceUnit.values()));
        }
        return factory;
    }

    public void testParsingRangeStrict() throws IOException {
        final String rangeAggregation = "{\n" +
                "\"field\" : \"location\",\n" +
                "\"origin\" : \"52.3760, 4.894\",\n" +
                "\"unit\" : \"m\",\n" +
                "\"ranges\" : [\n" +
                "    { \"from\" : 10000, \"to\" : 20000, \"badField\" : \"abcd\" }\n" +
                "]\n" +
            "}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, rangeAggregation);
        XContentParseException ex = expectThrows(XContentParseException.class,
            () -> GeoDistanceAggregationBuilder.parse("aggregationName", parser));
        assertThat(ex.getCause(), notNullValue());
        assertThat(ex.getCause().getMessage(), containsString("badField"));
    }

    @Override
    public void testFromXContent() throws IOException {
        super.testFromXContent();
        assertWarnings(true,
            "Deprecated field [distance_type] used, this field is unused and will be removed entirely");
    }

    @Override
    public void testFromXContentMulti() throws IOException {
        try {
            super.testFromXContentMulti();
            assertWarnings(true,
                "Deprecated field [distance_type] used, this field is unused and will be removed entirely");
        } catch (java.lang.AssertionError assertionError) {
            if (assertionError.getMessage().startsWith("Expected 1 warnings but found ")) {
                // expect number of warnings to be a random number > 2
            } else {
                throw assertionError;
            }
        }
    }

    @Override
    public void testToString() throws IOException {
        super.testToString();
        assertWarnings(true,
            "Deprecated field [distance_type] used, this field is unused and will be removed entirely");
    }

    /**
     * We never render "null" values to xContent, but we should test that we can parse them (and they return correct defaults)
     */
    public void testParsingNull() throws IOException {
        final String rangeAggregation = "{\n" +
                "\"field\" : \"location\",\n" +
                "\"origin\" : \"52.3760, 4.894\",\n" +
                "\"unit\" : \"m\",\n" +
                "\"ranges\" : [\n" +
                "    { \"from\" : null, \"to\" : null }\n" +
                "]\n" +
            "}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, rangeAggregation);
        GeoDistanceAggregationBuilder aggregationBuilder = (GeoDistanceAggregationBuilder) GeoDistanceAggregationBuilder
                .parse("aggregationName", parser);
        assertEquals(1, aggregationBuilder.range().size());
        assertEquals(0.0, aggregationBuilder.range().get(0).getFrom(), 0.0);
        assertEquals(Double.POSITIVE_INFINITY, aggregationBuilder.range().get(0).getTo(), 0.0);
    }
}
