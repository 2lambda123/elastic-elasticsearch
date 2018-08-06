/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.lucene.util.LuceneTestCase.random;
import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomDateHistogramGroupConfig;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DateHistogramGroupConfigSerializingTests extends AbstractSerializingTestCase<DateHistogramGroupConfig> {
    @Override
    protected DateHistogramGroupConfig doParseInstance(final XContentParser parser) throws IOException {
        return DateHistogramGroupConfig.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<DateHistogramGroupConfig> instanceReader() {
        return DateHistogramGroupConfig::new;
    }

    @Override
    protected DateHistogramGroupConfig createTestInstance() {
        return randomDateHistogramGroupConfig(random());
    }

    public void testValidateNoMapping() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        DateHistogramGroupConfig config = new DateHistogramGroupConfig("my_field", new DateHistogramInterval("1d"), null, null);
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("Could not find a [date] field with name [my_field] in any of the " +
                "indices matching the index pattern."));
    }

    public void testValidateNomatchingField() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        responseMap.put("some_other_field", Collections.singletonMap("date", fieldCaps));

        DateHistogramGroupConfig config = new DateHistogramGroupConfig("my_field", new DateHistogramInterval("1d"), null, null);
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("Could not find a [date] field with name [my_field] in any of the " +
                "indices matching the index pattern."));
    }

    public void testValidateFieldWrongType() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        responseMap.put("my_field", Collections.singletonMap("keyword", fieldCaps));

        DateHistogramGroupConfig config = new DateHistogramGroupConfig("my_field", new DateHistogramInterval("1d"), null, null);
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("The field referenced by a date_histo group must be a [date] type across all " +
                "indices in the index pattern.  Found: [keyword] for field [my_field]"));
    }

    public void testValidateFieldMixtureTypes() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        Map<String, FieldCapabilities> types = new HashMap<>(2);
        types.put("date", fieldCaps);
        types.put("keyword", fieldCaps);
        responseMap.put("my_field", types);

        DateHistogramGroupConfig config = new DateHistogramGroupConfig("my_field", new DateHistogramInterval("1d"), null, null);
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("The field referenced by a date_histo group must be a [date] type across all " +
                "indices in the index pattern.  Found: [date, keyword] for field [my_field]"));
    }

    public void testValidateFieldMatchingNotAggregatable() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(false);
        responseMap.put("my_field", Collections.singletonMap("date", fieldCaps));

        DateHistogramGroupConfig config =new DateHistogramGroupConfig("my_field", new DateHistogramInterval("1d"), null, null);
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("The field [my_field] must be aggregatable across all indices, but is not."));
    }

    public void testValidateMatchingField() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(true);
        responseMap.put("my_field", Collections.singletonMap("date", fieldCaps));

        DateHistogramGroupConfig config = new DateHistogramGroupConfig("my_field", new DateHistogramInterval("1d"), null, null);
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().size(), equalTo(0));
    }

    /**
     * Tests that a DateHistogramGroupConfig can be serialized/deserialized correctly after
     * the timezone was changed from DateTimeZone to String.
     */
    public void testBwcSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            final DateHistogramGroupConfig reference = ConfigTestHelpers.randomDateHistogramGroupConfig(random());

            final BytesStreamOutput out = new BytesStreamOutput();
            reference.writeTo(out);

            // previous way to deserialize a DateHistogramGroupConfig
            final StreamInput in = out.bytes().streamInput();
            DateHistogramInterval interval = new DateHistogramInterval(in);
            String field = in.readString();
            DateHistogramInterval delay = in.readOptionalWriteable(DateHistogramInterval::new);
            DateTimeZone timeZone = in.readTimeZone();

            assertEqualInstances(reference, new DateHistogramGroupConfig(field, interval, delay, timeZone.getID()));
        }

        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            final String field = ConfigTestHelpers.randomField(random());
            final DateHistogramInterval interval = ConfigTestHelpers.randomInterval();
            final DateHistogramInterval delay = randomBoolean() ? ConfigTestHelpers.randomInterval() : null;
            final DateTimeZone timezone = randomDateTimeZone();

            // previous way to serialize a DateHistogramGroupConfig
            final BytesStreamOutput out = new BytesStreamOutput();
            interval.writeTo(out);
            out.writeString(field);
            out.writeOptionalWriteable(delay);
            out.writeTimeZone(timezone);

            final StreamInput in = out.bytes().streamInput();
            DateHistogramGroupConfig deserialized = new DateHistogramGroupConfig(in);

            assertEqualInstances(new DateHistogramGroupConfig(field, interval, delay, timezone.getID()), deserialized);
        }
    }
}
