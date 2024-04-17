/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class FieldDataParseHelperTests extends ESTestCase {

    private String encodeAndDecode(String value) throws IOException {
        XContentParser p = createParser(JsonXContent.jsonXContent, "{ \"foo\": " + value + " }");
        assertThat(p.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(p.currentName(), equalTo("foo"));
        p.nextToken();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.humanReadable(true);
        FieldDataParseHelper.decodeAndWrite(builder, FieldDataParseHelper.encodeToken(p));
        return Strings.toString(builder);
    }

    public void testBoolean() throws IOException {
        boolean b = randomBoolean();
        assertEquals(b, Boolean.parseBoolean(encodeAndDecode(Boolean.toString(b))));
    }

    public void testString() throws IOException {
        String s = "\"" + randomAlphaOfLength(5) + "\"";
        assertEquals(s, encodeAndDecode(s));
    }

    public void testInt() throws IOException {
        int i = randomInt();
        assertEquals(i, Integer.parseInt(encodeAndDecode(Integer.toString(i))));
    }

    public void testLong() throws IOException {
        long l = randomLong();
        assertEquals(l, Long.parseLong(encodeAndDecode(Long.toString(l))));
    }

    public void testFloat() throws IOException {
        float f = randomFloat();
        assertEquals(0, Float.compare(f, Float.parseFloat(encodeAndDecode(Float.toString(f)))));
    }

    public void testDouble() throws IOException {
        double d = randomDouble();
        assertEquals(0, Double.compare(d, Double.parseDouble(encodeAndDecode(Double.toString(d)))));
    }

    public void testBigInteger() throws IOException {
        BigInteger i = randomBigInteger();
        assertEquals(i, new BigInteger(encodeAndDecode(i.toString()), 10));
    }

    public void testBigDecimal() throws IOException {
        BigDecimal d = new BigDecimal(randomBigInteger(), randomInt());
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject().field("foo", d).endObject();
        XContentParser p = JsonXContent.jsonXContent.createParser(parserConfig(), BytesReference.bytes(builder).streamInput());
        assertThat(p.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(p.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(p.currentName(), equalTo("foo"));
        p.nextToken();
        assertEquals(p.numberType(), XContentParser.NumberType.DOUBLE);

        // FieldDataParseHelper.decodeAndWrite(builder, FieldDataParseHelper.encodeToken(p));
        // assertEquals(d, new BigDecimal(Strings.toString(builder)));
    }

    public void testObject() throws IOException {
        String object = "{\"name\":\"foo\"}";
        XContentParser p = createParser(JsonXContent.jsonXContent, object);
        assertThat(p.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.humanReadable(true);
        FieldDataParseHelper.decodeAndWrite(builder, FieldDataParseHelper.encodeToken(p));
        assertEquals(object, Strings.toString(builder));
    }

    public void testArrayInt() throws IOException {
        String values = "["
            + String.join(",", List.of(Integer.toString(randomInt()), Integer.toString(randomInt()), Integer.toString(randomInt())))
            + "]";
        assertEquals(values, encodeAndDecode(values));
    }
}
