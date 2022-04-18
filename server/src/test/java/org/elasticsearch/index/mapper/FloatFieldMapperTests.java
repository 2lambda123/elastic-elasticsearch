/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.mapper.NumberFieldTypeTests.OutOfRangeSpec;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class FloatFieldMapperTests extends FloatingPointNumberFieldMapperTestCase {

    @Override
    protected Number missingValue() {
        return 123f;
    }

    @Override
    protected List<OutOfRangeSpec> outOfRangeSpecs() {
        return List.of(
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.FLOAT, "3.4028235E39", "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.FLOAT, "-3.4028235E39", "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.FLOAT, Float.NaN, "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.FLOAT, Float.POSITIVE_INFINITY, "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberFieldMapper.NumberType.FLOAT, Float.NEGATIVE_INFINITY, "[float] supports only finite values")
        );
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "float");
    }

    @Override
    protected Number randomNumber() {
        /*
         * The source parser and doc values round trip will both reduce
         * the precision to 32 bits if the value is more precise.
         * randomDoubleBetween will smear the values out across a wide
         * range of valid values.
         */
        return randomBoolean() ? randomDoubleBetween(-Float.MAX_VALUE, Float.MAX_VALUE, true) : randomFloat();
    }

    @Override
    protected SyntheticSourceExample syntheticSourceExample() throws IOException {
        if (randomBoolean()) {
            Number n = randomNumber();
            return new SyntheticSourceExample(n, n.floatValue(), this::minimalMapping);
        }
        List<Number> in = randomList(1, 5, this::randomNumber);
        Object out = in.size() == 1 ? in.get(0).floatValue() : in.stream().map(n -> n.floatValue()).sorted().toList();
        return new SyntheticSourceExample(in, out, this::minimalMapping);
    }

    @Override
    protected Optional<ScriptFactory> emptyFieldScript() {
        return Optional.empty();
    }

    @Override
    protected Optional<ScriptFactory> nonEmptyFieldScript() {
        return Optional.empty();
    }
}
