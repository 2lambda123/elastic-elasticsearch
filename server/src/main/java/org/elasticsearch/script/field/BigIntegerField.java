/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;

public class BigIntegerField extends Field<java.math.BigInteger> {

    /* ---- Conversion Class From Other Fields ----*/

    /**
     * Convert to a {@link BigIntegerField} from Long, Double or String Fields.
     * Longs and Doubles are wrapped as BigIntegers.
     * Strings are parsed as either Longs or Doubles and wrapped in a BigInteger.
     */
    public static final Converter<BigInteger, BigIntegerField> BigInteger;

    static {
        BigInteger = new Converter<BigInteger, BigIntegerField>() {
            @Override
            public BigIntegerField convert(Field<?> sourceField) {
                if (sourceField instanceof LongField) {
                    return LongField.convertLongToBigIntegerField((LongField) sourceField);
                }

                if (sourceField instanceof DoubleField) {
                    return DoubleField.convertDoubleToBigIntegerField((DoubleField) sourceField);
                }

                if (sourceField instanceof StringField) {
                    return StringField.convertStringToBigIntegerField((StringField) sourceField);
                }

                if (sourceField instanceof DateMillisField) {
                    return LongField.convertLongToBigIntegerField(
                            DateMillisField.convertDateMillisToLongField((DateMillisField) sourceField));
                }

                if (sourceField instanceof DateNanosField) {
                    return LongField.convertLongToBigIntegerField(
                            DateNanosField.convertDateNanosToLongField((DateNanosField) sourceField));
                }

                if (sourceField instanceof BooleanField) {
                    return LongField.convertLongToBigIntegerField(
                            BooleanField.convertBooleanToLongField((BooleanField) sourceField));
                }

                throw new InvalidConversion(sourceField.getClass(), getFieldClass());
            }

            @Override
            public Class<BigIntegerField> getFieldClass() {
                return BigIntegerField.class;
            }

            @Override
            public Class<BigInteger> getTargetClass() {
                return BigInteger.class;
            }
        };
    }

    /* ---- Conversion Helpers To Other Fields ---- */

    public static LongField convertBigIntegerToLongField(BigIntegerField sourceField) {
        FieldValues<BigInteger> fv = sourceField.getFieldValues();
        return new LongField(sourceField.getName(), new DelegatingFieldValues<Long, BigInteger>(fv) {
            @Override
            public List<Long> getValues() {
                return values.getValues().stream().map(BigIntegerField::convertBigIntegerToLong).collect(Collectors.toList());
            }

            @Override
            public Long getNonPrimitiveValue() {
                return convertBigIntegerToLong(values.getNonPrimitiveValue());
            }

            @Override
            public long getLongValue() {
                return convertBigIntegerToLong(values.getNonPrimitiveValue());
            }

            @Override
            public double getDoubleValue() {
                return convertBigIntegerToDouble(values.getNonPrimitiveValue());
            }
        });
    }

    /* ---- Conversion Helpers To Other Types ---- */

    public static long convertBigIntegerToLong(BigInteger bigInteger) {
        return bigInteger.longValue();
    }

    public static double convertBigIntegerToDouble(BigInteger bigInteger) {
        return bigInteger.doubleValue();
    }

    /* ---- Big Integer Field Members ---- */

    public BigIntegerField(String name, FieldValues<BigInteger> values) {
        super(name, values);
    }
}
