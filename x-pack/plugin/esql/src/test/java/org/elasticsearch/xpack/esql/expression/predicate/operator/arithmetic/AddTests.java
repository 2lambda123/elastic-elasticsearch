/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isDateTimeOrTemporal;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isTemporalAmount;
import static org.elasticsearch.xpack.ql.type.DataTypes.isDateTime;
import static org.elasticsearch.xpack.ql.type.DateUtils.asDateTime;
import static org.elasticsearch.xpack.ql.type.DateUtils.asMillis;
import static org.elasticsearch.xpack.ql.util.NumericUtils.asLongUnsigned;
import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongAsBigInteger;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class AddTests extends AbstractDateTimeArithmeticTestCase {
    public AddTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.addAll(
            TestCaseSupplier.forBinaryNumericNotCasting(
                "AddIntsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> l.intValue() + r.intValue(),
                DataTypes.INTEGER,
                TestCaseSupplier.intCases((Integer.MIN_VALUE >> 1) - 1, (Integer.MAX_VALUE >> 1) - 1),
                TestCaseSupplier.intCases((Integer.MIN_VALUE >> 1) - 1, (Integer.MAX_VALUE >> 1) - 1),
                List.of(),
                false)
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNumericNotCasting(
                "AddLongsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> l.longValue() + r.longValue(),
                DataTypes.LONG,
                TestCaseSupplier.longCases((Long.MIN_VALUE >> 1) - 1, (Long.MAX_VALUE >> 1) - 1),
                TestCaseSupplier.longCases((Long.MIN_VALUE >> 1) - 1, (Long.MAX_VALUE >> 1) - 1),
                List.of(),
                false)
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNumericNotCasting(
                "AddDoublesEvaluator",
                "lhs",
                "rhs",
                (l, r) -> l.doubleValue() + r.doubleValue(),
                DataTypes.DOUBLE,
                TestCaseSupplier.doubleCases(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY),
                TestCaseSupplier.doubleCases(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY),
                List.of(),
                false)
        );
        suppliers.addAll(
            TestCaseSupplier.forBinaryNumericNotCasting(
                "AddUnsignedLongsEvaluator",
                "lhs",
                "rhs",
                (l, r) -> {
                    assert l instanceof BigInteger;
                    assert r instanceof BigInteger;
                    return ((BigInteger)l).add((BigInteger) r);
                },
                DataTypes.UNSIGNED_LONG,
                // TODO: we should be able to test values over Long.MAX_VALUE too...
                TestCaseSupplier.ulongCases(BigInteger.ONE, BigInteger.valueOf(Long.MAX_VALUE)),
                TestCaseSupplier.ulongCases(BigInteger.ONE, BigInteger.valueOf(Long.MAX_VALUE)),
                List.of(),
                false)
        );
        suppliers.addAll(List.of(/*, new TestCaseSupplier("ULong + ULong", () -> {
            // Ensure we don't have an overflow
            // TODO: we should be able to test values over Long.MAX_VALUE too...
            long rhs = randomLongBetween(0, (Long.MAX_VALUE >> 1) - 1);
            long lhs = randomLongBetween(0, (Long.MAX_VALUE >> 1) - 1);
            BigInteger lhsBI = unsignedLongAsBigInteger(lhs);
            BigInteger rhsBI = unsignedLongAsBigInteger(rhs);
            return new TestCase(
                Source.EMPTY,
                List.of(new TypedData(lhs, DataTypes.UNSIGNED_LONG, "lhs"), new TypedData(rhs, DataTypes.UNSIGNED_LONG, "rhs")),
                "AddUnsignedLongsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                equalTo(asLongUnsigned(lhsBI.add(rhsBI).longValue()))
            );
          }) */ new TestCaseSupplier("Datetime + Period", () -> {
            long lhs = (Long) randomLiteral(DataTypes.DATETIME).value();
            Period rhs = (Period) randomLiteral(EsqlDataTypes.DATE_PERIOD).value();
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataTypes.DATETIME, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, EsqlDataTypes.DATE_PERIOD, "rhs")
                ),
                // TODO: There is an evaluator for Datetime + Period, so it should be tested. Similarly below.
                "No evaluator, the tests only trigger the folding code since Period is not representable",
                DataTypes.DATETIME,
                equalTo(asMillis(asDateTime(lhs).plus(rhs)))
            );
        }), new TestCaseSupplier("Period + Datetime", () -> {
            Period lhs = (Period) randomLiteral(EsqlDataTypes.DATE_PERIOD).value();
            long rhs = (Long) randomLiteral(DataTypes.DATETIME).value();
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, EsqlDataTypes.DATE_PERIOD, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataTypes.DATETIME, "rhs")
                ),
                "No evaluator, the tests only trigger the folding code since Period is not representable",
                DataTypes.DATETIME,
                equalTo(asMillis(asDateTime(rhs).plus(lhs)))
            );
        }), new TestCaseSupplier("Period + Period", () -> {
            Period lhs = (Period) randomLiteral(EsqlDataTypes.DATE_PERIOD).value();
            Period rhs = (Period) randomLiteral(EsqlDataTypes.DATE_PERIOD).value();
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, EsqlDataTypes.DATE_PERIOD, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, EsqlDataTypes.DATE_PERIOD, "rhs")
                ),
                "Only folding possible, so there's no evaluator",
                EsqlDataTypes.DATE_PERIOD,
                equalTo(lhs.plus(rhs))
            );
        }), new TestCaseSupplier("Datetime + Duration", () -> {
            long lhs = (Long) randomLiteral(DataTypes.DATETIME).value();
            Duration rhs = (Duration) randomLiteral(EsqlDataTypes.TIME_DURATION).value();
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataTypes.DATETIME, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, EsqlDataTypes.TIME_DURATION, "rhs")
                ),
                "No evaluator, the tests only trigger the folding code since Duration is not representable",
                DataTypes.DATETIME,
                equalTo(asMillis(asDateTime(lhs).plus(rhs)))
            );
        }), new TestCaseSupplier("Duration + Datetime", () -> {
            long lhs = (Long) randomLiteral(DataTypes.DATETIME).value();
            Duration rhs = (Duration) randomLiteral(EsqlDataTypes.TIME_DURATION).value();
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, DataTypes.DATETIME, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, EsqlDataTypes.TIME_DURATION, "rhs")
                ),
                "No evaluator, the tests only trigger the folding code since Duration is not representable",
                DataTypes.DATETIME,
                equalTo(asMillis(asDateTime(lhs).plus(rhs)))
            );
        }), new TestCaseSupplier("Duration + Duration", () -> {
            Duration lhs = (Duration) randomLiteral(EsqlDataTypes.TIME_DURATION).value();
            Duration rhs = (Duration) randomLiteral(EsqlDataTypes.TIME_DURATION).value();
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(lhs, EsqlDataTypes.TIME_DURATION, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, EsqlDataTypes.TIME_DURATION, "rhs")
                ),
                "Only folding possible, so there's no evaluator",
                EsqlDataTypes.TIME_DURATION,
                equalTo(lhs.plus(rhs))
            );
        }), new TestCaseSupplier("MV", () -> {
            // Ensure we don't have an overflow
            int rhs = randomIntBetween((Integer.MIN_VALUE >> 1) - 1, (Integer.MAX_VALUE >> 1) - 1);
            int lhs = randomIntBetween((Integer.MIN_VALUE >> 1) - 1, (Integer.MAX_VALUE >> 1) - 1);
            int lhs2 = randomIntBetween((Integer.MIN_VALUE >> 1) - 1, (Integer.MAX_VALUE >> 1) - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(List.of(lhs, lhs2), DataTypes.INTEGER, "lhs"),
                    new TestCaseSupplier.TypedData(rhs, DataTypes.INTEGER, "rhs")
                ),
                "AddIntsEvaluator[lhs=Attribute[channel=0], rhs=Attribute[channel=1]]",
                DataTypes.INTEGER,
                is(nullValue())
            );
        })));
        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected boolean supportsTypes(DataType lhsType, DataType rhsType) {
        if (isDateTimeOrTemporal(lhsType) || isDateTimeOrTemporal(rhsType)) {
            return isDateTime(lhsType) && isTemporalAmount(rhsType) || isTemporalAmount(lhsType) && isDateTime(rhsType);
        }
        return super.supportsTypes(lhsType, rhsType);
    }

    @Override
    protected Add build(Source source, Expression lhs, Expression rhs) {
        return new Add(source, lhs, rhs);
    }

    @Override
    protected double expectedValue(double lhs, double rhs) {
        return lhs + rhs;
    }

    @Override
    protected int expectedValue(int lhs, int rhs) {
        return lhs + rhs;
    }

    @Override
    protected long expectedValue(long lhs, long rhs) {
        return lhs + rhs;
    }

    @Override
    protected long expectedUnsignedLongValue(long lhs, long rhs) {
        BigInteger lhsBI = unsignedLongAsBigInteger(lhs);
        BigInteger rhsBI = unsignedLongAsBigInteger(rhs);
        return asLongUnsigned(lhsBI.add(rhsBI).longValue());
    }

    @Override
    protected long expectedValue(long datetime, TemporalAmount temporalAmount) {
        return asMillis(asDateTime(datetime).plus(temporalAmount));
    }
}
