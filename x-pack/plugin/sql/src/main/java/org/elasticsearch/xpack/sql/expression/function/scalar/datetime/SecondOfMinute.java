/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;
import org.elasticsearch.xpack.sql.tree.NodeInfo.NodeCtor2;
import org.elasticsearch.xpack.sql.tree.Source;

import java.time.OffsetTime;
import java.time.ZoneId;

import static org.elasticsearch.xpack.sql.expression.TypeResolutions.isDateOrTime;

/**
 * Extract the second of the minute from a datetime.
 */
public class SecondOfMinute extends DateTimeFunction {
    public SecondOfMinute(Source source, Expression field, ZoneId zoneId) {
        super(source, field, zoneId, DateTimeExtractor.SECOND_OF_MINUTE);
    }

    @Override
    protected NodeCtor2<Expression, ZoneId, BaseDateTimeFunction> ctorForInfo() {
        return SecondOfMinute::new;
    }

    @Override
    protected TypeResolution resolveType() {
        return isDateOrTime(field(), sourceText(), Expressions.ParamOrdinal.DEFAULT);
    }

    @Override
    protected Object doFold(OffsetTime time) {
        return extractor().extract(time);
    }

    @Override
    protected SecondOfMinute replaceChild(Expression newChild) {
        return new SecondOfMinute(source(), newChild, zoneId());
    }

    @Override
    public String dateTimeFormat() {
        return "s";
    }
}
