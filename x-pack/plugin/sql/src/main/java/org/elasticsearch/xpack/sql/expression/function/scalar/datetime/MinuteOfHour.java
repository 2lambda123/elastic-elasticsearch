/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo.NodeCtor2;

import java.time.OffsetTime;
import java.time.ZoneId;

import static org.elasticsearch.xpack.sql.expression.TypeResolutions.isDateOrTime;

/**
 * Exract the minute of the hour from a datetime.
 */
public class MinuteOfHour extends DateTimeFunction {
    public MinuteOfHour(Source source, Expression field, ZoneId zoneId) {
        super(source, field, zoneId, DateTimeExtractor.MINUTE_OF_HOUR);
    }

    @Override
    protected NodeCtor2<Expression, ZoneId, BaseDateTimeFunction> ctorForInfo() {
        return MinuteOfHour::new;
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
    protected MinuteOfHour replaceChild(Expression newChild) {
        return new MinuteOfHour(source(), newChild, zoneId());
    }

    @Override
    public String dateTimeFormat() {
        return "m";
    }
}
