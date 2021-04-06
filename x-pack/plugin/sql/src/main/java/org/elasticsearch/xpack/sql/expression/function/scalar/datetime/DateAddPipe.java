/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;

public class DateAddPipe extends ThreeArgsDateTimePipe {

    public DateAddPipe(Source source, Expression expression, Pipe first, Pipe second, Pipe third, ZoneId zoneId) {
        super(source, expression, first, second, third, zoneId);
    }

    @Override
    protected NodeInfo<DateAddPipe> info() {
        return NodeInfo.create(this, DateAddPipe::new, expression(), first(), second(), third(), zoneId());
    }

    @Override
    public ThreeArgsDateTimePipe replaceChildren(Pipe newFirst, Pipe newSecond, Pipe newThird) {
        return new DateAddPipe(source(), expression(), newFirst, newSecond, newThird, zoneId());
    }

    @Override
    protected Processor makeProcessor(Processor first, Processor second, Processor third, ZoneId zoneId) {
        return new DateAddProcessor(first, second, third, zoneId);
    }
}
