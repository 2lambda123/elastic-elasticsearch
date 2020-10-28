/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.parser;

import org.antlr.v4.runtime.Token;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.SingleStatementContext;

import java.time.ZoneId;
import java.util.Map;

class AstBuilder extends CommandBuilder {
    /**
     * Create AST Builder
     * @param params a map between '?' tokens that represent parameters
     *               and the parameter indexes and values
     * @param zoneId user specified timezone in the session
     */
    AstBuilder(Map<Token, SqlParser.SqlParameter> params, ZoneId zoneId) {
        super(params, zoneId);
    }

    @Override
    public LogicalPlan visitSingleStatement(SingleStatementContext ctx) {
        return plan(ctx.statement());
    }
}
