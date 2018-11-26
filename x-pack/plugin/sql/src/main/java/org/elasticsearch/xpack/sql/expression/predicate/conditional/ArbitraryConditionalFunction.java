/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.expression.predicate.conditional.ConditionalProcessor.ConditionalOperation;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataTypeConversion;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * Base class for conditional predicates with arbitrary number of arguments
 */
public abstract class ArbitraryConditionalFunction extends ConditionalFunction {

    private ConditionalOperation operation;

    ArbitraryConditionalFunction(Location location, List<Expression> fields, ConditionalOperation operation) {
        super(location, fields);
        this.operation = operation;
    }

    @Override
    protected TypeResolution resolveType() {
        for (Expression e : children()) {
            dataType = DataTypeConversion.commonType(dataType, e.dataType());
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    protected Pipe makePipe() {
        return new ConditionalPipe(location(), this, Expressions.pipe(children()), operation);
    }

    @Override
    public ScriptTemplate asScript() {
        List<ScriptTemplate> templates = new ArrayList<>();
        for (Expression ex : children()) {
            templates.add(asScript(ex));
        }

        StringJoiner template = new StringJoiner(",", "{sql}." + operation.scriptMethodName() +"([", "])");
        ParamsBuilder params = paramsBuilder();

        for (ScriptTemplate scriptTemplate : templates) {
            template.add(scriptTemplate.template());
            params.script(scriptTemplate.params());
        }

        return new ScriptTemplate(template.toString(), params.build(), dataType());
    }
}
