/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import java.util.List;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

/**
 * Find the minimum value in matched documents.
 */
public class Min extends NumericAggregate implements EnclosedAgg {

    public Min(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<Min> info() {
        return NodeInfo.create(this, Min::new, field());
    }

    @Override
    public Min replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 1) {
            throw new IllegalArgumentException("expected [1] child but received [" + newChildren.size() + "]");
        }
        return new Min(location(), newChildren.get(0));
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    public String innerName() {
        return "min";
    }

    @Override
    protected TypeResolution resolveType() {
        return Expressions.typeMustBe(
            field(),
            e -> e.dataType().isNumeric() || e.dataType() == DataType.DATE,
            Expressions.getErrorMessageForNumericRequirement(field()));
    }
}
