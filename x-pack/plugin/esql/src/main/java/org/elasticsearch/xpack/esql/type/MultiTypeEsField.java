/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.InvalidMappedField;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * During IndexResolution it could occur that the same field is mapped to different types in different indices.
 * The class MultiTypeEfField.UnresolvedField holds that information and allows for later resolution of the field
 * to a single type during LogicalPlanOptimization.
 * If the plan contains conversion expressions for the different types, the resolution will be done using the conversion expressions,
 * in which case a MultiTypeEsField will be created to encapsulate the type resolution capabilities.
 * This class can be communicated to the data nodes and used during physical planning to influence field extraction so that
 * type conversion is done at the data node level.
 */
public class MultiTypeEsField extends EsField {
    private final Map<String, Expression> indexToConversionExpressions;

    public MultiTypeEsField(String name, DataType dataType, boolean aggregatable, Map<String, Expression> indexToConversionExpressions) {
        super(name, dataType, Map.of(), aggregatable);
        this.indexToConversionExpressions = indexToConversionExpressions;
    }

    public Map<String, Expression> getIndexToConversionExpressions() {
        return indexToConversionExpressions;
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        if (obj instanceof MultiTypeEsField other) {
            return super.equals(other) && indexToConversionExpressions.equals(other.indexToConversionExpressions);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), indexToConversionExpressions);
    }

    public Expression getConversionExpressionForIndex(String indexName) {
        return indexToConversionExpressions.get(indexName);
    }

    public static MultiTypeEsField resolveFrom(
        InvalidMappedField invalidMappedField,
        Map<String, Expression> typesToConversionExpressions
    ) {
        Map<String, Set<String>> typesToIndices = invalidMappedField.getTypesToIndices();
        DataType resolvedDataType = DataTypes.UNSUPPORTED;
        Map<String, Expression> indexToConversionExpressions = new HashMap<>();
        for (String typeName : typesToIndices.keySet()) {
            Set<String> indices = typesToIndices.get(typeName);
            Expression convertExpr = typesToConversionExpressions.get(typeName);
            if (resolvedDataType == DataTypes.UNSUPPORTED) {
                resolvedDataType = convertExpr.dataType();
            } else if (resolvedDataType != convertExpr.dataType()) {
                throw new IllegalArgumentException("Resolved data type mismatch: " + resolvedDataType + " != " + convertExpr.dataType());
            }
            for (String indexName : indices) {
                indexToConversionExpressions.put(indexName, convertExpr);
            }
        }
        return new MultiTypeEsField(
            invalidMappedField.getName(),
            resolvedDataType,
            invalidMappedField.isAggregatableIfMultiValuedResolved(),
            indexToConversionExpressions
        );
    }
}
