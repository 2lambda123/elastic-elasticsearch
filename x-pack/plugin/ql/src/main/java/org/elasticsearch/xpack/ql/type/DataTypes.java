/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.type;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;

import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableMap;

public final class DataTypes {

    public static final DataType UNSUPPORTED = new DataType(null, 0, false, false, false);

    public static final DataType NULL = new DataType("null", 0, false, false, false);

    public static final DataType BOOLEAN = new DataType("boolean", 1, false, false, false);
    // integer numeric
    public static final DataType BYTE = new DataType("byte", Byte.BYTES, true, false, true);
    public static final DataType SHORT = new DataType("short", Short.BYTES, true, false, true);
    public static final DataType INTEGER = new DataType("integer", Integer.BYTES, true, false, true);
    public static final DataType LONG = new DataType("long", Long.BYTES, true, false, true);
    // decimal numeric
    public static final DataType DOUBLE = new DataType("double", Double.BYTES, false, true, true);
    public static final DataType FLOAT = new DataType("float", Float.BYTES, false, true, true);
    public static final DataType HALF_FLOAT = new DataType("half_float", Float.BYTES, false, true, true);
    public static final DataType SCALED_FLOAT = new DataType("scaled_float", Long.BYTES, false, true, true);
    // string
    public static final DataType KEYWORD = new DataType("keyword", Short.MAX_VALUE, true, false, true);
    public static final DataType TEXT = new DataType("text", Integer.MAX_VALUE, true, false, true);
    // date
    public static final DataType DATETIME = new DataType("date", Long.BYTES, false, false, true);
    // ip
    public static final DataType IP = new DataType("ip", 39, false, false, true);
    // binary
    public static final DataType BINARY = new DataType("binary", Integer.MAX_VALUE, false, false, true);
    // complex types
    public static final DataType OBJECT = new DataType("object", 0, false, false, true);
    public static final DataType NESTED = new DataType("nested", 0, false, false, true);

    
    public static final Collection<DataType> TYPES = Arrays.asList(
            UNSUPPORTED,
            NULL,
            BOOLEAN,
            BYTE,
            SHORT,
            INTEGER,
            LONG,
            DOUBLE,
            FLOAT,
            HALF_FLOAT,
            SCALED_FLOAT,
            KEYWORD,
            TEXT,
            DATETIME,
            IP,
            BINARY,
            OBJECT,
            NESTED)
            .stream()
            .sorted(Comparator.comparing(DataType::typeName))
            .collect(toUnmodifiableList());
    
    private static final Map<String, DataType> ES_TO_TYPE = TYPES.stream()
            .filter(e -> e.esType() != null)
            .collect(toUnmodifiableMap(DataType::esType, t -> t));
    
    private DataTypes() {}

    public static boolean isUnsupported(DataType from) {
        return from == UNSUPPORTED;
    }

    public static boolean isString(DataType t) {
        return t == KEYWORD || t == TEXT;
    }

    public static boolean isPrimitive(DataType t) {
        return t != OBJECT && t != NESTED;
    }

    public static boolean isNull(DataType t) {
        return t == NULL;
    }

    public static boolean isSigned(DataType t) {
        return t.isNumeric();
    }

    public static DataType fromEs(String name) {
        return ES_TO_TYPE.get(name);
    }

    public static DataType fromJava(Object value) {
        if (value == null) {
            return NULL;
        }
        if (value instanceof Integer) {
            return INTEGER;
        }
        if (value instanceof Long) {
            return LONG;
        }
        if (value instanceof Boolean) {
            return BOOLEAN;
        }
        if (value instanceof Double) {
            return DOUBLE;
        }
        if (value instanceof Float) {
            return FLOAT;
        }
        if (value instanceof Byte) {
            return BYTE;
        }
        if (value instanceof Short) {
            return SHORT;
        }
        if (value instanceof ZonedDateTime) {
            return DATETIME;
        }
        if (value instanceof String || value instanceof Character) {
            return KEYWORD;
        }

        return null;
    }
}