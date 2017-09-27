/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.composite;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.DocValueFormat;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A key that is composed of multiple {@link Comparable} values.
 */
class CompositeKey {
    private final Comparable<?>[] values;

    CompositeKey(Comparable<?>... values) {
        this.values = values;
    }

    Comparable<?>[] values() {
        return values;
    }

    int size() {
        return values.length;
    }

    Comparable<?> get(int pos) {
        assert pos < values.length;
        return values[pos];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompositeKey that = (CompositeKey) o;
        return Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    static String formatValue(Object value, DocValueFormat formatter) {
        if (value.getClass() == Long.class || value.getClass() == Integer.class) {
            return formatter.format(((Number) value).longValue());
        } if (value.getClass() == Double.class || value.getClass() == Float.class) {
            return formatter.format(((Number) value).doubleValue());
        } else if (value.getClass() == BytesRef.class) {
            return formatter.format((BytesRef) value);
        } else {
            return value.toString();
        }
    }
}
