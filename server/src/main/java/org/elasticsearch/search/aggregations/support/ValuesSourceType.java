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

package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.aggregations.AggregationExecutionException;

import java.io.IOException;
import java.util.Locale;

public enum ValuesSourceType implements Writeable {
    ANY {
        @Override
        public ValuesSource getEmpty() {
            throw new UnsupportedOperationException("ValuesSourceType.ANY is still a special case");
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            throw new UnsupportedOperationException("ValuesSourceType.ANY is still a special case");
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            throw new UnsupportedOperationException("ValuesSourceType.ANY is still a special case");
        }

        @Override
        public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing) {
            throw new UnsupportedOperationException("ValuesSourceType.ANY is still a special case");
        }
    },
    NUMERIC {
        @Override
        public ValuesSource getEmpty() {
            return ValuesSource.Numeric.EMPTY;
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            return new ValuesSource.Numeric.Script(script, scriptValueType);
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            if (!(fieldContext.indexFieldData() instanceof IndexNumericFieldData)) {
                throw new IllegalArgumentException("Expected numeric type on field [" + fieldContext.field() +
                    "], but got [" + fieldContext.fieldType().typeName() + "]");
            }

            ValuesSource.Numeric dataSource = new ValuesSource.Numeric.FieldData((IndexNumericFieldData)fieldContext.indexFieldData());
            if (script != null) {
                dataSource = new ValuesSource.Numeric.WithScript(dataSource, script);
            }
            return dataSource;
        }

        @Override
        public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing) {
            if (rawMissing instanceof Number == false) {
                throw new IllegalArgumentException("Can't apply missing value [" +
                    (rawMissing == null ? "null" : rawMissing.toString())
                    + "] to Numeric values source");
            }
            Number missing = (Number) rawMissing;
            return MissingValues.replaceMissing((ValuesSource.Numeric) valuesSource, missing);
        }
    },
    BYTES {
        @Override
        public ValuesSource getEmpty() {
            return ValuesSource.Bytes.WithOrdinals.EMPTY;
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            return new ValuesSource.Bytes.Script(script);
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            final IndexFieldData<?> indexFieldData = fieldContext.indexFieldData();
            ValuesSource dataSource;
            if (indexFieldData instanceof IndexOrdinalsFieldData) {
                dataSource = new ValuesSource.Bytes.WithOrdinals.FieldData((IndexOrdinalsFieldData) indexFieldData);
            } else {
                dataSource = new ValuesSource.Bytes.FieldData(indexFieldData);
            }
            if (script != null) {
                dataSource = new ValuesSource.WithScript(dataSource, script);
            }
            return dataSource;
        }

        @Override
        public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing) {
            if (rawMissing instanceof BytesRef == false) {
                throw new IllegalArgumentException("Can't apply missing value [" +
                    (rawMissing == null ? "null" : rawMissing.toString())
                    + "] to Bytes values source");
            }
            BytesRef missing = (BytesRef) rawMissing;
            if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals) {
                return MissingValues.replaceMissing((ValuesSource.Bytes.WithOrdinals) valuesSource, missing);
            } else {
                return MissingValues.replaceMissing((ValuesSource.Bytes) valuesSource, missing);
            }
        }
    },
    GEOPOINT {
        @Override
        public ValuesSource getEmpty() {
            return ValuesSource.GeoPoint.EMPTY;
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            throw new AggregationExecutionException("value source of type [" + this.name() + "] is not supported by scripts");
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            if (!(fieldContext.indexFieldData() instanceof IndexGeoPointFieldData)) {
                throw new IllegalArgumentException("Expected geo_point type on field [" + fieldContext.field() +
                    "], but got [" + fieldContext.fieldType().typeName() + "]");
            }
            return new ValuesSource.GeoPoint.Fielddata((IndexGeoPointFieldData) fieldContext.indexFieldData());
        }

        @Override
        public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing) {
            if (rawMissing instanceof GeoPoint == false) {
                throw new IllegalArgumentException("Can't apply missing value [" +
                    (rawMissing == null ? "null" : rawMissing.toString())
                    + "] to Geopoint values source");
            }
            GeoPoint missing = (GeoPoint) rawMissing;
            return MissingValues.replaceMissing((ValuesSource.GeoPoint) valuesSource, missing);
        }
    },
    RANGE {
        @Override
        public ValuesSource getEmpty() {
            throw new IllegalArgumentException("Can't deal with unmapped ValuesSource type range");
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            throw new AggregationExecutionException("value source of type [" + this.name() + "] is not supported by scripts");
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            MappedFieldType fieldType = fieldContext.fieldType();

            if (fieldType instanceof RangeFieldMapper.RangeFieldType == false) {
                throw new IllegalStateException("Asked for range ValuesSource, but field is of type " + fieldType.name());
            }
            RangeFieldMapper.RangeFieldType rangeFieldType = (RangeFieldMapper.RangeFieldType)fieldType;
            return new ValuesSource.Range(fieldContext.indexFieldData(), rangeFieldType.rangeType());
        }

        @Override
        public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing) {
            throw new IllegalArgumentException("Can't apply missing values on a Range values source");
        }
    };

    public static ValuesSourceType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static ValuesSourceType fromStream(StreamInput in) throws IOException {
        return in.readEnum(ValuesSourceType.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ValuesSourceType state = this;
        out.writeEnum(state);
    }

    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }

    public abstract ValuesSource getEmpty();

    public abstract ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType);

    public abstract ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script);

    public abstract ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing);
}
