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

package org.elasticsearch.index.query;

import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.unit.TimeValue;

import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.GeoPointFieldMapper.GeoPointFieldType;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A query to boost scores based on their proximity to the given origin
 * for date, date_nanos and geo_point field types
 */
public class DistanceFeatureQueryBuilder extends AbstractQueryBuilder<DistanceFeatureQueryBuilder> {
    public static final String NAME = "distance_feature";

    private static final ParseField FIELD_FIELD = new ParseField("field");
    private static final ParseField ORIGIN_FIELD = new ParseField("origin");
    private static final ParseField PIVOT_FIELD = new ParseField("pivot");

    private final String field;
    private final Origin origin;
    private final String pivot;

    private static final ConstructingObjectParser<DistanceFeatureQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
        "distance_feature", false,
        args -> new DistanceFeatureQueryBuilder((String) args[0], (Origin) args[1], (String) args[2])
    );

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        // origin: number or string for date and date_nanos fields; string, array, object for geo fields
        PARSER.declareField(constructorArg(), DistanceFeatureQueryBuilder.Origin::originFromXContent,
            ORIGIN_FIELD, ObjectParser.ValueType.OBJECT_ARRAY_STRING_OR_NUMBER);
        PARSER.declareString(constructorArg(), PIVOT_FIELD);
        declareStandardFields(PARSER);
    }

    public DistanceFeatureQueryBuilder(String field, Origin origin, String pivot) {
        this.field = Objects.requireNonNull(field);
        this.origin = Objects.requireNonNull(origin);
        this.pivot = Objects.requireNonNull(pivot);
    }

    public static DistanceFeatureQueryBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(FIELD_FIELD.getPreferredName(), field);
        builder.field(ORIGIN_FIELD.getPreferredName(), origin.origin);
        builder.field(PIVOT_FIELD.getPreferredName(), pivot);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public DistanceFeatureQueryBuilder(StreamInput in) throws IOException {
        super(in);
        field = in.readString();
        origin = new Origin(in);
        pivot = in.readString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        origin.writeTo(out);
        out.writeString(pivot);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        MappedFieldType fieldType = context.fieldMapper(field);
        if (fieldType == null) {
            throw new IllegalArgumentException("Can't run [" + NAME + "] query on unmapped fields!");
        }
        Object originObj = origin.origin();
        if (fieldType instanceof DateFieldType) {
            long originLong = (originObj instanceof Long) ? (Long) originObj :
                ((DateFieldType) fieldType).parseToLong(originObj, true, null, null, context);
            TimeValue pivotVal = TimeValue.parseTimeValue(pivot, TimeValue.timeValueHours(24),
                DistanceFeatureQueryBuilder.class.getSimpleName() + ".pivot");
            if (((DateFieldType) fieldType).resolution() == DateFieldMapper.Resolution.MILLISECONDS) {
                return LongPoint.newDistanceFeatureQuery(field, boost, originLong, pivotVal.getMillis());
            } else { // NANOSECONDS
                return LongPoint.newDistanceFeatureQuery(field, boost, originLong, pivotVal.getNanos());
            }
        } else if (fieldType instanceof GeoPointFieldType) {
            GeoPoint originGeoPoint = (originObj instanceof GeoPoint)? (GeoPoint) originObj : GeoUtils.parseFromString((String) originObj);
            double pivotDouble = DistanceUnit.DEFAULT.parse(pivot, DistanceUnit.DEFAULT);
            return LatLonPoint.newDistanceFeatureQuery(field, boost, originGeoPoint.lat(), originGeoPoint.lon(), pivotDouble);
        }
        throw new IllegalArgumentException(
            "Illegal data type! ["+ NAME + "] query can only be run on a date, date_nanos or geo_point field type!");
    }

    public String fieldName() {
        return field;
    }

    public Origin origin() {
        return origin;
    }

    public String pivot() {
        return pivot;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, origin, pivot);
    }

    @Override
    protected boolean doEquals(DistanceFeatureQueryBuilder other) {
        return this.field.equals(other.field) && Objects.equals(this.origin, other.origin) && this.pivot.equals(other.pivot);
    }

    public static class Origin {
        private final Object origin;
        public Origin(Object origin) {
            if ((origin instanceof Long) || (origin instanceof GeoPoint) || (origin instanceof String)) {
                this.origin = origin;
            } else {
                throw new IllegalArgumentException("Illegal type for [origin]! Must be of type [long] or [string] for " +
                    "date and date_nanos origins," + "[geo_point] or [string] for geo_point origins!");
            }
        }

        private static Origin originFromXContent(XContentParser parser) throws IOException {
            if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return new Origin(parser.longValue());
            } else if(parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                return new Origin(parser.text());
            } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                return new Origin(GeoUtils.parseGeoPoint(parser));
            } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                return new Origin(GeoUtils.parseGeoPoint(parser));
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                    "Illegal type while parsing [origin]! Must be [number] or [string] for date and date_nanos origins;" +
                    " or [string], [array], [object] for geo_point origins!");
            }
        }

        private Origin(StreamInput in) throws IOException {
            origin = in.readGenericValue();
        }

        private void writeTo(final StreamOutput out) throws IOException {
            out.writeGenericValue(origin);
        }

        Object origin() {
            return origin;
        }

        @Override
        public final boolean equals(Object other) {
            if ((other instanceof Origin) == false) return false;
            Object otherOrigin = ((Origin) other).origin();
            return this.origin().equals(otherOrigin);
        }

        @Override
        public int hashCode() {
            return Objects.hash(origin);
        }
    }
}
