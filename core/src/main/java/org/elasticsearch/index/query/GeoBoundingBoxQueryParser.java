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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder.Type;

import java.io.IOException;

public class GeoBoundingBoxQueryParser extends BaseQueryParser {

    public static final String NAME = "geo_bbox";

    /** Key to refer to the top of the bounding box. */
    public static final String TOP = "top";
    /** Key to refer to the left of the bounding box. */
    public static final String LEFT = "left";
    /** Key to refer to the right of the bounding box. */
    public static final String RIGHT = "right";
    /** Key to refer to the bottom of the bounding box. */
    public static final String BOTTOM = "bottom";

    /** Key to refer to top_left corner of bounding box. */
    public static final String TOP_LEFT = TOP + "_" + LEFT;
    /** Key to refer to bottom_right corner of bounding box. */
    public static final String BOTTOM_RIGHT = BOTTOM + "_" + RIGHT;
    /** Key to refer to top_right corner of bounding box. */
    public static final String TOP_RIGHT = TOP + "_" + RIGHT;
    /** Key to refer to bottom left corner of bounding box.  */
    public static final String BOTTOM_LEFT = BOTTOM + "_" + LEFT;

    /** Key to refer to top_left corner of bounding box. */
    public static final String TOPLEFT = "topLeft";
    /** Key to refer to bottom_right corner of bounding box. */
    public static final String BOTTOMRIGHT = "bottomRight";
    /** Key to refer to top_right corner of bounding box. */
    public static final String TOPRIGHT = "topRight";
    /** Key to refer to bottom left corner of bounding box.  */
    public static final String BOTTOMLEFT = "bottomLeft";

    public static final String FIELD = "field";

    @Inject
    public GeoBoundingBoxQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{GeoBoundingBoxQueryBuilder.NAME, "geoBbox", "geo_bounding_box", "geoBoundingBox"};
    }

    @Override
    public QueryBuilder<GeoBoundingBoxQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        String fieldName = null;

        double top = Double.NaN;
        double bottom = Double.NaN;
        double left = Double.NaN;
        double right = Double.NaN;

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        boolean coerce = GeoBoundingBoxQueryBuilder.DEFAULT_COERCE;
        boolean ignoreMalformed = GeoBoundingBoxQueryBuilder.DEFAULT_IGNORE_MALFORMED;

        GeoPoint sparse = new GeoPoint();

        String type = "memory";

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;

                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                        token = parser.nextToken();
                        if (parseContext.isDeprecatedSetting(currentFieldName)) {
                            // skip
                        } else if (FIELD.equals(currentFieldName)) {
                            fieldName = parser.text();
                        } else if (TOP.equals(currentFieldName)) {
                            top = parser.doubleValue();
                        } else if (BOTTOM.equals(currentFieldName)) {
                            bottom = parser.doubleValue();
                        } else if (LEFT.equals(currentFieldName)) {
                            left = parser.doubleValue();
                        } else if (RIGHT.equals(currentFieldName)) {
                            right = parser.doubleValue();
                        } else {
                            if (TOP_LEFT.equals(currentFieldName) || TOPLEFT.equals(currentFieldName)) {
                                GeoUtils.parseGeoPoint(parser, sparse);
                                top = sparse.getLat();
                                left = sparse.getLon();
                            } else if (BOTTOM_RIGHT.equals(currentFieldName) || BOTTOMRIGHT.equals(currentFieldName)) {
                                GeoUtils.parseGeoPoint(parser, sparse);
                                bottom = sparse.getLat();
                                right = sparse.getLon();
                            } else if (TOP_RIGHT.equals(currentFieldName) || TOPRIGHT.equals(currentFieldName)) {
                                GeoUtils.parseGeoPoint(parser, sparse);
                                top = sparse.getLat();
                                right = sparse.getLon();
                            } else if (BOTTOM_LEFT.equals(currentFieldName) || BOTTOMLEFT.equals(currentFieldName)) {
                                GeoUtils.parseGeoPoint(parser, sparse);
                                bottom = sparse.getLat();
                                left = sparse.getLon();
                            } else {
                                throw new ElasticsearchParseException("failed to parse [{}] query. unexpected field [{}]", NAME, currentFieldName);
                            }
                        }
                    } else {
                        throw new ElasticsearchParseException("failed to parse [{}] query. field name expected but [{}] found", NAME, token);
                    }
                }
            } else if (token.isValue()) {
                if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("coerce".equals(currentFieldName) || ("normalize".equals(currentFieldName))) {
                    coerce = parser.booleanValue();
                    if (coerce) {
                        ignoreMalformed = true;
                    }
                } else if ("type".equals(currentFieldName)) {
                    type = parser.text();
                } else if ("ignore_malformed".equals(currentFieldName)) {
                    ignoreMalformed = parser.booleanValue();
                } else {
                    throw new QueryParsingException(parseContext, "failed to parse [{}] query. unexpected field [{}]", NAME, currentFieldName);
                }
            }
        }

        final GeoPoint topLeft = sparse.reset(top, left);  //just keep the object
        final GeoPoint bottomRight = new GeoPoint(bottom, right);

        GeoBoundingBoxQueryBuilder builder = new GeoBoundingBoxQueryBuilder(fieldName);
        builder.topLeft(topLeft);
        builder.bottomRight(bottomRight);
        builder.queryName(queryName);
        builder.boost(boost);
        builder.type(Type.fromString(type));
        builder.coerce(coerce);
        builder.ignoreMalformed(ignoreMalformed);
        return builder;
    }

    @Override
    public GeoBoundingBoxQueryBuilder getBuilderPrototype() {
        return GeoBoundingBoxQueryBuilder.PROTOTYPE;
    }
}
