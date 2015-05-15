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

import com.google.common.collect.Lists;

import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class GeoPolygonQueryBuilder extends QueryBuilder {

    public static final String POINTS = GeoPolygonQueryParser.POINTS;
    
    private final String name;

    private final List<GeoPoint> shell = Lists.newArrayList();

    private String queryName;

    public GeoPolygonQueryBuilder(String name) {
        this.name = name;
    }

    /**
     * Adds a point with lat and lon
     *
     * @param lat The latitude
     * @param lon The longitude
     * @return
     */
    public GeoPolygonQueryBuilder addPoint(double lat, double lon) {
        return addPoint(new GeoPoint(lat, lon));
    }

    public GeoPolygonQueryBuilder addPoint(String geohash) {
        return addPoint(GeoHashUtils.decode(geohash));
    }

    public GeoPolygonQueryBuilder addPoint(GeoPoint point) {
        shell.add(point);
        return this;
    }
    
    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public GeoPolygonQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(GeoPolygonQueryParser.NAME);

        builder.startObject(name);
        builder.startArray(POINTS);
        for (GeoPoint point : shell) {
            builder.startArray().value(point.lon()).value(point.lat()).endArray();
        }
        builder.endArray();
        builder.endObject();

        if (queryName != null) {
            builder.field("_name", queryName);
        }

        builder.endObject();
    }
}
