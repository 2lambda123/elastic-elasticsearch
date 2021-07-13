/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile.feature;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.adapt.jts.UserDataIgnoreConverter;

import org.apache.lucene.geo.GeoTestUtil;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.List;

public class FeatureFactoryTests extends ESTestCase {

    public void testPoint() {
        int z = randomIntBetween(1, 10);
        int x = randomIntBetween(0, (1 << z) - 1);
        int y = randomIntBetween(0, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        // check if we might have numerical error due to floating point arithmetic
        assumeFalse("", hasNumericalError(z, x, y, extent));
        Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, z);
        SimpleFeatureFactory builder = new SimpleFeatureFactory(z, x, y, extent);
        FeatureFactory factory = new FeatureFactory(z, x, y, extent);
        VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
        for (int i = 0; i < 10; i++) {
            featureBuilder.clear();
            double lat = randomValueOtherThanMany((l) -> rectangle.getMinY() > l || rectangle.getMaxY() < l, GeoTestUtil::nextLatitude);
            double lon = randomValueOtherThanMany((l) -> rectangle.getMinX() > l || rectangle.getMaxX() < l, GeoTestUtil::nextLongitude);
            builder.point(featureBuilder, lon, lat);
            byte[] b1 = featureBuilder.build().toByteArray();
            Point point = new Point(lon, lat);
            List<VectorTile.Tile.Feature> features = factory.getFeatures(point, new UserDataIgnoreConverter());
            assertThat(features.size(), Matchers.equalTo(1));
            byte[] b2 = features.get(0).toByteArray();
            assertArrayEquals(b1, b2);
        }
    }

    public void testIssue74341() {
        int z = 1;
        int x = 0;
        int y = 0;
        int extent = 1730;
        // this is the typical case we need to guard from.
        assertThat(hasNumericalError(z, x, y, extent), Matchers.equalTo(true));
        double lon = -171.0;
        double lat = 0.9999999403953552;
        SimpleFeatureFactory builder = new SimpleFeatureFactory(z, x, y, extent);
        FeatureFactory factory = new FeatureFactory(z, x, y, extent);
        VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
        builder.point(featureBuilder, lon, lat);
        byte[] b1 = featureBuilder.build().toByteArray();
        Point point = new Point(lon, lat);
        List<VectorTile.Tile.Feature> features = factory.getFeatures(point, new UserDataIgnoreConverter());
        assertThat(features.size(), Matchers.equalTo(1));
        byte[] b2 = features.get(0).toByteArray();
        assertThat(Arrays.equals(b1, b2), Matchers.equalTo(false));
    }

    private boolean hasNumericalError(int z, int x, int y, int extent) {
        final Rectangle rectangle = FeatureFactoryUtils.getTileBounds(z, x, y);
        final double xDiff = rectangle.getMaxLon() - rectangle.getMinLon();
        final double yDiff = rectangle.getMaxLat() - rectangle.getMinLat();
        return (double) -extent / yDiff != -1d / (yDiff / (double) extent) || (double) extent / xDiff != 1d / (xDiff / (double) extent);
    }

    public void testRectangle() {
        int z = randomIntBetween(1, 10);
        int x = randomIntBetween(0, (1 << z) - 1);
        int y = randomIntBetween(0, (1 << z) - 1);
        int extent = randomIntBetween(1 << 8, 1 << 14);
        SimpleFeatureFactory builder = new SimpleFeatureFactory(z, x, y, extent);
        FeatureFactory factory = new FeatureFactory(z, x, y, extent);
        Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);
        VectorTile.Tile.Feature.Builder featureBuilder = VectorTile.Tile.Feature.newBuilder();
        for (int i = 0; i < extent; i++) {
            featureBuilder.clear();
            builder.box(featureBuilder, r.getMinLon(), r.getMaxLon(), r.getMinLat(), r.getMaxLat());
            byte[] b1 = featureBuilder.build().toByteArray();
            List<VectorTile.Tile.Feature> features = factory.getFeatures(r, new UserDataIgnoreConverter());
            assertThat(features.size(), Matchers.equalTo(1));
            byte[] b2 = features.get(0).toByteArray();
            assertArrayEquals(extent + "", b1, b2);
        }
    }
}
