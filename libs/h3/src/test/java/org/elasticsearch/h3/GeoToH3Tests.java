/*
 * Based on the h3 project by Uber (@uber)
 * https://github.com/uber/h3
 * Licensed to Elasticsearch B.V under the Apache 2.0 License.
 * Elasticsearch B.V licenses this file, including any modifications, to you under the Apache 2.0 License.
 * See the LICENSE file in the project root for more information.
 */

package org.elasticsearch.h3;

import org.apache.lucene.geo.GeoTestUtil;
import org.elasticsearch.test.ESTestCase;

public class GeoToH3Tests extends ESTestCase {

    public void testRandomPoints() {
        for (int i = 0; i < 50; i++) {
            // avoid points close to the poles
            double lat = randomValueOtherThanMany(d -> d > 60 || d < -60, GeoTestUtil::nextLatitude);
            // avoid points close to the dateline
            double lon = randomValueOtherThanMany(d -> d > 150 || d < -150, GeoTestUtil::nextLongitude);
            testPoint(lat, lon);
        }
    }

    private void testPoint(double lat, double lon) {
        for (int i = 0; i < Constants.MAX_H3_RES; i++) {
            String h3Address = H3.geoToH3Address(lat, lon, i);
            CellBoundary cellBoundary = H3.h3ToGeoBoundary(h3Address);
            double minLat = cellBoundary.getLatLon(0).getLatDeg();
            double maxLat = cellBoundary.getLatLon(0).getLatDeg();
            double minLon = cellBoundary.getLatLon(0).getLonDeg();
            double maxLon = cellBoundary.getLatLon(0).getLonDeg();
            for (int j = 0; j < cellBoundary.numPoints(); j++) {
                minLat = Math.min(minLat, cellBoundary.getLatLon(j).getLatDeg());
                maxLat = Math.max(maxLat, cellBoundary.getLatLon(j).getLatDeg());
                minLon = Math.min(minLon, cellBoundary.getLatLon(j).getLonDeg());
                maxLon = Math.max(maxLon, cellBoundary.getLatLon(j).getLonDeg());
            }
            assertTrue(minLat <= lat);
            assertTrue(maxLat >= lat);
            assertTrue(minLon <= lon);
            assertTrue(maxLon >= lon);
        }
    }

}
