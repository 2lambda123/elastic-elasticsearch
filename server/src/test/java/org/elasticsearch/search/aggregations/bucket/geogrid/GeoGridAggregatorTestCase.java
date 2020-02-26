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
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.geo.CentroidCalculator;
import org.elasticsearch.common.geo.GeoRelation;
import org.elasticsearch.common.geo.GeoShapeCoordinateEncoder;
import org.elasticsearch.common.geo.GeoTestUtils;
import org.elasticsearch.common.geo.TriangleTreeReader;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoBoundingBoxTests;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.BinaryGeoShapeDocValuesField;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.common.geo.GeoTestUtils.triangleTreeReader;
import static org.hamcrest.Matchers.equalTo;

public abstract class GeoGridAggregatorTestCase<T extends InternalGeoGridBucket> extends AggregatorTestCase {

    private static final String FIELD_NAME = "location";

    /**
     * Generate a random precision according to the rules of the given aggregation.
     */
    protected abstract int randomPrecision();

    /**
     * Convert geo point into a hash string (bucket string ID)
     */
    protected abstract String hashAsString(double lng, double lat, int precision);

    /**
     * Return a point within the bounds of the tile grid
     */
    protected abstract Point randomPoint();

    /**
     * Return the bounding tile as a {@link Rectangle} for a given point
     */
    protected abstract Rectangle getTile(double lng, double lat, int precision);

    /**
     * Create a new named {@link GeoGridAggregationBuilder}-derived builder
     */
    protected abstract GeoGridAggregationBuilder createBuilder(String name);

    public void testNoDocs() throws IOException {
        testCase(new MatchAllDocsQuery(), FIELD_NAME, randomPrecision(), null, iw -> {
            // Intentionally not writing any docs
        }, geoGrid -> {
            assertEquals(0, geoGrid.getBuckets().size());
        }, new GeoShapeFieldMapper.GeoShapeFieldType());

        testCase(new MatchAllDocsQuery(), FIELD_NAME, randomPrecision(), null, iw -> {
            // Intentionally not writing any docs
        }, geoGrid -> {
            assertEquals(0, geoGrid.getBuckets().size());
        }, new GeoPointFieldMapper.GeoPointFieldType());
    }

    public void testUnmapped() throws IOException {
        final MappedFieldType fieldType;
        if (randomBoolean()) {
            fieldType = new GeoPointFieldMapper.GeoPointFieldType();
        } else {
            fieldType = new GeoShapeFieldMapper.GeoShapeFieldType();
        }

        testCase(new MatchAllDocsQuery(), "wrong_field", randomPrecision(), null, iw -> {}, geoGrid -> {
            assertEquals(0, geoGrid.getBuckets().size());
        }, fieldType);

        testCase(new MatchAllDocsQuery(), "wrong_field", randomPrecision(), null, iw -> {
            iw.addDocument(Collections.singleton(
                new BinaryGeoShapeDocValuesField(FIELD_NAME, GeoTestUtils.toDecodedTriangles(new Point(10D, 10D)),
                    new CentroidCalculator(new Point(10D, 10D)))));
        }, geoGrid -> {
            assertEquals(0, geoGrid.getBuckets().size());
        }, fieldType);
    }

    public void testUnmappedMissingGeoPoint() throws IOException {
        GeoGridAggregationBuilder builder = createBuilder("_name")
            .field("wrong_field")
            .missing("53.69437,6.475031");
        testCase(new MatchAllDocsQuery(), randomPrecision(), null,
            iw -> iw.addDocument(Collections.singleton(new LatLonDocValuesField(FIELD_NAME, 10D, 10D))),
            geoGrid -> assertEquals(1, geoGrid.getBuckets().size()), builder, new GeoPointFieldMapper.GeoPointFieldType());
    }

    public void testUnmappedMissingGeoShape() throws IOException {
        GeoGridAggregationBuilder builder = createBuilder("_name")
            .field("wrong_field")
            .missing("LINESTRING (30 10, 10 30, 40 40)");
        testCase(new MatchAllDocsQuery(), 1, null,
            iw -> iw.addDocument(Collections.singleton(new LatLonDocValuesField(FIELD_NAME, 10D, 10D))),
            geoGrid -> assertEquals(1, geoGrid.getBuckets().size()), builder, new GeoPointFieldMapper.GeoPointFieldType());
    }


    public void testGeoPointWithSeveralDocs() throws IOException {
        int precision = randomPrecision();
        int numPoints = randomIntBetween(8, 128);
        Map<String, Integer> expectedCountPerGeoHash = new HashMap<>();
        testCase(new MatchAllDocsQuery(), FIELD_NAME, precision, null, iw -> {
                List<LatLonDocValuesField> points = new ArrayList<>();
                Set<String> distinctHashesPerDoc = new HashSet<>();
                for (int pointId = 0; pointId < numPoints; pointId++) {
                    double lat = (180d * randomDouble()) - 90d;
                    double lng = (360d * randomDouble()) - 180d;

                    // Precision-adjust longitude/latitude to avoid wrong bucket placement
                    // Internally, lat/lng get converted to 32 bit integers, loosing some precision.
                    // This does not affect geohashing because geohash uses the same algorithm,
                    // but it does affect other bucketing algos, thus we need to do the same steps here.
                    lng = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lng));
                    lat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));

                    points.add(new LatLonDocValuesField(FIELD_NAME, lat, lng));
                    String hash = hashAsString(lng, lat, precision);
                    if (distinctHashesPerDoc.contains(hash) == false) {
                        expectedCountPerGeoHash.put(hash, expectedCountPerGeoHash.getOrDefault(hash, 0) + 1);
                    }
                    distinctHashesPerDoc.add(hash);
                    if (usually()) {
                        iw.addDocument(points);
                        points.clear();
                        distinctHashesPerDoc.clear();
                    }
                }
                if (points.size() != 0) {
                    iw.addDocument(points);
                }
            },
            geoHashGrid -> {
                assertEquals(expectedCountPerGeoHash.size(), geoHashGrid.getBuckets().size());
                for (GeoGrid.Bucket bucket : geoHashGrid.getBuckets()) {
                    assertEquals((long) expectedCountPerGeoHash.get(bucket.getKeyAsString()), bucket.getDocCount());
                }
                assertTrue(AggregationInspectionHelper.hasValue(geoHashGrid));
            }, new GeoPointFieldMapper.GeoPointFieldType());
    }

    public void testGeoPointBounds() throws IOException {
        final int precision = randomPrecision();
        final int numDocs = randomIntBetween(100, 200);
        int numDocsWithin = 0;
        final GeoGridAggregationBuilder builder = createBuilder("_name");

        expectThrows(IllegalArgumentException.class, () -> builder.precision(-1));
        expectThrows(IllegalArgumentException.class, () -> builder.precision(30));

        GeoBoundingBox bbox = GeoBoundingBoxTests.randomBBox();
        final double boundsTop = bbox.top();
        final double boundsBottom = bbox.bottom();
        final double boundsWestLeft;
        final double boundsWestRight;
        final double boundsEastLeft;
        final double boundsEastRight;
        final boolean crossesDateline;
        if (bbox.right() < bbox.left()) {
            boundsWestLeft = -180;
            boundsWestRight = bbox.right();
            boundsEastLeft = bbox.left();
            boundsEastRight = 180;
            crossesDateline = true;
        } else { // only set east bounds
            boundsEastLeft = bbox.left();
            boundsEastRight = bbox.right();
            boundsWestLeft = 0;
            boundsWestRight = 0;
            crossesDateline = false;
        }

        List<LatLonDocValuesField> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            Point p;
            p = randomPoint();
            double x = GeoTestUtils.encodeDecodeLon(p.getX());
            double y = GeoTestUtils.encodeDecodeLat(p.getY());
            Rectangle pointTile = getTile(x, y, precision);

            boolean intersectsBounds = boundsTop >= pointTile.getMinY() && boundsBottom <= pointTile.getMaxY()
                && (boundsEastLeft <= pointTile.getMaxX() && boundsEastRight >= pointTile.getMinX()
                || (crossesDateline && boundsWestLeft <= pointTile.getMaxX() && boundsWestRight >= pointTile.getMinX()));
            if (intersectsBounds) {
                numDocsWithin += 1;
            }
            docs.add(new LatLonDocValuesField(FIELD_NAME, p.getLat(), p.getLon()));
        }

        final long numDocsInBucket = numDocsWithin;

        testCase(new MatchAllDocsQuery(), FIELD_NAME, precision, bbox, iw -> {
                for (LatLonDocValuesField docField : docs) {
                    iw.addDocument(Collections.singletonList(docField));
                }
            },
            geoGrid -> {
                assertTrue(AggregationInspectionHelper.hasValue(geoGrid));
                long docCount = 0;
                for (int i = 0; i < geoGrid.getBuckets().size(); i++) {
                    docCount += geoGrid.getBuckets().get(i).getDocCount();
                }
                assertThat(docCount, equalTo(numDocsInBucket));
            }, new GeoPointFieldMapper.GeoPointFieldType());
    }

    public void testGeoShapeBounds() throws IOException {
        final int precision = randomPrecision();
        final int numDocs = randomIntBetween(100, 200);
        int numDocsWithin = 0;
        final GeoGridAggregationBuilder builder = createBuilder("_name");

        expectThrows(IllegalArgumentException.class, () -> builder.precision(-1));
        expectThrows(IllegalArgumentException.class, () -> builder.precision(30));

        GeoBoundingBox bbox = GeoBoundingBoxTests.randomBBox();
        final double boundsTop = bbox.top();
        final double boundsBottom = bbox.bottom();
        final double boundsWestLeft;
        final double boundsWestRight;
        final double boundsEastLeft;
        final double boundsEastRight;
        final boolean crossesDateline;
        if (bbox.right() < bbox.left()) {
            boundsWestLeft = -180;
            boundsWestRight = bbox.right();
            boundsEastLeft = bbox.left();
            boundsEastRight = 180;
            crossesDateline = true;
        } else { // only set east bounds
            boundsEastLeft = bbox.left();
            boundsEastRight = bbox.right();
            boundsWestLeft = 0;
            boundsWestRight = 0;
            crossesDateline = false;
        }

        List<BinaryGeoShapeDocValuesField> docs = new ArrayList<>();
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            Point p;
            p = randomPoint();
            double x = GeoTestUtils.encodeDecodeLon(p.getX());
            double y = GeoTestUtils.encodeDecodeLat(p.getY());
            Rectangle pointTile = getTile(x, y, precision);


            TriangleTreeReader reader = triangleTreeReader(p, GeoShapeCoordinateEncoder.INSTANCE);
            GeoRelation tileRelation = reader.relateTile(GeoShapeCoordinateEncoder.INSTANCE.encodeX(pointTile.getMinX()),
                GeoShapeCoordinateEncoder.INSTANCE.encodeY(pointTile.getMinY()),
                GeoShapeCoordinateEncoder.INSTANCE.encodeX(pointTile.getMaxX()),
                GeoShapeCoordinateEncoder.INSTANCE.encodeY(pointTile.getMaxY()));
            boolean intersectsBounds = boundsTop >= pointTile.getMinY() && boundsBottom <= pointTile.getMaxY()
                && (boundsEastLeft <= pointTile.getMaxX() && boundsEastRight >= pointTile.getMinX()
                || (crossesDateline && boundsWestLeft <= pointTile.getMaxX() && boundsWestRight >= pointTile.getMinX()));
            if (tileRelation != GeoRelation.QUERY_DISJOINT && intersectsBounds) {
                numDocsWithin += 1;
            }


            points.add(p);
            docs.add(new BinaryGeoShapeDocValuesField(FIELD_NAME,
                GeoTestUtils.toDecodedTriangles(p), new CentroidCalculator(p)));
        }

        final long numDocsInBucket = numDocsWithin;

        testCase(new MatchAllDocsQuery(), FIELD_NAME, precision, bbox, iw -> {
                for (BinaryGeoShapeDocValuesField docField : docs) {
                    iw.addDocument(Collections.singletonList(docField));
                }
            },
            geoGrid -> {
                assertThat(AggregationInspectionHelper.hasValue(geoGrid), equalTo(numDocsInBucket > 0));
                long docCount = 0;
                for (int i = 0; i < geoGrid.getBuckets().size(); i++) {
                    docCount += geoGrid.getBuckets().get(i).getDocCount();
                }
                assertThat(docCount, equalTo(numDocsInBucket));
            }, new GeoShapeFieldMapper.GeoShapeFieldType());
    }

    public void testGeoShapeWithSeveralDocs() throws IOException {
        int precision = randomIntBetween(1, 4);
        int numShapes = randomIntBetween(8, 128);
        Map<String, Integer> expectedCountPerGeoHash = new HashMap<>();
        testCase(new MatchAllDocsQuery(), FIELD_NAME, precision, null, iw -> {
            List<Point> shapes = new ArrayList<>();
            Document document = new Document();
            Set<String> distinctHashesPerDoc = new HashSet<>();
            for (int shapeId = 0; shapeId < numShapes; shapeId++) {
                // undefined close to pole
                double lat = (170.10225756d * randomDouble()) - 85.05112878d;
                double lng = (360d * randomDouble()) - 180d;

                // Precision-adjust longitude/latitude to avoid wrong bucket placement
                // Internally, lat/lng get converted to 32 bit integers, loosing some precision.
                // This does not affect geohashing because geohash uses the same algorithm,
                // but it does affect other bucketing algos, thus we need to do the same steps here.
                lng = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lng));
                lat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));

                shapes.add(new Point(lng, lat));
                String hash = hashAsString(lng, lat, precision);
                if (distinctHashesPerDoc.contains(hash) == false) {
                    expectedCountPerGeoHash.put(hash, expectedCountPerGeoHash.getOrDefault(hash, 0) + 1);
                }
                distinctHashesPerDoc.add(hash);
                if (usually()) {
                    Geometry geometry = new MultiPoint(new ArrayList<>(shapes));
                    document.add(new BinaryGeoShapeDocValuesField(FIELD_NAME,
                        GeoTestUtils.toDecodedTriangles(geometry), new CentroidCalculator(geometry)));
                    iw.addDocument(document);
                    shapes.clear();
                    distinctHashesPerDoc.clear();
                    document.clear();
                }
            }
            if (shapes.size() != 0) {
                Geometry geometry = new MultiPoint(new ArrayList<>(shapes));
                document.add(new BinaryGeoShapeDocValuesField(FIELD_NAME,
                    GeoTestUtils.toDecodedTriangles(geometry), new CentroidCalculator(geometry)));
                iw.addDocument(document);
            }
        }, geoHashGrid -> {
            assertEquals(expectedCountPerGeoHash.size(), geoHashGrid.getBuckets().size());
            for (GeoGrid.Bucket bucket : geoHashGrid.getBuckets()) {
                assertEquals((long) expectedCountPerGeoHash.get(bucket.getKeyAsString()), bucket.getDocCount());
            }
            assertTrue(AggregationInspectionHelper.hasValue(geoHashGrid));
        }, new GeoShapeFieldMapper.GeoShapeFieldType());
    }

    private void testCase(Query query, String field, int precision, GeoBoundingBox geoBoundingBox,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalGeoGrid<T>> verify, MappedFieldType fieldType) throws IOException {
        testCase(query, precision, geoBoundingBox, buildIndex, verify, createBuilder("_name").field(field), fieldType);
    }

    @SuppressWarnings("unchecked")
    private void testCase(Query query, int precision, GeoBoundingBox geoBoundingBox,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex, Consumer<InternalGeoGrid<T>> verify,
                          GeoGridAggregationBuilder aggregationBuilder, MappedFieldType fieldType) throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        buildIndex.accept(indexWriter);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        fieldType.setHasDocValues(true);
        fieldType.setName(FIELD_NAME);

        aggregationBuilder.precision(precision);
        if (geoBoundingBox != null) {
            aggregationBuilder.setGeoBoundingBox(geoBoundingBox);
            assertThat(aggregationBuilder.geoBoundingBox(), equalTo(geoBoundingBox));
        }

        Aggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(query, aggregator);
        aggregator.postCollection();
        verify.accept((InternalGeoGrid<T>) aggregator.buildAggregation(0L));

        indexReader.close();
        directory.close();
    }
}
