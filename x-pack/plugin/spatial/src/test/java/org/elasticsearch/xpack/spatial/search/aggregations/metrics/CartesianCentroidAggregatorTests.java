/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.XYDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.index.mapper.PointFieldMapper;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianPointValuesSourceType;

import java.io.IOException;
import java.util.List;

public class CartesianCentroidAggregatorTests extends AggregatorTestCase {

    private static final double GEOHASH_TOLERANCE = 1E-6D;

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new LocalStateSpatialPlugin());
    }

    public void testEmpty() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            CartesianCentroidAggregationBuilder aggBuilder = new CartesianCentroidAggregationBuilder("my_agg").field("field");

            MappedFieldType fieldType = new PointFieldMapper.PointFieldType("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalCartesianCentroid result = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertNull(result.centroid());
                assertFalse(AggregationInspectionHelper.hasValue(result));
            }
        }
    }

    public void testUnmapped() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            CartesianCentroidAggregationBuilder aggBuilder = new CartesianCentroidAggregationBuilder("my_agg").field("another_field");

            Document document = new Document();
            document.add(new LatLonDocValuesField("field", 10, 10));
            w.addDocument(document);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);

                MappedFieldType fieldType = new PointFieldMapper.PointFieldType("another_field");
                InternalCartesianCentroid result = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertNull(result.centroid());

                fieldType = new PointFieldMapper.PointFieldType("another_field");
                result = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertNull(result.centroid());
                assertFalse(AggregationInspectionHelper.hasValue(result));
            }
        }
    }

    public void testUnmappedWithMissing() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            CartesianCentroidAggregationBuilder aggBuilder = new CartesianCentroidAggregationBuilder("my_agg").field("another_field")
                .missing("6.475030899047852, 53.69437026977539");

            CartesianPoint expectedCentroid = new CartesianPoint(6.475030899047852, 53.69437026977539);
            Document document = new Document();
            document.add(new LatLonDocValuesField("field", 10, 10));
            w.addDocument(document);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);

                MappedFieldType fieldType = new PointFieldMapper.PointFieldType("another_field");
                InternalCartesianCentroid result = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertEquals(expectedCentroid, result.centroid());
                assertTrue(AggregationInspectionHelper.hasValue(result));
            }
        }
    }

    public void testSingleValuedField() throws Exception {
        int numDocs = scaledRandomIntBetween(64, 256);
        int numUniqueCartesianPoints = randomIntBetween(1, numDocs);
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            CartesianPoint expectedCentroid = new CartesianPoint(0, 0);
            CartesianPoint[] singleValues = new CartesianPoint[numUniqueCartesianPoints];
            for (int i = 0; i < singleValues.length; i++) {
                Point point = GeometryTestUtils.randomPoint(false);
                singleValues[i] = new CartesianPoint(point.getX(), point.getY());
            }
            for (int i = 0; i < numDocs; i++) {
                CartesianPoint singleVal = singleValues[i % numUniqueCartesianPoints];
                Document document = new Document();
                document.add(new XYDocValuesField("field", (float) singleVal.getX(), (float) singleVal.getY()));
                w.addDocument(document);
                expectedCentroid = expectedCentroid.reset(
                    expectedCentroid.getX() + (singleVal.getX() - expectedCentroid.getX()) / (i + 1),
                    expectedCentroid.getY() + (singleVal.getY() - expectedCentroid.getY()) / (i + 1)
                );
            }
            assertCentroid(w, expectedCentroid);
        }
    }

    public void testMultiValuedField() throws Exception {
        int numDocs = scaledRandomIntBetween(64, 256);
        int numUniqueCartesianPoints = randomIntBetween(1, numDocs);
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {

            CartesianPoint expectedCentroid = new CartesianPoint(0, 0);
            CartesianPoint[] multiValues = new CartesianPoint[numUniqueCartesianPoints];
            for (int i = 0; i < multiValues.length; i++) {
                Point point = GeometryTestUtils.randomPoint(false);
                multiValues[i] = new CartesianPoint(point.getX(), point.getY());
            }
            final CartesianPoint[] multiVal = new CartesianPoint[2];
            for (int i = 0; i < numDocs; i++) {
                multiVal[0] = multiValues[i % numUniqueCartesianPoints];
                multiVal[1] = multiValues[(i + 1) % numUniqueCartesianPoints];
                Document document = new Document();
                document.add(new XYDocValuesField("field", (float) multiVal[0].getX(), (float) multiVal[0].getY()));
                document.add(new XYDocValuesField("field", (float) multiVal[1].getX(), (float) multiVal[1].getY()));
                w.addDocument(document);
                double newMVx = (multiVal[0].getX() + multiVal[1].getX()) / 2d;
                double newMVy = (multiVal[0].getY() + multiVal[1].getY()) / 2d;
                expectedCentroid = expectedCentroid.reset(
                    expectedCentroid.getX() + (newMVx - expectedCentroid.getX()) / (i + 1),
                    expectedCentroid.getY() + (newMVy - expectedCentroid.getY()) / (i + 1)
                );
            }
            assertCentroid(w, expectedCentroid);
        }
    }

    private void assertCentroid(RandomIndexWriter w, CartesianPoint expectedCentroid) throws IOException {
        MappedFieldType fieldType = new PointFieldMapper.PointFieldType("field");
        CartesianCentroidAggregationBuilder aggBuilder = new CartesianCentroidAggregationBuilder("my_agg").field("field");
        try (IndexReader reader = w.getReader()) {
            IndexSearcher searcher = new IndexSearcher(reader);
            InternalCartesianCentroid result = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);

            assertEquals("my_agg", result.getName());
            CartesianPoint centroid = result.centroid();
            assertNotNull(centroid);
            assertEquals(expectedCentroid.getX(), centroid.getX(), GEOHASH_TOLERANCE);
            assertEquals(expectedCentroid.getY(), centroid.getY(), GEOHASH_TOLERANCE);
            assertTrue(AggregationInspectionHelper.hasValue(result));
        }
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new CartesianCentroidAggregationBuilder("foo").field(fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CartesianPointValuesSourceType.instance());
    }
}
