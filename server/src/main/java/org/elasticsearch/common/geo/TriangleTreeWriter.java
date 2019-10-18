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
package org.elasticsearch.common.geo;
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.GeoShapeIndexer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * This is a tree-writer that serializes a {@link Geometry} and tessellate it to write it into a byte array.
 * Internally it tessellate the given {@link Geometry} and it builds an interval tree with the
 * tessellation.
 */
public class TriangleTreeWriter implements Writeable {

    private final TriangleTreeBuilder builder;
    private final TriangleTreeNode node;
    private final CoordinateEncoder coordinateEncoder;
    private CentroidCalculator centroidCalculator;

    public TriangleTreeWriter(Geometry geometry, CoordinateEncoder coordinateEncoder) {
        this.coordinateEncoder = coordinateEncoder;
        this.centroidCalculator = new CentroidCalculator();
        builder = new TriangleTreeBuilder(coordinateEncoder);
        geometry.visit(builder);
        node = builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(coordinateEncoder.encodeX(centroidCalculator.getX()));
        out.writeInt(coordinateEncoder.encodeY(centroidCalculator.getY()));
        node.writeTo(out);
    }

    /**
     * Class that tessellate the geometry and build an interval tree in memory.
     */
    class TriangleTreeBuilder implements GeometryVisitor<Void, RuntimeException> {

        private List<TriangleTreeLeaf> triangles;
        private final CoordinateEncoder coordinateEncoder;

        TriangleTreeBuilder(CoordinateEncoder coordinateEncoder) {
            this.coordinateEncoder = coordinateEncoder;
            this.triangles = new ArrayList<>();
        }

        private void addTriangles(List<TriangleTreeLeaf> triangles) {
            this.triangles.addAll(triangles);
        }

        @Override
        public Void visit(GeometryCollection<?> collection) {
            for (Geometry geometry : collection) {
                geometry.visit(this);
            }
            return null;
        }

        @Override
        public Void visit(Line line) {
            for (int i =0; i < line.length(); i++) {
                centroidCalculator.addCoordinate(line.getX(i), line.getY(i));
            }
            addTriangles(TriangleTreeLeaf.fromLine(coordinateEncoder, line));
            return null;
        }

        @Override
        public Void visit(MultiLine multiLine) {
            for (Line line : multiLine) {
                visit(line);
            }
            return null;
        }

        @Override
        public Void visit(Polygon polygon) {
            // TODO: Shall we consider holes for centroid computation?
            for (int i =0; i < polygon.getPolygon().length() - 1; i++) {
                centroidCalculator.addCoordinate(polygon.getPolygon().getX(i), polygon.getPolygon().getY(i));
            }
            addTriangles(TriangleTreeLeaf.fromPolygon(coordinateEncoder, polygon));
            return null;
        }

        @Override
        public Void visit(MultiPolygon multiPolygon) {
            for (Polygon polygon : multiPolygon) {
                visit(polygon);
            }
            return null;
        }

        @Override
        public Void visit(Rectangle r) {
            centroidCalculator.addCoordinate(r.getMinX(), r.getMinY());
            centroidCalculator.addCoordinate(r.getMaxX(), r.getMaxY());
            addTriangles(TriangleTreeLeaf.fromRectangle(coordinateEncoder, r));
            return null;
        }

        @Override
        public Void visit(Point point) {
            centroidCalculator.addCoordinate(point.getX(), point.getY());
            addTriangles(TriangleTreeLeaf.fromPoints(coordinateEncoder, point));
            return null;
        }

        @Override
        public Void visit(MultiPoint multiPoint) {
            for (Point point : multiPoint) {
                visit(point);
            }
            return null;
        }

        @Override
        public Void visit(LinearRing ring) {
            throw new IllegalArgumentException("invalid shape type found [LinearRing]");
        }

        @Override
        public Void visit(Circle circle) {
            throw new IllegalArgumentException("invalid shape type found [Circle]");
        }


        public TriangleTreeNode build() {
            if (triangles.size() == 1) {
                return new TriangleTreeNode(triangles.get(0));
            }
            TriangleTreeNode[] nodes = new TriangleTreeNode[triangles.size()];
            for (int i = 0; i < triangles.size(); i++) {
                nodes[i] = new TriangleTreeNode(triangles.get(i));
            }
            TriangleTreeNode root =  createTree(nodes, 0, triangles.size() - 1, false);
            for (TriangleTreeNode node : nodes) {
                root.minX = Math.min(root.minX, node.minX);
                root.minY = Math.min(root.minY, node.minY);
            }
            return root;
        }

        /** Creates tree from sorted components (with range low and high inclusive) */
        private TriangleTreeNode createTree(TriangleTreeNode[] components, int low, int high, boolean splitX) {
            if (low > high) {
                return null;
            }
            final int mid = (low + high) >>> 1;
            if (low < high) {
                Comparator<TriangleTreeNode> comparator;
                if (splitX) {
                    comparator = (left, right) -> {
                        int ret = Double.compare(left.minX, right.minX);
                        if (ret == 0) {
                            ret = Double.compare(left.maxX, right.maxX);
                        }
                        return ret;
                    };
                } else {
                    comparator = (left, right) -> {
                        int ret = Double.compare(left.minY, right.minY);
                        if (ret == 0) {
                            ret = Double.compare(left.maxY, right.maxY);
                        }
                        return ret;
                    };
                }
                //Collections.sort(components, comparator);
                ArrayUtil.select(components, low, high + 1, mid, comparator);
            }
            TriangleTreeNode newNode = components[mid];
            // find children
            newNode.left = createTree(components, low, mid - 1, !splitX);
            newNode.right = createTree(components, mid + 1, high, !splitX);

            // pull up max values to this node
            if (newNode.left != null) {
                newNode.maxX = Math.max(newNode.maxX, newNode.left.maxX);
                newNode.maxY = Math.max(newNode.maxY, newNode.left.maxY);
            }
            if (newNode.right != null) {
                newNode.maxX = Math.max(newNode.maxX, newNode.right.maxX);
                newNode.maxY = Math.max(newNode.maxY, newNode.right.maxY);
            }
            return newNode;
        }
    }

    /**
     * Represents an inner node of the tree.
     */
    static class TriangleTreeNode implements Writeable {
        /** minimum latitude of this geometry's bounding box area */
        private int minY;
        /** maximum latitude of this geometry's bounding box area */
        private int maxY;
        /** minimum longitude of this geometry's bounding box area */
        private int minX;
        /** maximum longitude of this geometry's bounding box area */
        private int maxX;
        // child components, or null. Note internal nodes might mot have
        // a consistent bounding box. Internal nodes should not be accessed
        // outside if this class.
        private TriangleTreeNode left;
        private TriangleTreeNode right;
        /** root node of edge tree */
        private TriangleTreeLeaf component;

        protected TriangleTreeNode(TriangleTreeLeaf component) {
            this.minY = component.minY;
            this.maxY = component.maxY;
            this.minX = component.minX;
            this.maxX = component.maxX;
            this.component = component;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(maxX);
            out.writeVLong((long) maxX - minX);
            out.writeInt(maxY);
            out.writeVLong((long) maxY - minY);
            writeMetadata(out);
            writeComponent(out);
            if (left != null) {
                left.writeNode(out, maxX, maxY);
            }
            if (right != null) {
                right.writeNode(out, maxX, maxY);
            }
        }

        private void writeNode(StreamOutput out, int parentMaxX, int parentMaxY) throws IOException {
            out.writeVLong((long) parentMaxX - maxX);
            out.writeVLong((long) parentMaxY - maxY);
            int size = nodeSize(false, parentMaxX, parentMaxY);
            out.writeVInt(size);
            writeMetadata(out);
            writeComponent(out);
            if (left != null) {
                left.writeNode(out, maxX, maxY);
            }
            if (right != null) {
                int rightSize = right.nodeSize(true, maxX, maxY);
                out.writeVInt(rightSize);
                right.writeNode(out, maxX, maxY);
            }
        }

        private void writeMetadata(StreamOutput out) throws IOException {
            byte metadata = 0;
            metadata |= (left != null) ? (1 << 0) : 0;
            metadata |= (right != null) ? (1 << 1) : 0;
            if (component.type == TriangleTreeLeaf.TYPE.POINT) {
                metadata |= (1 << 2);
            } else if (component.type == TriangleTreeLeaf.TYPE.LINE) {
                metadata |= (1 << 3);
            } else {
                metadata |= (1 << 2);
                metadata |= (1 << 3);
                // TODO: bits 4, 5 & 6 should be used to stored if the edge is from the polygon
            }
            out.writeByte(metadata);
        }

        private void writeComponent(StreamOutput out) throws IOException {
            if (component.type == TriangleTreeLeaf.TYPE.POINT) {
                out.writeVLong((long) maxX - component.aX);
                out.writeVLong((long) maxY - component.aY);
            } else if (component.type == TriangleTreeLeaf.TYPE.LINE) {
                out.writeVLong((long) maxX - component.aX);
                out.writeVLong((long) maxY - component.aY);
                out.writeVLong((long) maxX - component.bX);
                out.writeVLong((long) maxY - component.bY);
            } else {
                out.writeVLong((long) maxX - component.aX);
                out.writeVLong((long) maxY - component.aY);
                out.writeVLong((long) maxX - component.bX);
                out.writeVLong((long) maxY - component.bY);
                out.writeVLong((long) maxX - component.cX);
                out.writeVLong((long) maxY - component.cY);
            }
        }

        public int nodeSize(boolean includeBox, int parentMaxX, int parentMaxY) throws IOException {
            int size =0;
            size++; //metadata
            size += componentSize();
            if (left != null) {
                size +=  left.nodeSize(true, maxX, maxY);
            }
            if (right != null) {
                int rightSize = right.nodeSize(true, maxX, maxY);
                size +=  vLongSize(rightSize); // jump size
                size +=  rightSize;
            }
            if (includeBox) {
                int jumpSize = size;
                size += vLongSize((long) parentMaxX - maxX);
                size += vLongSize((long) parentMaxY - maxY);// box
                size +=  vLongSize(jumpSize); // jump size
            }
            return size;
        }

        public int componentSize() throws IOException {
            int size = 0;
            if (component.type == TriangleTreeLeaf.TYPE.POINT) {
                size += vLongSize((long) maxX - component.aX);
                size += vLongSize((long) maxY - component.aY);
            } else if (component.type == TriangleTreeLeaf.TYPE.LINE) {
                size += vLongSize((long) maxX - component.aX);
                size += vLongSize((long) maxY - component.aY);
                size += vLongSize((long) maxX - component.bX);
                size += vLongSize((long) maxY - component.bY);
            } else {
                size += vLongSize((long) maxX - component.aX);
                size += vLongSize((long) maxY - component.aY);
                size += vLongSize((long) maxX - component.bX);
                size += vLongSize((long) maxY - component.bY);
                size += vLongSize((long) maxX - component.cX);
                size += vLongSize((long) maxY - component.cY);
            }
            return size;
        }

        public int vLongSize(long i) throws IOException {
            int size = 1;
            while ((i & ~0x7F) != 0) {
               size++;
               i >>>= 7;
            }
            return size;
        }

    }

    /**
     * Represents an leaf of the tree containing one of the triangles.
     */
    //TODO: Add edges belong to the polygon when updating to LUCENE 8.3
    static class TriangleTreeLeaf {

        public enum TYPE {
            POINT, LINE, TRIANGLE
        }

        int minX, maxX, minY, maxY;
        int aX, aY, bX, bY, cX, cY;
        TYPE type;

        // constructor for points
        TriangleTreeLeaf(int aXencoded, int aYencoded) {
            encodePoint(aXencoded, aYencoded);
        }

        // constructor for points and lines
        TriangleTreeLeaf(int aXencoded, int aYencoded, int bXencoded, int bYencoded) {
            if (aXencoded == bXencoded && aYencoded == bYencoded) {
                encodePoint(aXencoded, aYencoded);
            } else {
                encodeLine(aXencoded, aYencoded, bXencoded, bYencoded);
            }
        }

        // generic constructor
        TriangleTreeLeaf(int aXencoded, int aYencoded, int bXencoded, int bYencoded, int cXencoded, int cYencoded) {
            if (aXencoded == bXencoded && aYencoded == bYencoded) {
                if (aXencoded == cXencoded && aYencoded == cYencoded) {
                    encodePoint(aYencoded, aXencoded);
                } else {
                    encodeLine(aYencoded, aXencoded, cYencoded, cXencoded);
                    return;
                }
            } else if (aXencoded == cXencoded && aYencoded == cYencoded) {
                encodeLine(aYencoded, aXencoded, bYencoded, bXencoded);
            } else {
                encodeTriangle(aXencoded, aYencoded, bXencoded, bYencoded, cXencoded, cYencoded);
            }
        }

        private void encodePoint(int aXencoded, int aYencoded) {
            this.type = TYPE.POINT;
            aX = aXencoded;
            aY = aYencoded;
            minX = aX;
            maxX = aX;
            minY = aY;
            maxY = aY;
        }

        private void encodeLine(int aXencoded, int aYencoded, int bXencoded, int bYencoded) {
            this.type = TYPE.LINE;
            //rotate edges and place minX at the beginning
            if (aXencoded > bXencoded) {
                aX = bXencoded;
                aY = bYencoded;
                bX = aXencoded;
                bY = aYencoded;
            } else {
                aX = aXencoded;
                aY = aYencoded;
                bX = bXencoded;
                bY = bYencoded;
            }
            this.minX = aX;
            this.maxX = bX;
            this.minY = Math.min(aY, bY);
            this.maxY = Math.max(aY, bY);
        }

        private void encodeTriangle(int aXencoded, int aYencoded, int bXencoded, int bYencoded, int cXencoded, int cYencoded) {

            int aX, aY, bX, bY, cX, cY;
            //change orientation if CW
            if (GeoUtils.orient(aXencoded, aYencoded, bXencoded, bYencoded, cXencoded, cYencoded) == -1) {
                aX = cXencoded;
                bX = bXencoded;
                cX = aXencoded;
                aY = cYencoded;
                bY = bYencoded;
                cY = aYencoded;
            } else {
                aX = aXencoded;
                bX = bXencoded;
                cX = cXencoded;
                aY = aYencoded;
                bY = bYencoded;
                cY = cYencoded;
            }
            //rotate edges and place minX at the beginning
            if (bX < aX || cX < aX) {
                if (bX < cX) {
                    int tempX = aX;
                    int tempY = aY;
                    aX = bX;
                    aY = bY;
                    bX = cX;
                    bY = cY;
                    cX = tempX;
                    cY = tempY;
                } else if (cX < aX) {
                    int tempX = aX;
                    int tempY = aY;
                    aX = cX;
                    aY = cY;
                    cX = bX;
                    cY = bY;
                    bX = tempX;
                    bY = tempY;
                }
            } else if (aX == bX && aX == cX) {
                //degenerated case, all points with same longitude
                //we need to prevent that aX is in the middle (not part of the MBS)
                if (bY < aY || cY < aY) {
                    if (bY < cY) {
                        int tempX = aX;
                        int tempY = aY;
                        aX = bX;
                        aY = bY;
                        bX = cX;
                        bY = cY;
                        cX = tempX;
                        cY = tempY;
                    } else if (cY < aY) {
                        int tempX = aX;
                        int tempY = aY;
                        aX = cX;
                        aY = cY;
                        cX = bX;
                        cY = bY;
                        bX = tempX;
                        bY = tempY;
                    }
                }
            }
            this.aX = aX;
            this.aY = aY;
            this.bX = bX;
            this.bY = bY;
            this.cX = cX;
            this.cY = cY;
            this.minX = aX;
            this.maxX = Math.max(aX, Math.max(bX, cX));
            this.minY = Math.min(aY, Math.min(bY, cY));
            this.maxY = Math.max(aY, Math.max(bY, cY));
            type = TYPE.TRIANGLE;
        }

        private static List<TriangleTreeLeaf> fromPoints(CoordinateEncoder encoder, Point... points) {
            List<TriangleTreeLeaf> triangles = new ArrayList<>(points.length);
            for (int i = 0; i < points.length; i++) {
                triangles.add(new TriangleTreeLeaf(encoder.encodeX(points[i].getX()), encoder.encodeY(points[i].getY())));
            }
            return triangles;
        }

        private static List<TriangleTreeLeaf> fromRectangle(CoordinateEncoder encoder, Rectangle... rectangles) {
            List<TriangleTreeLeaf> triangles = new ArrayList<>(2 * rectangles.length);
            for (Rectangle r : rectangles) {
                triangles.add(new TriangleTreeLeaf(
                    encoder.encodeX(r.getMinX()), encoder.encodeY(r.getMinY()),
                    encoder.encodeX(r.getMaxX()), encoder.encodeY(r.getMinY()),
                    encoder.encodeX(r.getMinX()), encoder.encodeY(r.getMaxY())));
                triangles.add(new TriangleTreeLeaf(
                    encoder.encodeX(r.getMinX()), encoder.encodeY(r.getMaxY()),
                    encoder.encodeX(r.getMaxX()), encoder.encodeY(r.getMinY()),
                    encoder.encodeX(r.getMaxX()), encoder.encodeY(r.getMaxY())));
            }
            return triangles;
        }

        private static List<TriangleTreeLeaf> fromLine(CoordinateEncoder encoder, Line line) {
            List<TriangleTreeLeaf> triangles = new ArrayList<>(line.length() - 1);
            for (int i = 0, j = 1; i < line.length() - 1; i++, j++) {
                triangles.add(new TriangleTreeLeaf(encoder.encodeX(line.getX(i)), encoder.encodeY(line.getY(i)),
                    encoder.encodeX(line.getX(j)), encoder.encodeY(line.getY(j))));
            }
            return triangles;
        }

        private static List<TriangleTreeLeaf> fromPolygon(CoordinateEncoder encoder, Polygon polygon) {
            // TODO: We are going to be tessellating the polygon twice, can we do something?
            // TODO: tessellator seems to have some reference to the encoding but does not need to have.
            List<Tessellator.Triangle> tessallation = Tessellator.tessellate(GeoShapeIndexer.toLucenePolygon(polygon));
            List<TriangleTreeLeaf> triangles = new ArrayList<>(tessallation.size());
            for (Tessellator.Triangle t : tessallation) {
                triangles.add(new TriangleTreeLeaf(encoder.encodeX(t.getX(0)), encoder.encodeY(t.getY(0)),
                    encoder.encodeX(t.getX(1)), encoder.encodeY(t.getY(1)),
                    encoder.encodeX(t.getX(2)), encoder.encodeY(t.getY(2))));
            }
            return triangles;
        }
    }
}
