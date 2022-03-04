/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.script.field.ScriptFieldDocValues;
import org.elasticsearch.script.field.ScriptFieldValues;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.AbstractList;
import java.util.Comparator;
import java.util.function.UnaryOperator;

/**
 * Script level doc values, the assumption is that any implementation will
 * implement a {@link Longs#getValue getValue} method.
 *
 * Implementations should not internally re-use objects for the values that they
 * return as a single {@link ScriptDocValues} instance can be reused to return
 * values form multiple documents.
 */
public abstract class ScriptDocValues<T> extends AbstractList<T> {

    // Throw meaningful exceptions if someone tries to modify the ScriptDocValues.
    @Override
    public final void add(int index, T element) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    @Override
    public final boolean remove(Object o) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    @Override
    public final void replaceAll(UnaryOperator<T> operator) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    @Override
    public final T set(int index, T element) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    @Override
    public final void sort(Comparator<? super T> c) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    protected void throwIfEmpty() {
        if (size() == 0) {
            throw new IllegalStateException(
                "A document doesn't have a value for a field! " + "Use doc[<field>].size()==0 to check if a document is missing a field!"
            );
        }
    }

    public static class Longs extends ScriptDocValues<Long> {

        public Longs(ScriptFieldDocValues<Long> supplier) {
            super(supplier);
        }

        public long getValue() {
            return get(0);
        }

        @Override
        public Long get(int index) {
            throwIfEmpty();
            return supplier.getInternal(index);
        }

        @Override
        public int size() {
            return supplier.size();
        }
    }

    public static class Dates extends ScriptDocValues<ZonedDateTime> {

        public Dates(ScriptFieldDocValues<ZonedDateTime> supplier) {
            super(supplier);
        }

        /**
         * Fetch the first field value or 0 millis after epoch if there are no
         * in.
         */
        public ZonedDateTime getValue() {
            return get(0);
        }

        @Override
        public ZonedDateTime get(int index) {
            if (supplier.size() == 0) {
                throw new IllegalStateException(
                    "A document doesn't have a value for a field! "
                        + "Use doc[<field>].size()==0 to check if a document is missing a field!"
                );
            }
            if (index >= supplier.size()) {
                throw new IndexOutOfBoundsException(
                    "attempted to fetch the [" + index + "] date when there are only [" + supplier.size() + "] dates."
                );
            }
            return supplier.getInternal(index);
        }

        @Override
        public int size() {
            return supplier.size();
        }
    }

    public static class DoublesSupplier implements ScriptFieldDocValues<Double> {

        private final SortedNumericDoubleValues in;
        private double[] values = new double[0];
        private int count;

        public DoublesSupplier(SortedNumericDoubleValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    values[i] = in.nextValue();
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        private void resize(int newSize) {
            count = newSize;
            values = ArrayUtil.grow(values, count);
        }

        @Override
        public Double getInternal(int index) {
            return values[index];
        }

        @Override
        public int size() {
            return count;
        }
    }

    public static class Doubles extends ScriptDocValues<Double> {

        public Doubles(ScriptFieldDocValues<Double> supplier) {
            super(supplier);
        }

        public double getValue() {
            return get(0);
        }

        @Override
        public Double get(int index) {
            if (supplier.size() == 0) {
                throw new IllegalStateException(
                    "A document doesn't have a value for a field! "
                        + "Use doc[<field>].size()==0 to check if a document is missing a field!"
                );
            }
            return supplier.getInternal(index);
        }

        @Override
        public int size() {
            return supplier.size();
        }
    }

    public abstract static class Geometry<T> extends ScriptDocValues<T> {

        public Geometry(ScriptFieldDocValues<T> supplier) {
            super(supplier);
        }

        /** Returns the dimensional type of this geometry */
        public abstract int getDimensionalType();

        /** Returns the bounding box of this geometry  */
        public abstract GeoBoundingBox getBoundingBox();

        /** Returns the centroid of this geometry  */
        public abstract GeoPoint getCentroid();

        /** Returns the width of the bounding box diagonal in the spherical Mercator projection (meters)  */
        public abstract double getMercatorWidth();

        /** Returns the height of the bounding box diagonal in the spherical Mercator projection (meters) */
        public abstract double getMercatorHeight();
    }

    public interface GeometrySupplier<T> extends ScriptFieldDocValues<T> {

        GeoPoint getInternalCentroid();

        GeoBoundingBox getInternalBoundingBox();
    }

    public static class GeoPoints extends Geometry<GeoPoint> {

        private final GeometrySupplier<GeoPoint> geometrySupplier;

        public GeoPoints(GeometrySupplier<GeoPoint> supplier) {
            super(supplier);
            geometrySupplier = supplier;
        }

        public GeoPoint getValue() {
            return get(0);
        }

        public double getLat() {
            return getValue().lat();
        }

        public double[] getLats() {
            double[] lats = new double[size()];
            for (int i = 0; i < size(); i++) {
                lats[i] = get(i).lat();
            }
            return lats;
        }

        public double[] getLons() {
            double[] lons = new double[size()];
            for (int i = 0; i < size(); i++) {
                lons[i] = get(i).lon();
            }
            return lons;
        }

        public double getLon() {
            return getValue().lon();
        }

        @Override
        public GeoPoint get(int index) {
            if (supplier.size() == 0) {
                throw new IllegalStateException(
                    "A document doesn't have a value for a field! "
                        + "Use doc[<field>].size()==0 to check if a document is missing a field!"
                );
            }
            final GeoPoint point = supplier.getInternal(index);
            return new GeoPoint(point.lat(), point.lon());
        }

        @Override
        public int size() {
            return supplier.size();
        }

        public double arcDistance(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoUtils.arcDistance(point.lat(), point.lon(), lat, lon);
        }

        public double arcDistanceWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            return arcDistance(lat, lon);
        }

        public double planeDistance(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoUtils.planeDistance(point.lat(), point.lon(), lat, lon);
        }

        public double planeDistanceWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            return planeDistance(lat, lon);
        }

        public double geohashDistance(String geohash) {
            GeoPoint point = getValue();
            return GeoUtils.arcDistance(point.lat(), point.lon(), Geohash.decodeLatitude(geohash), Geohash.decodeLongitude(geohash));
        }

        public double geohashDistanceWithDefault(String geohash, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            return geohashDistance(geohash);
        }

        @Override
        public int getDimensionalType() {
            return size() == 0 ? -1 : 0;
        }

        @Override
        public GeoPoint getCentroid() {
            return size() == 0 ? null : geometrySupplier.getInternalCentroid();
        }

        @Override
        public double getMercatorWidth() {
            return 0;
        }

        @Override
        public double getMercatorHeight() {
            return 0;
        }

        @Override
        public GeoBoundingBox getBoundingBox() {
            return size() == 0 ? null : geometrySupplier.getInternalBoundingBox();
        }
    }

    public static class Booleans extends ScriptDocValues<Boolean> {

        private final ScriptFieldValues.BooleanValues supplier;

        public Booleans(ScriptFieldValues.BooleanValues supplier) {
            this.supplier = supplier;
        }

        public boolean getValue() {
            throwIfEmpty();
            return get(0);
        }

        @Override
        public Boolean get(int index) {
            throwIfEmpty();
            return supplier.get(index);
        }

        @Override
        public int size() {
            return supplier.size();
        }
    }

    public static class StringsSupplier implements ScriptFieldDocValues<String> {

        private final SortedBinaryDocValues in;
        private BytesRefBuilder[] values = new BytesRefBuilder[0];
        private int count;

        public StringsSupplier(SortedBinaryDocValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    // We need to make a copy here, because BytesBinaryDVLeafFieldData's SortedBinaryDocValues
                    // implementation reuses the returned BytesRef. Otherwise we would end up with the same BytesRef
                    // instance for all slots in the values array.
                    values[i].copyBytes(in.nextValue());
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        private void resize(int newSize) {
            count = newSize;
            if (newSize > values.length) {
                final int oldLength = values.length;
                values = ArrayUtil.grow(values, count);
                for (int i = oldLength; i < values.length; ++i) {
                    values[i] = new BytesRefBuilder();
                }
            }
        }

        protected String bytesToString(BytesRef bytesRef) {
            return bytesRef.utf8ToString();
        }

        @Override
        public String getInternal(int index) {
            return bytesToString(values[index].toBytesRef());
        }

        @Override
        public int size() {
            return count;
        }
    }

    public static class Strings extends ScriptDocValues<String> {

        public Strings(ScriptFieldDocValues<String> supplier) {
            super(supplier);
        }

        public String getValue() {
            return get(0);
        }

        @Override
        public String get(int index) {
            if (supplier.size() == 0) {
                throw new IllegalStateException(
                    "A document doesn't have a value for a field! "
                        + "Use doc[<field>].size()==0 to check if a document is missing a field!"
                );
            }
            return supplier.getInternal(index);
        }

        @Override
        public int size() {
            return supplier.size();
        }
    }

    public static final class BytesRefs extends ScriptDocValues<BytesRef> {

        public BytesRefs(ScriptFieldDocValues<BytesRef> supplier) {
            super(supplier);
        }

        public BytesRef getValue() {
            throwIfEmpty();
            return get(0);
        }

        @Override
        public BytesRef get(int index) {
            throwIfEmpty();
            return supplier.getInternal(index);
        }

        @Override
        public int size() {
            return supplier.size();
        }
    }
}
