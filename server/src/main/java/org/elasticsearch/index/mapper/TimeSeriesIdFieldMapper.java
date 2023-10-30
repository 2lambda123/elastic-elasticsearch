/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.Murmur3Hasher;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.SortedOrdinalsIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Mapper for {@code _tsid} field included generated when the index is
 * {@link IndexMode#TIME_SERIES organized into time series}.
 */
public class TimeSeriesIdFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_tsid";
    public static final String CONTENT_TYPE = "_tsid";
    public static final TimeSeriesIdFieldType FIELD_TYPE = new TimeSeriesIdFieldType();
    public static final TimeSeriesIdFieldMapper INSTANCE = new TimeSeriesIdFieldMapper();

    // NOTE: used by {@link TimeSeriesIdFieldMapper#decodeTsid(StreamInput)} )}. Remove both if using _tsid hashing
    public static final int TSID_HASH_SENTINEL = 0xBAADCAFE;

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder().init(this);
    }

    public static class Builder extends MetadataFieldMapper.Builder {

        protected Builder() {
            super(NAME);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return EMPTY_PARAMETERS;
        }

        @Override
        public TimeSeriesIdFieldMapper build() {
            return INSTANCE;
        }
    }

    public static final TypeParser PARSER = new FixedTypeParser(c -> c.getIndexSettings().getMode().timeSeriesIdFieldMapper());

    public static final class TimeSeriesIdFieldType extends MappedFieldType {
        private TimeSeriesIdFieldType() {
            super(NAME, false, false, true, TextSearchInfo.NONE, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new DocValueFetcher(docValueFormat(format, null), context.getForField(this, FielddataOperation.SEARCH));
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            return DocValueFormat.TIME_SERIES_ID;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            // TODO don't leak the TSID's binary format into the script
            return new SortedOrdinalsIndexFieldData.Builder(
                name(),
                CoreValuesSourceType.KEYWORD,
                (dv, n) -> new DelegateDocValuesField(
                    new ScriptDocValues.Strings(new ScriptDocValues.StringsSupplier(FieldData.toString(dv))),
                    n
                )
            );
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("[" + NAME + "] is not searchable");
        }
    }

    private TimeSeriesIdFieldMapper() {
        super(FIELD_TYPE);
    }

    @Override
    public void postParse(DocumentParserContext context) throws IOException {
        assert fieldType().isIndexed() == false;

        final TimeSeriesIdBuilder timeSeriesIdBuilder = (TimeSeriesIdBuilder) context.getDocumentFields();
        final BytesRef timeSeriesId = timeSeriesIdBuilder.build().toBytesRef();
        context.doc().add(new SortedDocValuesField(fieldType().name(), timeSeriesIdBuilder.similarityHash().toBytesRef()));
        TsidExtractingIdFieldMapper.createField(context, timeSeriesIdBuilder.routingBuilder, timeSeriesId);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        return SourceLoader.SyntheticFieldLoader.NOTHING;
    }

    /**
     * Decode the {@code _tsid} into a human readable map.
     */
    public static Map<String, Object> decodeTsid(StreamInput in) {
        try {
            int sizeOrTsidHashSentinel = in.readVInt();
            if (sizeOrTsidHashSentinel == TSID_HASH_SENTINEL) {
                final BytesRef bytesRef = in.readBytesRef();
                return Collections.singletonMap("_tsid", Base64.getUrlEncoder().withoutPadding().encodeToString(bytesRef.bytes));
            }
            Map<String, Object> result = new LinkedHashMap<>(sizeOrTsidHashSentinel);

            for (int i = 0; i < sizeOrTsidHashSentinel; i++) {
                String name = in.readBytesRef().utf8ToString();

                int type = in.read();
                switch (type) {
                    case (byte) 's' -> // parse a string
                        result.put(name, in.readBytesRef().utf8ToString());
                    case (byte) 'l' -> // parse a long
                        result.put(name, in.readLong());
                    case (byte) 'u' -> { // parse an unsigned_long
                        Object ul = DocValueFormat.UNSIGNED_LONG_SHIFTED.format(in.readLong());
                        result.put(name, ul);
                    }
                    default -> throw new IllegalArgumentException("Cannot parse [" + name + "]: Unknown type [" + type + "]");
                }
            }
            return result;
        } catch (IOException | IllegalArgumentException e) {
            throw new IllegalArgumentException("Error formatting " + NAME + ": " + e.getMessage(), e);
        }
    }

    public static class TimeSeriesIdBuilder implements DocumentFields {

        public static final int MAX_DIMENSIONS = 512;

        private record DimensionDataHolder(BytesRef name, BytesRef value) {}

        private final Murmur3Hasher tsidHasher = new Murmur3Hasher(0);

        /**
         * A sorted set of the serialized values of dimension fields that will be used
         * for generating the _tsid field. The map will be used by {@link TimeSeriesIdFieldMapper}
         * to build the _tsid field for the document.
         */
        private final SortedSet<DimensionDataHolder> dimensions = new TreeSet<>(Comparator.comparing(o -> o.name));
        private final Set<String> metrics = new TreeSet<>();
        /**
         * Builds the routing. Used for building {@code _id}. If null then skipped.
         */
        @Nullable
        private final IndexRouting.ExtractFromSource.Builder routingBuilder;

        public TimeSeriesIdBuilder(@Nullable IndexRouting.ExtractFromSource.Builder routingBuilder) {
            this.routingBuilder = routingBuilder;
        }

        public BytesReference build() throws IOException {
            if (dimensions.isEmpty()) {
                throw new IllegalArgumentException("Dimension fields are missing.");
            }

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.writeVInt(dimensions.size());
                for (DimensionDataHolder entry : dimensions) {
                    out.writeBytesRef(entry.name);
                    out.writeBytesRef(entry.value);
                }
                return out.bytes();
            }
        }

        /**
         * Here we build the hash of the tsid using a similarity function so that we have a result
         * with the following pattern:
         *
         * hash128(catenate(dimension field names)) +
         * hash128(catenate(metric field names)) +
         * foreach(dimension field value, limit = MAX_DIMENSIONS) { hash32(dimension field value) } +
         * hash128(catenate(dimension field values))
         *
         * The idea is to be able to place 'similar' time series close to each other. Two time series
         * are considered 'similar' if they share the same dimensions (names and values).
         */
        public BytesReference similarityHash() throws IOException {
            // NOTE: hash all dimension field names
            int numberOfDimensions = Math.min(MAX_DIMENSIONS, dimensions.size());
            int tsidHashIndex = 0;
            byte[] tsidHash = new byte[16 + 16 + 16 + 4 * numberOfDimensions];

            tsidHasher.reset();
            for (final DimensionDataHolder dimension : dimensions) {
                tsidHasher.update(dimension.name.bytes);
            }
            tsidHashIndex = writeHash128(tsidHasher.digestHash(), tsidHash, tsidHashIndex);

            // NOTE: hash all metric field names
            tsidHasher.reset();
            for (final String metric : metrics) {
                tsidHasher.update(metric.getBytes(StandardCharsets.UTF_8));
            }
            tsidHashIndex = writeHash128(tsidHasher.digestHash(), tsidHash, tsidHashIndex);

            // NOTE: concatenate all dimension value hashes up to a certain number of dimensions
            int tsidHashStartIndex = tsidHashIndex;
            for (final DimensionDataHolder dimension : dimensions) {
                if ((tsidHashIndex - tsidHashStartIndex) >= 4 * numberOfDimensions) break;
                ByteUtils.writeIntLE(
                    StringHelper.murmurhash3_x86_32(dimension.value.bytes, dimension.value.offset, dimension.value.length, 0),
                    tsidHash,
                    tsidHashIndex
                );
                tsidHashIndex += 4;
            }

            // NOTE: hash all dimension field allValues
            tsidHasher.reset();
            for (final DimensionDataHolder dimension : dimensions) {
                tsidHasher.update(dimension.value.bytes);
            }
            tsidHashIndex = writeHash128(tsidHasher.digestHash(), tsidHash, tsidHashIndex);

            assert tsidHashIndex == tsidHash.length;
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.writeVInt(TSID_HASH_SENTINEL);
                out.writeBytesRef(new BytesRef(tsidHash, 0, tsidHash.length));
                return out.bytes();
            }
        }

        private int writeHash128(final MurmurHash3.Hash128 hash128, byte[] buffer, int tsidHashIndex) {
            ByteUtils.writeLongLE(hash128.h1, buffer, tsidHashIndex);
            tsidHashIndex += 8;
            ByteUtils.writeLongLE(hash128.h2, buffer, tsidHashIndex);
            tsidHashIndex += 8;
            return tsidHashIndex;
        }

        @Override
        public void addKeywordDimension(String fieldName, BytesRef utf8Value) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.write((byte) 's');
                /*
                 * Write in utf8 instead of StreamOutput#writeString which is utf-16-ish
                 * so it's easier for folks to reason about the space taken up. Mostly
                 * it'll be smaller too.
                 */
                out.writeBytesRef(utf8Value);
                add(fieldName, out.bytes());

                if (routingBuilder != null) {
                    routingBuilder.addMatching(fieldName, utf8Value);
                }
            } catch (IOException e) {
                throw new IllegalArgumentException("Dimension field cannot be serialized.", e);
            }
        }

        @Override
        public void addIpDimension(String fieldName, InetAddress value) {
            addKeywordDimension(fieldName, NetworkAddress.format(value));
        }

        @Override
        public void addLongDimension(String fieldName, long value) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.write((byte) 'l');
                out.writeLong(value);
                add(fieldName, out.bytes());
            } catch (IOException e) {
                throw new IllegalArgumentException("Dimension field cannot be serialized.", e);
            }
        }

        @Override
        public void addUnsignedLongDimension(String fieldName, long value) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                Object ul = DocValueFormat.UNSIGNED_LONG_SHIFTED.format(value);
                if (ul instanceof Long l) {
                    out.write((byte) 'l');
                    out.writeLong(l);
                } else {
                    out.write((byte) 'u');
                    out.writeLong(value);
                }
                add(fieldName, out.bytes());
            } catch (IOException e) {
                throw new IllegalArgumentException("Dimension field cannot be serialized.", e);
            }
        }

        @Override
        public void addMetric(String fieldName) {
            metrics.add(fieldName);
        }

        private void add(String fieldName, BytesReference encoded) throws IOException {
            final DimensionDataHolder dimension = new DimensionDataHolder(new BytesRef(fieldName), encoded.toBytesRef());
            if (dimensions.contains(dimension)) {
                throw new IllegalArgumentException("Dimension field [" + fieldName + "] cannot be a multi-valued field.");
            }
            dimensions.add(dimension);
        }
    }

    public static Map<String, Object> decodeTsid(BytesRef bytesRef) {
        try (StreamInput input = new BytesArray(bytesRef).streamInput()) {
            return decodeTsid(input);
        } catch (IOException ex) {
            throw new IllegalArgumentException("Dimension field cannot be deserialized.", ex);
        }
    }
}
