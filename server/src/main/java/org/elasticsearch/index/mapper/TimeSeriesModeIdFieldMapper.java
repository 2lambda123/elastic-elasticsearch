/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.hash.MurmurHash3.Hash128;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * A mapper for the _id field. It does nothing since _id is neither indexed nor
 * stored, but we need to keep it so that its FieldType can be used to generate
 * queries.
 */
public class TimeSeriesModeIdFieldMapper extends IdFieldMapper {
    public static class Defaults {

        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setStored(true);  // TODO reconstruct the id on fetch from tsid and timestamp
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static final TimeSeriesModeIdFieldMapper INSTANCE = new TimeSeriesModeIdFieldMapper();

    public static final TypeParser PARSER = new FixedTypeParser(MappingParserContext::idFieldMapper);

    static final class IdFieldType extends TermBasedFieldType {
        IdFieldType() {
            super(NAME, true, true, false, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean isSearchable() {
            // The _id field is always searchable.
            return true;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new StoredValueFetcher(context.lookup(), NAME);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            return termsQuery(Arrays.asList(value), context);
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            return new MatchAllDocsQuery();
        }

        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
            failIfNotIndexed();
            BytesRef[] bytesRefs = values.stream().map(v -> {
                Object idObject = v;
                if (idObject instanceof BytesRef) {
                    idObject = ((BytesRef) idObject).utf8ToString();
                }
                return Uid.encodeId(idObject.toString());
            }).toArray(BytesRef[]::new);
            return new TermInSetQuery(name(), bytesRefs);
        }
    }

    private TimeSeriesModeIdFieldMapper() {
        super(new IdFieldType(), Lucene.KEYWORD_ANALYZER);
    }

    private static final long SEED = 0;

    public void createField(DocumentParserContext context, BytesRef tsid) {
        IndexableField[] timestampFields = context.rootDoc().getFields(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        if (timestampFields.length == 0) {
            throw new IllegalArgumentException(
                "data stream timestamp field [" + DataStreamTimestampFieldMapper.DEFAULT_PATH + "] is missing"
            );
        }
        long timestamp = timestampFields[0].numericValue().longValue();

        Hash128 hash = new Hash128();
        MurmurHash3.hash128(tsid.bytes, tsid.offset, tsid.length, SEED, hash);

        byte[] suffix = new byte[16];
        ByteUtils.writeLongLE(hash.h1, suffix, 0);
        ByteUtils.writeLongLE(timestamp, suffix, 8);   // TODO compare disk usage for LE and BE on timestamp

        IndexRouting.ExtractFromSource indexRouting = (IndexRouting.ExtractFromSource) IndexRouting.fromIndexMetadata(
            context.indexSettings().getIndexMetadata()
        );
        context.id(indexRouting.createId(context.sourceToParse().getXContentType(), context.sourceToParse().source(), suffix));
        assert Uid.isURLBase64WithoutPadding(context.id()); // Make sure we get to use Uid's nice optimizations

        BytesRef uidEncoded = Uid.encodeId(context.id());
        context.doc().add(new Field(NAME, uidEncoded, Defaults.FIELD_TYPE));
    }
}
