/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.constantkeyword.mapper;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.FuzzyTermsEnum;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.ConstantKeywordIndexFieldData;
import org.elasticsearch.index.mapper.ConstantFieldType;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryShardContext;

/**
 * A {@link FieldMapper} that assigns every document the same value.
 */
public class ConstantKeywordFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "constant_keyword";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new ConstantKeywordFieldType();
        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, ConstantKeywordFieldMapper> {

        public Builder(String name, String value) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
            fieldType().setValue(value);
        }

        @Override
        public ConstantKeywordFieldType fieldType() {
            return (ConstantKeywordFieldType) super.fieldType();
        }

        @Override
        public ConstantKeywordFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new ConstantKeywordFieldMapper(
                    name, fieldType, defaultFieldType,
                    context.indexSettings());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?,?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            final Object value = node.remove("value");
            if (value == null) {
                throw new MapperParsingException("Property [value] of field [" + name + "] is required and can't be null.");
            }
            if (value instanceof Number == false && value instanceof CharSequence == false) {
                throw new MapperParsingException("Property [value] of field [" + name +
                        "] must be a number or a string, but got [" + value + "]");
            }
            return new ConstantKeywordFieldMapper.Builder(name, value.toString());
        }
    }

    public static final class ConstantKeywordFieldType extends ConstantFieldType {

        private String value;

        public ConstantKeywordFieldType() {
            super();
        }

        protected ConstantKeywordFieldType(ConstantKeywordFieldType ref) {
            super(ref);
            this.value = ref.value;
        }

        public ConstantKeywordFieldType clone() {
            return new ConstantKeywordFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            if (super.equals(o) == false) {
                return false;
            }
            ConstantKeywordFieldType other = (ConstantKeywordFieldType) o;
            return Objects.equals(value, other.value);
        }

        @Override
        public void checkCompatibility(MappedFieldType otherFT, List<String> conflicts) {
            super.checkCompatibility(otherFT, conflicts);
            ConstantKeywordFieldType other = (ConstantKeywordFieldType) otherFT;
            if (Objects.equals(value, other.value) == false) {
                conflicts.add("mapper [" + name() + "] has different [value]: [" + value + "] vs. [" + other.value + "]");
            }
        }

        @Override
        public int hashCode() {
            return 31 * super.hashCode() + Objects.hashCode(value);
        }

        /** Return the value that this field wraps. */
        public String value() {
            return value;
        }

        /** Set the value. */
        public void setValue(String value) {
            checkIfFrozen();
            this.value = Objects.requireNonNull(value);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            return new ConstantKeywordIndexFieldData.Builder(mapperService -> value);
        }

        private static String valueToString(Object v) {
            if (v instanceof BytesRef) {
                return ((BytesRef) v).utf8ToString();
            } else {
                return v.toString();
            }
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            if (Objects.equals(valueToString(value), this.value)) {
                return new MatchAllDocsQuery();
            } else {
                return new MatchNoDocsQuery();
            }
        }

        @Override
        public Query termsQuery(List<?> values, QueryShardContext context) {
            for (Object v : values) {
                if (Objects.equals(valueToString(v), value)) {
                    return new MatchAllDocsQuery();
                }
            }
            return new MatchNoDocsQuery();
        }

        @Override
        public Query prefixQuery(String value,
                @Nullable MultiTermQuery.RewriteMethod method,
                QueryShardContext context) {
            if (this.value.startsWith(value)) {
                return new MatchAllDocsQuery();
            } else {
                return new MatchNoDocsQuery();
            }
        }

        public Query wildcardQuery(String value,
                @Nullable MultiTermQuery.RewriteMethod method,
                QueryShardContext context) {
            if (Regex.simpleMatch(value, this.value)) {
                return new MatchAllDocsQuery();
            } else {
                return new MatchNoDocsQuery();
            }
        }

        @Override
        public Query rangeQuery(
                Object lowerTerm, Object upperTerm,
                boolean includeLower, boolean includeUpper,
                ShapeRelation relation, ZoneId timeZone, DateMathParser parser,
                QueryShardContext context) {
            final BytesRef valueAsBytesRef = new BytesRef(value);
            if (lowerTerm != null && BytesRefs.toBytesRef(lowerTerm).compareTo(valueAsBytesRef) >= (includeLower ? 1 : 0)) {
                return new MatchNoDocsQuery();
            }
            if (upperTerm != null && valueAsBytesRef.compareTo(BytesRefs.toBytesRef(upperTerm)) >= (includeUpper ? 1 : 0)) {
                return new MatchNoDocsQuery();
            }
            return new MatchAllDocsQuery();
        }

        @Override
        public Query fuzzyQuery(Object term, Fuzziness fuzziness, int prefixLength, int maxExpansions,
                boolean transpositions) {
            final String termAsString = BytesRefs.toString(term);
            final int maxEdits = fuzziness.asDistance(termAsString);
            final Automaton automaton = FuzzyTermsEnum.buildAutomaton(termAsString, prefixLength, transpositions, maxEdits);
            final CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(automaton);
            if (runAutomaton.run(this.value)) {
                return new MatchAllDocsQuery();
            } else {
                return new MatchNoDocsQuery();
            }
        }

        @Override
        public Query regexpQuery(String value, int flags, int maxDeterminizedStates,
                MultiTermQuery.RewriteMethod method, QueryShardContext context) {
            final Automaton automaton = new RegExp(value, flags).toAutomaton(maxDeterminizedStates);
            final CharacterRunAutomaton runAutomaton = new CharacterRunAutomaton(automaton);
            if (runAutomaton.run(this.value)) {
                return new MatchAllDocsQuery();
            } else {
                return new MatchNoDocsQuery();
            }
        }
    }

    protected ConstantKeywordFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                 Settings indexSettings) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, MultiFields.empty(), CopyTo.empty());
    }

    @Override
    protected ConstantKeywordFieldMapper clone() {
        return (ConstantKeywordFieldMapper) super.clone();
    }

    @Override
    public ConstantKeywordFieldType fieldType() {
        return (ConstantKeywordFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        String value;
        if (context.externalValueSet()) {
            value = context.externalValue().toString();
        } else {
            XContentParser parser = context.parser();
            value =  parser.textOrNull();
        }

        if (Objects.equals(fieldType().value, value) == false) {
            throw new IllegalArgumentException("[constant_keyword] field [" + name() +
                    "] only accepts values that are equal to the value defined in the mappings [" + fieldType().value() +
                    "], but got [" + value + "]");
        }
    }
    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        builder.field("value", fieldType().value());
    }
}
