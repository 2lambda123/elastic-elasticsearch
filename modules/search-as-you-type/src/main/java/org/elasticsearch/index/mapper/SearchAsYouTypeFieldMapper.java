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

package org.elasticsearch.index.mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.shingle.FixedShingleFilter;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.index.mapper.TypeParsers.nodeIndexOptionValue;
import static org.elasticsearch.index.mapper.TypeParsers.parseMultiField;

public class SearchAsYouTypeFieldMapper extends FieldMapper implements ArrayValueMapperParser {

    private static final Logger LOG = LogManager.getLogger(SearchAsYouTypeFieldMapper.class);

    public static class Defaults {

        public static final int MIN_GRAM = 1;
        public static final int MAX_GRAM = 20;
        public static final int MAX_SHINGLE_SIZE = 2;

        public static final MappedFieldType FIELD_TYPE = new SearchAsYouTypeFieldType();

        static {
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            FIELD_TYPE.freeze();
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?, ?> parse(String name,
                                          Map<String, Object> node,
                                          ParserContext parserContext) throws MapperParsingException {

            final SearchAsYouTypeFieldMapper.Builder builder = new SearchAsYouTypeFieldMapper.Builder(name);

            NamedAnalyzer analyzer = parserContext.getIndexAnalyzers().getDefaultIndexAnalyzer();
            int minGram = Defaults.MIN_GRAM;
            int maxGram = Defaults.MAX_GRAM;
            int maxShingleSize = Defaults.MAX_SHINGLE_SIZE;

            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                final Map.Entry<String, Object> entry = iterator.next();
                final String fieldName = entry.getKey();
                final Object fieldNode = entry.getValue();
                if (fieldName.equals("type")) {
                    continue;
                } else if (fieldName.equals("index_options")) {
                    builder.indexOptions(nodeIndexOptionValue(fieldNode));
                    iterator.remove();
                } else if (fieldName.equals("fields")) {
                    parseMultiField(builder, name, parserContext, fieldName, fieldNode);
                    iterator.remove();
                } else if (fieldName.equals("analyzer")) {
                    final String analyzerName = fieldNode.toString();
                    analyzer = parserContext.getIndexAnalyzers().get(analyzerName);
                    if (analyzer == null) {
                        throw new MapperParsingException("Analyzer [" + analyzerName + "] not found for field  [" + name + "]");
                    }
                    iterator.remove();
                } else if (fieldName.equals("min_gram")) {
                    minGram = XContentMapValues.nodeIntegerValue(fieldNode);
                    iterator.remove();
                } else if (fieldName.equals("max_gram")) {
                    maxGram = XContentMapValues.nodeIntegerValue(fieldNode);
                    iterator.remove();
                } else if (fieldName.equals("max_shingle_size")) {
                    maxShingleSize = XContentMapValues.nodeIntegerValue(fieldNode);
                    iterator.remove();
                }
            }

            builder.indexAnalyzer(analyzer);
            builder.searchAnalyzer(analyzer);
            builder.edgeNGrams(minGram, maxGram);
            builder.maxShingleSize(maxShingleSize);

            return builder;
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, SearchAsYouTypeFieldMapper> {

        private int minGram;
        private int maxGram;
        private int maxShingleSize;

        public Builder(String name) {

            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            this.builder = this;
        }

        public Builder edgeNGrams(int minGram, int maxGram) {
            if (minGram < 1) {
                throw new MapperParsingException("[min_gram] must be at least 1, got [" + minGram + "]");
            }

            if (minGram > maxGram) {
                throw new MapperParsingException("[min_gram] value [" + minGram + "] must not be greater than [max_gram] value " +
                    "[" + maxGram + "]");
            }

            this.minGram = minGram;
            this.maxGram = maxGram;

            return builder;
        }

        public Builder maxShingleSize(int maxShingleSize) {
            if (maxShingleSize < 2) {
                throw new MapperParsingException("[max_shingle_size] must be at least 2, got [" + maxShingleSize + "]");
            }

            this.maxShingleSize = maxShingleSize;

            return builder;
        }

        @Override
        public SearchAsYouTypeFieldType fieldType() {
            return (SearchAsYouTypeFieldType) this.fieldType;
        }

        @Override
        public SearchAsYouTypeFieldMapper build(BuilderContext context) {
            setupFieldType(context);

            final NamedAnalyzer indexAnalyzer = fieldType().indexAnalyzer();
            final NamedAnalyzer searchAnalyzer = fieldType().searchAnalyzer();
            if (indexAnalyzer.equals(searchAnalyzer) == false) {
                throw new MapperParsingException("Index and search analyzers must be the same");
            }

            final Set<SuggesterizedFieldType> suggesterizedFieldTypes = new HashSet<>();

            final SuggesterizedFieldType withEdgeNgrams = new SuggesterizedFieldType(name() + "._with_edge_ngrams");
            final SearchAsYouTypeAnalyzer wrappedWithEdgeNGrams = SearchAsYouTypeAnalyzer.withEdgeNGrams(indexAnalyzer, minGram, maxGram);
            withEdgeNgrams.setIndexAnalyzer(new NamedAnalyzer(indexAnalyzer.name(), AnalyzerScope.INDEX, wrappedWithEdgeNGrams));
            withEdgeNgrams.setSearchAnalyzer(indexAnalyzer);
            suggesterizedFieldTypes.add(withEdgeNgrams);

            for (int i = 2; i < maxShingleSize; i++) {
                final int numberOfShingles = i;
                final SuggesterizedFieldType withShingles = new SuggesterizedFieldType(name() + "._with_" + numberOfShingles + "_shingles");
                final SuggesterizedFieldType withShinglesAndEdgeNGrams = new SuggesterizedFieldType(name() + "._with_" + numberOfShingles +
                    "_shingles_and_edge_ngrams");

                final SearchAsYouTypeAnalyzer withShinglesAnalyzer = SearchAsYouTypeAnalyzer.withShingles(indexAnalyzer, numberOfShingles);
                final SearchAsYouTypeAnalyzer withShinglesAndEdgeNGramsAnalyzer =
                    SearchAsYouTypeAnalyzer.withShinglesAndEdgeNGrams(indexAnalyzer, numberOfShingles, minGram, maxGram);

                withShingles.setIndexAnalyzer(new NamedAnalyzer(indexAnalyzer.name(), AnalyzerScope.INDEX, withShinglesAnalyzer));
                withShingles.setSearchAnalyzer(new NamedAnalyzer(indexAnalyzer.name(), AnalyzerScope.INDEX, withShinglesAnalyzer));

                withShinglesAndEdgeNGrams.setIndexAnalyzer(
                    new NamedAnalyzer(indexAnalyzer.name(), AnalyzerScope.INDEX, withShinglesAndEdgeNGramsAnalyzer));
                withShinglesAndEdgeNGrams.setSearchAnalyzer(
                    new NamedAnalyzer(indexAnalyzer.name(), AnalyzerScope.INDEX, withShinglesAnalyzer));

                suggesterizedFieldTypes.add(withShingles);
                suggesterizedFieldTypes.add(withShinglesAndEdgeNGrams);
            }

            final Set<SuggesterizedFieldMapper> suggesterizedFieldMappers = suggesterizedFieldTypes.stream()
                .map(suggesterizedFieldType -> new SuggesterizedFieldMapper(suggesterizedFieldType, context.indexSettings()))
                .collect(Collectors.toSet());
            return new SearchAsYouTypeFieldMapper(
                name(),
                fieldType(),
                suggesterizedFieldMappers,
                context.indexSettings(),
                multiFieldsBuilder.build(this, context),
                copyTo
            );
        }
    }

//    private static AnalyzerWrapper wrapAnalyzerWithTokenFilter(NamedAnalyzer analyzer, Function<TokenStream, TokenFilter> function) {
//
//        return new AnalyzerWrapper(analyzer.getReuseStrategy()) {
//
//            @Override
//            protected Analyzer getWrappedAnalyzer(String fieldName) {
//                return analyzer;
//            }
//
//            @Override
//            protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
//                TokenFilter filter = function.apply(components.getTokenStream());
//                return new TokenStreamComponents(components.getTokenizer(), filter);
//            }
//        };
//    }

    public static class SearchAsYouTypeAnalyzer extends AnalyzerWrapper {

        private final Analyzer delegate;
        private final boolean withShingles;
        private final int maxShingleSize;
        private final boolean withEdgeNGrams;
        private final int minGram;
        private final int maxGram;

        private SearchAsYouTypeAnalyzer(Analyzer delegate,
                                        boolean withShingles,
                                        int maxShingleSize,
                                        boolean withEdgeNGrams,
                                        int minGram,
                                        int maxGram) {

            super(delegate.getReuseStrategy());
            this.delegate = delegate;
            this.withShingles = withShingles;
            this.maxShingleSize = maxShingleSize;
            this.withEdgeNGrams = withEdgeNGrams;
            this.minGram = minGram;
            this.maxGram = maxGram;
        }

        public static SearchAsYouTypeAnalyzer withShingles(Analyzer delegate, int maxShingleSize) {
            return new SearchAsYouTypeAnalyzer(delegate, true, maxShingleSize, false, -1, -1);
        }

        public static SearchAsYouTypeAnalyzer withEdgeNGrams(Analyzer delegate, int minGram, int maxGram) {
            return new SearchAsYouTypeAnalyzer(delegate, false, -1, true, minGram, maxGram);
        }

        public static SearchAsYouTypeAnalyzer withShinglesAndEdgeNGrams(Analyzer delegate, int maxShingleSize, int minGram, int maxGram) {
            return new SearchAsYouTypeAnalyzer(delegate, true, maxShingleSize, true, minGram, maxGram);
        }

        @Override
        protected Analyzer getWrappedAnalyzer(String fieldName) {
            return delegate;
        }

        @Override
        protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
            // TODO we must find a way to add the last unigram term (michael jackson -> jackson)
            TokenStream tokenStream = components.getTokenStream();
            if (withShingles) {
                tokenStream = new FixedShingleFilter(tokenStream, maxShingleSize);
            }
            if (withEdgeNGrams) {
                tokenStream = new EdgeNGramTokenFilter(tokenStream, minGram, maxGram, true);
            }
            return new TokenStreamComponents(components.getTokenizer(), tokenStream);
        }

        public boolean isWithEdgeNGrams() {
            return withEdgeNGrams;
        }

        public int getMaxShingleSize() {
            return maxShingleSize;
        }

        public boolean isWithShingles() {
            return withShingles;
        }

        public int getMinGram() {
            return minGram;
        }

        public int getMaxGram() {
            return maxGram;
        }
    }

    public static class SuggesterizedFieldMapper extends FieldMapper implements ArrayValueMapperParser { // todo better name

        static final String CONTENT_TYPE = "suggesterized";

        protected SuggesterizedFieldMapper(SuggesterizedFieldType fieldType, Settings indexSettings) {
            super(fieldType.name(), fieldType, fieldType, indexSettings, MultiFields.empty(), CopyTo.empty());
        }

        void addField(String value, List<IndexableField> fields) {
            fields.add(new Field(fieldType().name(), value, fieldType));
        }

        @Override
        protected void parseCreateField(ParseContext context, List<IndexableField> fields) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String contentType() {
            return CONTENT_TYPE;
        }
    }

    public static class SuggesterizedFieldType extends TermBasedFieldType {

        SuggesterizedFieldType(String name) {
            setName(name);
            setOmitNorms(true);
            setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS); // todo do we need to always set omitNorms and INdexOptions
            setTokenized(true);
        }

        SuggesterizedFieldType(SuggesterizedFieldType reference) {
            super(reference);
        }

        @Override
        public SuggesterizedFieldType clone() {
            return new SuggesterizedFieldType(this);
        }

        @Override
        public String typeName() {
            return SuggesterizedFieldMapper.CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
        }
    }


    public static class SearchAsYouTypeFieldType extends TermBasedFieldType {

        private SuggesterizedFieldType suggesterizedFieldType;

        public SearchAsYouTypeFieldType() {
            setTokenized(true);
            // todo we should take in the subfield type here
        }

        public SearchAsYouTypeFieldType(SearchAsYouTypeFieldType reference) {
            super(reference);
            if (reference.suggesterizedFieldType != null) {
                this.suggesterizedFieldType = reference.suggesterizedFieldType.clone();
            }
        }

        public SuggesterizedFieldType getSuggesterizedFieldType() {
            return this.suggesterizedFieldType;
        }

        public void setSuggesterizedFieldType(SuggesterizedFieldType suggesterizedFieldType) {
            checkIfFrozen();
            this.suggesterizedFieldType = suggesterizedFieldType;
        }

        @Override
        public MappedFieldType clone() {
            return new SearchAsYouTypeFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
        }
    }

    public static final String CONTENT_TYPE = "search_as_you_type";

    private Set<SuggesterizedFieldMapper> suggesterizedFieldMappers;

    public SearchAsYouTypeFieldMapper(String simpleName,
                                      MappedFieldType fieldType,
                                      Set<SuggesterizedFieldMapper> suggesterizedFieldMappers,
                                      Settings indexSettings,
                                      MultiFields multiFields,
                                      CopyTo copyTo) {

        super(simpleName, fieldType, Defaults.FIELD_TYPE, indexSettings, multiFields, copyTo);
        this.suggesterizedFieldMappers = suggesterizedFieldMappers;
    }

    @Override
    public SearchAsYouTypeFieldType fieldType() {
        return (SearchAsYouTypeFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        final String value = context.externalValueSet()
            ? context.externalValue().toString()
            : context.parser().textOrNull();

        if (value == null) {
            return;
        }

        Field field = new Field(fieldType().name(), value, fieldType());
        fields.add(field);

        if (fieldType().omitNorms()) {
            createFieldNamesField(context, fields);
        }

        for (SuggesterizedFieldMapper fieldMapper : suggesterizedFieldMappers) {
            fieldMapper.addField(value, fields);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(simpleName());
        builder.field("type", CONTENT_TYPE);
        if (fieldType().indexAnalyzer().name().equals("default") == false) {
            builder.field("analyzer", fieldType().indexAnalyzer().name());
        }
        builder.endObject();
        return builder;
        // todo we should provide more info about the under the hood fields, or at least how they're analyzed
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
