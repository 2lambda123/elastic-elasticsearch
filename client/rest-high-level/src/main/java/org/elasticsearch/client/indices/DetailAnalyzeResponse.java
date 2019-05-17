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

package org.elasticsearch.client.indices;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DetailAnalyzeResponse implements ToXContentFragment {

    private final boolean customAnalyzer;
    private final AnalyzeTokenList analyzer;
    private final CharFilteredText[] charfilters;
    private final AnalyzeTokenList tokenizer;
    private final AnalyzeTokenList[] tokenfilters;

    public DetailAnalyzeResponse(AnalyzeTokenList analyzer) {
        this(false, analyzer, null, null, null);
    }

    public DetailAnalyzeResponse(CharFilteredText[] charfilters, AnalyzeTokenList tokenizer, AnalyzeTokenList[] tokenfilters) {
        this(true, null, charfilters, tokenizer, tokenfilters);
    }

    public DetailAnalyzeResponse(boolean customAnalyzer,
                                 AnalyzeTokenList analyzer,
                                 CharFilteredText[] charfilters,
                                 AnalyzeTokenList tokenizer,
                                 AnalyzeTokenList[] tokenfilters) {
        this.customAnalyzer = customAnalyzer;
        this.analyzer = analyzer;
        this.charfilters = charfilters;
        this.tokenizer = tokenizer;
        this.tokenfilters = tokenfilters;
    }

    public AnalyzeTokenList analyzer() {
        return this.analyzer;
    }

    public CharFilteredText[] charfilters() {
        return this.charfilters;
    }

    public AnalyzeTokenList tokenizer() {
        return tokenizer;
    }

    public AnalyzeTokenList[] tokenfilters() {
        return tokenfilters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DetailAnalyzeResponse that = (DetailAnalyzeResponse) o;
        return customAnalyzer == that.customAnalyzer &&
            Objects.equals(analyzer, that.analyzer) &&
            Arrays.equals(charfilters, that.charfilters) &&
            Objects.equals(tokenizer, that.tokenizer) &&
            Arrays.equals(tokenfilters, that.tokenfilters);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(customAnalyzer, analyzer, tokenizer);
        result = 31 * result + Arrays.hashCode(charfilters);
        result = 31 * result + Arrays.hashCode(tokenfilters);
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.CUSTOM_ANALYZER, customAnalyzer);

        if (analyzer != null) {
            builder.startObject(Fields.ANALYZER);
            analyzer.toXContentWithoutObject(builder, params);
            builder.endObject();
        }

        if (charfilters != null) {
            builder.startArray(Fields.CHARFILTERS);
            for (CharFilteredText charfilter : charfilters) {
                charfilter.toXContent(builder, params);
            }
            builder.endArray();
        }

        if (tokenizer != null) {
            builder.startObject(Fields.TOKENIZER);
            tokenizer.toXContentWithoutObject(builder, params);
            builder.endObject();
        }

        if (tokenfilters != null) {
            builder.startArray(Fields.TOKENFILTERS);
            for (AnalyzeTokenList tokenfilter : tokenfilters) {
                tokenfilter.toXContent(builder, params);
            }
            builder.endArray();
        }
        return builder;
    }

    @SuppressWarnings("unchecked")
    private static <T> T[] fromList(Class<T> clazz, List<T> list) {
        if (list == null) {
            return null;
        }
        return list.toArray((T[]) Array.newInstance(clazz, 0));
    }

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<DetailAnalyzeResponse, Void> PARSER = new ConstructingObjectParser<>("detail",
        true, args -> new DetailAnalyzeResponse((boolean) args[0], (AnalyzeTokenList) args[1],
        fromList(CharFilteredText.class, (List<CharFilteredText>)args[2]),
        (AnalyzeTokenList) args[3],
        fromList(AnalyzeTokenList.class, (List<AnalyzeTokenList>)args[4])));

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField(Fields.CUSTOM_ANALYZER));
        PARSER.declareObject(optionalConstructorArg(), AnalyzeTokenList.PARSER, new ParseField(Fields.ANALYZER));
        PARSER.declareObjectArray(optionalConstructorArg(), CharFilteredText.PARSER, new ParseField(Fields.CHARFILTERS));
        PARSER.declareObject(optionalConstructorArg(), AnalyzeTokenList.PARSER, new ParseField(Fields.TOKENIZER));
        PARSER.declareObjectArray(optionalConstructorArg(), AnalyzeTokenList.PARSER, new ParseField(Fields.TOKENFILTERS));
    }

    public static DetailAnalyzeResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    static final class Fields {
        static final String NAME = "name";
        static final String FILTERED_TEXT = "filtered_text";
        static final String CUSTOM_ANALYZER = "custom_analyzer";
        static final String ANALYZER = "analyzer";
        static final String CHARFILTERS = "charfilters";
        static final String TOKENIZER = "tokenizer";
        static final String TOKENFILTERS = "tokenfilters";
    }

    public static class AnalyzeTokenList implements ToXContentObject {
        private final String name;
        private final AnalyzeResponse.AnalyzeToken[] tokens;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AnalyzeTokenList that = (AnalyzeTokenList) o;
            return Objects.equals(name, that.name) &&
                Arrays.equals(tokens, that.tokens);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(name);
            result = 31 * result + Arrays.hashCode(tokens);
            return result;
        }

        public AnalyzeTokenList(String name, AnalyzeResponse.AnalyzeToken[] tokens) {
            this.name = name;
            this.tokens = tokens;
        }

        public String getName() {
            return name;
        }

        public AnalyzeResponse.AnalyzeToken[] getTokens() {
            return tokens;
        }

        XContentBuilder toXContentWithoutObject(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.NAME, this.name);
            builder.startArray(AnalyzeResponse.Fields.TOKENS);
            if (tokens != null) {
                for (AnalyzeResponse.AnalyzeToken token : tokens) {
                    token.toXContent(builder, params);
                }
            }
            builder.endArray();
            return builder;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            toXContentWithoutObject(builder, params);
            builder.endObject();
            return builder;
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<AnalyzeTokenList, Void> PARSER = new ConstructingObjectParser<>("token_list",
            true, args -> new AnalyzeTokenList((String) args[0],
            fromList(AnalyzeResponse.AnalyzeToken.class, (List<AnalyzeResponse.AnalyzeToken>)args[1])));

        static {
            PARSER.declareString(constructorArg(), new ParseField(Fields.NAME));
            PARSER.declareObjectArray(constructorArg(), (p, c) -> AnalyzeResponse.AnalyzeToken.fromXContent(p),
                new ParseField(AnalyzeResponse.Fields.TOKENS));
        }

        public static AnalyzeTokenList fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

    }

    public static class CharFilteredText implements ToXContentObject {
        private final String name;
        private final String[] texts;

        public CharFilteredText(String name, String[] texts) {
            this.name = name;
            if (texts != null) {
                this.texts = texts;
            } else {
                this.texts = Strings.EMPTY_ARRAY;
            }
        }

        public String getName() {
            return name;
        }

        public String[] getTexts() {
            return texts;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.NAME, name);
            builder.array(Fields.FILTERED_TEXT, texts);
            builder.endObject();
            return builder;
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<CharFilteredText, Void> PARSER = new ConstructingObjectParser<>("char_filtered_text",
            true, args -> new CharFilteredText((String) args[0], ((List<String>) args[1]).toArray(new String[0])));

        static {
            PARSER.declareString(constructorArg(), new ParseField(Fields.NAME));
            PARSER.declareStringArray(constructorArg(), new ParseField(Fields.FILTERED_TEXT));
        }

        public static CharFilteredText fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CharFilteredText that = (CharFilteredText) o;
            return Objects.equals(name, that.name) &&
                Arrays.equals(texts, that.texts);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(name);
            result = 31 * result + Arrays.hashCode(texts);
            return result;
        }
    }
}
