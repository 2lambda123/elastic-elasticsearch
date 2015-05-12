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


package org.elasticsearch.search.aggregations.bucket.significant.heuristics;


import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.Script.ScriptField;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptParameterParser;
import org.elasticsearch.script.ScriptParameterParser.ScriptParameterValue;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

public class ScriptHeuristic extends SignificanceHeuristic {

    protected static final ParseField NAMES_FIELD = new ParseField("script_heuristic");
    private final LongAccessor subsetSizeHolder;
    private final LongAccessor supersetSizeHolder;
    private final LongAccessor subsetDfHolder;
    private final LongAccessor supersetDfHolder;
    ExecutableScript searchScript = null;
    Script script;

    public static final SignificanceHeuristicStreams.Stream STREAM = new SignificanceHeuristicStreams.Stream() {
        @Override
        public SignificanceHeuristic readResult(StreamInput in) throws IOException {
            Script script = Script.readScript(in);
            return new ScriptHeuristic(null, script);
        }

        @Override
        public String getName() {
            return NAMES_FIELD.getPreferredName();
        }
    };

    public ScriptHeuristic(ExecutableScript searchScript, Script script) {
        subsetSizeHolder = new LongAccessor();
        supersetSizeHolder = new LongAccessor();
        subsetDfHolder = new LongAccessor();
        supersetDfHolder = new LongAccessor();
        this.searchScript = searchScript;
        if (searchScript != null) {
            searchScript.setNextVar("_subset_freq", subsetDfHolder);
            searchScript.setNextVar("_subset_size", subsetSizeHolder);
            searchScript.setNextVar("_superset_freq", supersetDfHolder);
            searchScript.setNextVar("_superset_size", supersetSizeHolder);
        }
        this.script = script;


    }

    public void initialize(InternalAggregation.ReduceContext context) {
        searchScript = context.scriptService().executable(script, ScriptContext.Standard.AGGS);
        searchScript.setNextVar("_subset_freq", subsetDfHolder);
        searchScript.setNextVar("_subset_size", subsetSizeHolder);
        searchScript.setNextVar("_superset_freq", supersetDfHolder);
        searchScript.setNextVar("_superset_size", supersetSizeHolder);
    }

    /**
     * Calculates score with a script
     *
     * @param subsetFreq   The frequency of the term in the selected sample
     * @param subsetSize   The size of the selected sample (typically number of docs)
     * @param supersetFreq The frequency of the term in the superset from which the sample was taken
     * @param supersetSize The size of the superset from which the sample was taken  (typically number of docs)
     * @return a "significance" score
     */
    @Override
    public double getScore(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize) {
        if (searchScript == null) {
            //In tests, wehn calling assertSearchResponse(..) the response is streamed one additional time with an arbitrary version, see assertVersionSerializable(..).
            // Now, for version before 1.5.0 the score is computed after streaming the response but for scripts the script does not exists yet.
            // assertSearchResponse() might therefore fail although there is no problem.
            // This should be replaced by an exception in 2.0.
            ESLoggerFactory.getLogger("script heuristic").warn("cannot compute score - script has not been initialized yet.");
            return 0;
        }
        subsetSizeHolder.value = subsetSize;
        supersetSizeHolder.value = supersetSize;
        subsetDfHolder.value = subsetFreq;
        supersetDfHolder.value = supersetFreq;
        return ((Number) searchScript.run()).doubleValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(STREAM.getName());
        script.writeTo(out);
    }

    public static class ScriptHeuristicParser implements SignificanceHeuristicParser {
        private final ScriptService scriptService;
        @Inject
        public ScriptHeuristicParser(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public SignificanceHeuristic parse(XContentParser parser) throws IOException, QueryParsingException {
            NAMES_FIELD.match(parser.currentName(), ParseField.EMPTY_FLAGS);
            Script script = null;
            XContentParser.Token token;
            Map<String, Object> params = null;
            String currentFieldName = null;
            ScriptParameterParser scriptParameterParser = new ScriptParameterParser();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token.equals(XContentParser.Token.FIELD_NAME)) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (ScriptField.SCRIPT.match(currentFieldName)) {
                        script = Script.parse(parser);
                    } else if ("params".equals(currentFieldName)) { // TODO remove in 2.0 (here to support old script APIs)
                        params = parser.map();
                    } else {
                        throw new ElasticsearchParseException("unknown object " + currentFieldName + " in script_heuristic");
                    }
                } else if (!scriptParameterParser.token(currentFieldName, token, parser)) {
                    throw new ElasticsearchParseException("unknown field " + currentFieldName + " in script_heuristic");
                }
            }

            if (script == null) { // Didn't find anything using the new API so try using the old one instead
                ScriptParameterValue scriptValue = scriptParameterParser.getDefaultScriptParameterValue();
                if (scriptValue != null) {
                    if (params == null) {
                        params = newHashMap();
                    }
                    script = new Script(scriptValue.script(), scriptValue.scriptType(), scriptParameterParser.lang(), params);
                }
            } else if (params != null) {
                throw new ElasticsearchParseException("script params must be specified inside script object");
            }

            if (script == null) {
                throw new ElasticsearchParseException("No script found in script_heuristic");
            }
            ExecutableScript searchScript;
            try {
                searchScript = scriptService.executable(script, ScriptContext.Standard.AGGS);
            } catch (Exception e) {
                throw new ElasticsearchParseException("The script [" + script + "] could not be loaded", e);
            }
            return new ScriptHeuristic(searchScript, script);
        }

        @Override
        public String[] getNames() {
            return NAMES_FIELD.getAllNamesIncludedDeprecated();
        }
    }

    public static class ScriptHeuristicBuilder implements SignificanceHeuristicBuilder {

        private Script script = null;
        private String scriptString = null;
        private ScriptType type = null;
        private String lang = null;
        private Map<String, Object> params = null;

        public ScriptHeuristicBuilder setScript(Script script) {
            this.script = script;
            return this;
        }

        /**
         * @deprecated use {@link #setScript(Script)}
         */
        @Deprecated
        public ScriptHeuristicBuilder setScript(String script) {
            if (script != null) {
                this.scriptString = script;
                this.type = ScriptType.INLINE;
            }
            return this;
        }

        /**
         * @deprecated use {@link #setScript(Script)}
         */
        @Deprecated
        public ScriptHeuristicBuilder setScriptFile(String script) {
            if (script != null) {
                this.scriptString = script;
                this.type = ScriptType.FILE;
            }
            return this;
        }

        /**
         * @deprecated use {@link #setScript(Script)}
         */
        @Deprecated
        public ScriptHeuristicBuilder setLang(String lang) {
            this.lang = lang;
            return this;
        }

        /**
         * @deprecated use {@link #setScript(Script)}
         */
        @Deprecated
        public ScriptHeuristicBuilder setParams(Map<String, Object> params) {
            this.params = params;
            return this;
        }

        /**
         * @deprecated use {@link #setScript(Script)}
         */
        @Deprecated
        public ScriptHeuristicBuilder setScriptId(String scriptId) {
            if (scriptId != null) {
                this.scriptString = scriptId;
                this.type = ScriptType.INDEXED;
            }
            return this;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params builderParams) throws IOException {
            builder.startObject(STREAM.getName());
            builder.field(ScriptField.SCRIPT.getPreferredName());
            if (script == null) {
                new Script(scriptString, type, lang, params).toXContent(builder, builderParams);
            } else {
                script.toXContent(builder, builderParams);
            }
            builder.endObject();
            return builder;
        }

    }

    public final class LongAccessor extends Number {
        public long value;
        public int intValue() {
            return (int)value;
        }
        public long longValue() {
            return value;
        }

        @Override
        public float floatValue() {
            return (float)value;
        }

        @Override
        public double doubleValue() {
            return (double)value;
        }

        @Override
        public String toString() {
            return Long.toString(value);
        }
    }
}

