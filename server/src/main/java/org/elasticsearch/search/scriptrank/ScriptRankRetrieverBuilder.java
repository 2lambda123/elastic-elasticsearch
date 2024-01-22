/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.scriptrank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ScriptRankRetrieverBuilder extends RetrieverBuilder<ScriptRankRetrieverBuilder> {
    public static final String NAME = "script_rank";

    public static final int DEFAULT_WINDOW_SIZE = 100;


    public static final ParseField RETRIEVERS_FIELD = new ParseField("retrievers");
    public static final ParseField WINDOW_SIZE_FIELD = new ParseField("window_size");
    public static final ParseField SCRIPT_FIELD = new ParseField("script");

    public static final ObjectParser<ScriptRankRetrieverBuilder, RetrieverParserContext> PARSER = new ObjectParser<>(
        NAME,
        ScriptRankRetrieverBuilder::new
    );

    static {
        PARSER.declareObjectArray((v, l) -> v.retrieverBuilders = l, (p, c) -> {
            p.nextToken();
            String name = p.currentName();
            RetrieverBuilder<?> retrieverBuilder = (RetrieverBuilder<?>) p.namedObject(RetrieverBuilder.class, name, c);
            p.nextToken();
            return retrieverBuilder;
        }, RETRIEVERS_FIELD);
        PARSER.declareInt((b, v) -> b.windowSize = v, WINDOW_SIZE_FIELD);
        PARSER.declareObject(
            (builder, parsedValue) -> builder.script = parsedValue,
            (parser, context) -> Script.parse(parser),
            SCRIPT_FIELD
        );

        RetrieverBuilder.declareBaseParserFields(NAME, PARSER);
    }

    public static ScriptRankRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        return PARSER.apply(parser, context);
    }

    private List<? extends RetrieverBuilder<?>> retrieverBuilders = Collections.emptyList();
    private int windowSize = ScriptRankRetrieverBuilder.DEFAULT_WINDOW_SIZE;
    private Script script = null;

    public ScriptRankRetrieverBuilder() {
    }

    public ScriptRankRetrieverBuilder(ScriptRankRetrieverBuilder original) {
        super(original);
        this.retrieverBuilders = original.retrieverBuilders;
        this.windowSize = original.windowSize;
        this.script = original.script;
    }

    public ScriptRankRetrieverBuilder(List<? extends RetrieverBuilder<?>> retrieverBuilders, int windowSize, Script script) {
        this.retrieverBuilders = retrieverBuilders;
        this.windowSize = windowSize;
        this.script = script;
    }

    @SuppressWarnings("unchecked")
    public ScriptRankRetrieverBuilder(StreamInput in) throws IOException {
        super(in);
        this.retrieverBuilders = (List<RetrieverBuilder<?>>) (Object) in.readNamedWriteableCollectionAsList(RetrieverBuilder.class);
        this.windowSize = in.readVInt();
        this.script = new Script(in);
    }


    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.SCRIPT_RANK_ADDED;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableCollection(retrieverBuilders);
        out.writeVInt(windowSize);
        script.writeTo(out);
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        for (RetrieverBuilder<?> retrieverBuilder : retrieverBuilders) {
            builder.startArray(RETRIEVERS_FIELD.getPreferredName());
            retrieverBuilder.toXContent(builder, params);
            builder.endArray();
        }

        builder.field(WINDOW_SIZE_FIELD.getPreferredName(), windowSize);
        builder.field(SCRIPT_FIELD.getPreferredName(), script);
    }

    @Override
    protected ScriptRankRetrieverBuilder shallowCopyInstance() {
        return new ScriptRankRetrieverBuilder(this);
    }

    @Override
    public void doExtractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder) {
        for (RetrieverBuilder<?> retrieverBuilder : retrieverBuilders) {
            retrieverBuilder.doExtractToSearchSourceBuilder(searchSourceBuilder);
        }

        if (searchSourceBuilder.rankBuilder() == null) {
            searchSourceBuilder.rankBuilder(new ScriptRankBuilder(windowSize, script));
        } else {
            throw new IllegalStateException("[rank] cannot be declared as a retriever value and as a global value");
        }
    }
}
