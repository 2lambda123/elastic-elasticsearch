/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rerank;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class RRFBuilder implements Writeable, ToXContent {

    public static final int RANK_CONSTANT_DEFAULT = 20;
    public static final int WINDOW_SIZE_DEFAULT = 10;

    public static final ParseField RANK_CONSTANT_FIELD = new ParseField("rank_constant");
    public static final ParseField WINDOW_SIZE_FIELD = new ParseField("window_size");

    private static final ConstructingObjectParser<RRFBuilder, Void> PARSER = new ConstructingObjectParser<>(
        "rrf",
        args -> new RRFBuilder().windowSize(args[0] == null ? WINDOW_SIZE_DEFAULT : (int) args[0])
            .rankConstant(args[1] == null ? RANK_CONSTANT_DEFAULT : (int) args[1])
    );

    static {
        PARSER.declareInt(optionalConstructorArg(), WINDOW_SIZE_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_CONSTANT_FIELD);
    }

    public static RRFBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(WINDOW_SIZE_FIELD.getPreferredName(), windowSize);
        builder.field(RANK_CONSTANT_FIELD.getPreferredName(), rankConstant);
        builder.endArray();

        return builder;
    }

    private int windowSize;
    private int rankConstant;

    public RRFBuilder() {}

    public RRFBuilder(StreamInput in) throws IOException {
        windowSize = in.readVInt();
        rankConstant = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(windowSize);
        out.writeVInt(rankConstant);
    }

    public RRFBuilder windowSize(int windowSize) {
        this.windowSize = windowSize;
        return this;
    }

    public int windowSize() {
        return windowSize;
    }

    public RRFBuilder rankConstant(int rankConstant) {
        this.rankConstant = rankConstant;
        return this;
    }

    public int rankConstant() {
        return rankConstant;
    }

    public RRFReranker reranker(int size) {
        return new RRFReranker(windowSize, size, rankConstant);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RRFBuilder that = (RRFBuilder) o;
        return rankConstant == that.rankConstant && windowSize == that.windowSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(rankConstant, windowSize);
    }

    @Override
    public String toString() {
        return toString(EMPTY_PARAMS);
    }

    public String toString(Params params) {
        try {
            return XContentHelper.toXContent(this, XContentType.JSON, params, true).utf8ToString();
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }
}
