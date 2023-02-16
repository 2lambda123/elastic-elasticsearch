/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class RankContextBuilder implements Writeable, ToXContent {

    protected final List<QueryBuilder> queryBuilders;
    protected int size = 10;
    protected int from = 0;

    public RankContextBuilder() {
        queryBuilders = new ArrayList<>();
    }

    public RankContextBuilder(StreamInput in) throws IOException {
        queryBuilders = in.readNamedWriteableList(QueryBuilder.class);
        size = in.readVInt();
        from = in.readVInt();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList(queryBuilders);
        out.writeVInt(size);
        out.writeVInt(from);
    }

    public abstract ParseField name();

    public List<QueryBuilder> queryBuilders() {
        return queryBuilders;
    }

    public RankContextBuilder size(int size) {
        this.size = size == -1 ? 10 : size;
        return this;
    }

    public RankContextBuilder from(int from) {
        this.from = from == -1 ? 0 : from;
        return this;
    }

    public RankContextBuilder shallowCopy() {
        RankContextBuilder rankContextBuilder = subShallowCopy();
        rankContextBuilder.queryBuilders.addAll(this.queryBuilders);
        rankContextBuilder.size = this.size;
        rankContextBuilder.from = this.from;
        return rankContextBuilder;
    }

    public abstract RankContextBuilder subShallowCopy();

    public abstract QueryBuilder searchQuery();

    public abstract RankShardContext build(SearchExecutionContext searchExecutionContext) throws IOException;

    public abstract RankContext build();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RankContextBuilder that = (RankContextBuilder) o;
        return size == that.size && from == that.from && Objects.equals(queryBuilders, that.queryBuilders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryBuilders, size, from);
    }

    @Override
    public String toString() {
        return "RankContextBuilder{" + "queryBuilders=" + queryBuilders + ", size=" + size + ", from=" + from + '}';
    }
}
