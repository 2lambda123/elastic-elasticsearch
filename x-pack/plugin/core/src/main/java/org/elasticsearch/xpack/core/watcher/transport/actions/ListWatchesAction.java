/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

public class ListWatchesAction extends ActionType<ListWatchesAction.Response> {

    public static final ListWatchesAction INSTANCE = new ListWatchesAction();
    public static final String NAME = "cluster:monitor/xpack/watcher/watch/list";

    private ListWatchesAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        public static final ParseField FROM_FIELD = new ParseField("from");
        public static final ParseField SIZE_FIELD = new ParseField("size");
        public static final ParseField QUERY_FIELD = new ParseField("query");
        public static final ParseField SORT_FIELD = new ParseField("sort");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "list_watch_request",
            true,
            (args, c) -> new Request((Integer) args[0], (Integer) args[1], (QueryBuilder) args[2], (List<FieldSortBuilder>) args[3])
        );

        static {
            PARSER.declareInt(optionalConstructorArg(), FROM_FIELD);
            PARSER.declareInt(optionalConstructorArg(), SIZE_FIELD);
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> parseInnerQueryBuilder(p), QUERY_FIELD);
            PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> {
                String fieldName = null;
                FieldSortBuilder result = null;
                for (Token token = p.nextToken(); token != Token.END_OBJECT; token = p.nextToken()) {
                    if (token == Token.FIELD_NAME) {
                        fieldName = p.currentName();
                    } else {
                        result = FieldSortBuilder.fromXContent(p, fieldName);
                    }
                }
                return result;
            }, SORT_FIELD);
        }

        public static Request fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        private final Integer from;
        private final Integer size;
        private final QueryBuilder query;
        private final List<FieldSortBuilder> sorts;

        public Request(StreamInput in) throws IOException {
            super(in);
            from = in.readOptionalVInt();
            size = in.readOptionalVInt();
            query = in.readOptionalNamedWriteable(QueryBuilder.class);
            if (in.readBoolean()) {
                sorts = in.readList(FieldSortBuilder::new);
            } else {
                sorts = null;
            }
        }

        public Request(Integer from, Integer size, QueryBuilder query, List<FieldSortBuilder> sorts) {
            this.from = from;
            this.size = size;
            this.query = query;
            this.sorts = sorts;
        }

        public Integer getFrom() {
            return from;
        }

        public Integer getSize() {
            return size;
        }

        public QueryBuilder getQuery() {
            return query;
        }

        public List<FieldSortBuilder> getSorts() {
            return sorts;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalVInt(from);
            out.writeOptionalVInt(size);
            out.writeOptionalNamedWriteable(query);
            if (sorts != null) {
                out.writeBoolean(true);
                out.writeList(sorts);
            } else {
                out.writeBoolean(false);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            if (from != null) {
                builder.field(FROM_FIELD.getPreferredName(), from);
            }
            if (size != null) {
                builder.field(SIZE_FIELD.getPreferredName(), size);
            }
            if (query != null) {
                builder.field(QUERY_FIELD.getPreferredName(), query);
            }
            if (sorts != null) {
                builder.startArray(SORT_FIELD.getPreferredName());
                for (FieldSortBuilder sort : sorts) {
                    sort.toXContent(builder, params);
                }
                builder.endArray();
            }
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(from, request.from) &&
                Objects.equals(size, request.size) &&
                Objects.equals(query, request.query) &&
                Objects.equals(sorts, request.sorts);
        }

        @Override
        public int hashCode() {
            return Objects.hash(from, size, query, sorts);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final List<Item> watches;
        private final long watchTotalCount;

        public Response(long watchTotalCount, List<Item> watches) {
            this.watches = watches;
            this.watchTotalCount = watchTotalCount;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            watches = in.readList(Item::new);
            watchTotalCount = in.readVLong();
        }

        public List<Item> getWatches() {
            return watches;
        }

        public long getWatchTotalCount() {
            return watchTotalCount;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(watches);
            out.writeVLong(watchTotalCount);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("count", watchTotalCount);
            builder.startArray("watches");
            for (Item watch : watches) {
                builder.startObject();
                watch.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return watchTotalCount == response.watchTotalCount &&
                watches.equals(response.watches);
        }

        @Override
        public int hashCode() {
            return Objects.hash(watches, watchTotalCount);
        }

        public static class Item implements Writeable, ToXContentFragment {

            private final String id;
            private final XContentSource source;
            private final WatchStatus status;
            private final long seqNo;
            private final long primaryTerm;

            public Item(String id, XContentSource source, WatchStatus status, long seqNo, long primaryTerm) {
                this.id = id;
                this.source = source;
                this.status = status;
                this.seqNo = seqNo;
                this.primaryTerm = primaryTerm;
            }

            public String getId() {
                return id;
            }

            public XContentSource getSource() {
                return source;
            }

            public WatchStatus getStatus() {
                return status;
            }

            public long getSeqNo() {
                return seqNo;
            }

            public long getPrimaryTerm() {
                return primaryTerm;
            }

            public Item(StreamInput in) throws IOException {
                id = in.readString();
                source = XContentSource.readFrom(in);
                status = new WatchStatus(in);
                seqNo = in.readZLong();
                primaryTerm = in.readVLong();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(id);
                XContentSource.writeTo(source, out);
                status.writeTo(out);
                out.writeZLong(seqNo);
                out.writeVLong(primaryTerm);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.field("_id", id);
                builder.field("watch", source, params);
                builder.field("status", status,  params);
                builder.field("_seq_no", seqNo);
                builder.field("_primary_term", primaryTerm);
                return builder;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Item item = (Item) o;
                return seqNo == item.seqNo &&
                    primaryTerm == item.primaryTerm &&
                    id.equals(item.id) &&
                    source.equals(item.source);
            }

            @Override
            public int hashCode() {
                return Objects.hash(id, source, seqNo, primaryTerm);
            }
        }
    }

}
