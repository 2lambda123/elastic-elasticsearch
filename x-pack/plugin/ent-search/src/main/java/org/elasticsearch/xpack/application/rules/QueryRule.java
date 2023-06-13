/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A query rule consists of:
 * <ul>
 *     <li>A unique identifier</li>
 *     <li>The type of rule, e.g. pinned</li>
 *     <li>The criteria required for a query to match this rule</li>
 *     <li>The actions that should be taken if this rule is matched, dependent on the type of rule</li>
 *     <li>Tags associated with this rule, for example to tie rules to a specific campaign</li>
 * </ul>
 */
public class QueryRule implements Writeable, ToXContentObject {

    private final String id;
    private final QueryRuleType type;
    // TODO Criteria
    // TODO Actions
    // TODO Tags


    public enum QueryRuleType {

        PINNED;
        public static QueryRuleType queryRuleType(String type) {
            for (QueryRuleType queryRuleType : QueryRuleType.values()) {
                if (queryRuleType.name().equalsIgnoreCase(type)) {
                    return queryRuleType;
                }
            }
            throw new IllegalArgumentException("Unknown QueryRuleType: " + type);
        }
    }

    /**
     * Public constructor.
     *
     * @param id                        The unique identifier associated with this query rule
     * @param type                      The type of query rule
     */
    public QueryRule(
        String id,
        QueryRuleType type
    ) {
        if (Strings.isNullOrEmpty(id)) {
            throw new IllegalArgumentException("Query rule id cannot be null or blank");
        }
        this.id = id;

        Objects.requireNonNull(type, "Query rule type cannot be null");
        this.type = type;
    }

    public QueryRule(StreamInput in) throws IOException {
        this.id = in.readString();
        this.type = QueryRuleType.queryRuleType(in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(type.toString());
    }

    private static final ConstructingObjectParser<QueryRule, String> PARSER = new ConstructingObjectParser<>(
        "query_rule",
        false,
        (params, resourceName) -> {
            final String id = (String) params[0];
            // If id is provided, check that it matches the resource name. We don't want it to be updatable
            if (id != null) {
                if (id.equals(resourceName) == false) {
                    throw new IllegalArgumentException(
                        "Query rule identifier [" + id + "] does not match the resource name: [" + resourceName + "]"
                    );
                }
            }
            final QueryRuleType type = QueryRuleType.queryRuleType((String) params[1]);

            return new QueryRule(resourceName, type);
        }
    );

    public static final ParseField ID_FIELD = new ParseField("id");
    public static final ParseField TYPE_FIELD = new ParseField("type");

    static {
        PARSER.declareStringOrNull(optionalConstructorArg(), ID_FIELD);
        PARSER.declareStringOrNull(constructorArg(), TYPE_FIELD);
    }

    /**
     * Parses a {@link QueryRule} from its {@param xContentType} representation in bytes.
     *
     * @param resourceName The name of the resource (must match the {@link QueryRule} id).
     * @param source The bytes that represents the {@link QueryRule}.
     * @param xContentType The format of the representation.
     *
     * @return The parsed {@link QueryRule}.
     */
    public static QueryRule fromXContentBytes(String resourceName, BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
            return QueryRule.fromXContent(resourceName, parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse: " + source.utf8ToString(), e);
        }
    }

    /**
     * Parses a {@link QueryRule} through the provided {@param parser}.
     *
     * @param resourceName The name of the resource (must match the {@link QueryRule} name).
     * @param parser The {@link XContentType} parser.
     *
     * @return The parsed {@link QueryRule}.
     */
    public static QueryRule fromXContent(String resourceName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, resourceName);
    }

    /**
     * Converts the {@link QueryRule} to XContent.
     *
     * @return The {@link XContentBuilder} containing the serialized {@link QueryRule}.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID_FIELD.getPreferredName(), id);
        builder.field(TYPE_FIELD.getPreferredName(), type);
        builder.endObject();
        return builder;
    }

    /**
     * Returns the unique ID of the {@link QueryRule}.
     *
     * @return The unique ID of the {@link QueryRule}.
     */
    public String id() {
        return id;
    }

    /**
     * Returns the type of {@link QueryRule}.
     *
     * @return The type of the {@link QueryRule}.
     */
    public QueryRuleType type() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryRule queryRule = (QueryRule) o;
        return Objects.equals(id, queryRule.id) && type == queryRule.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

//    /**
//     * Returns the merged {@link QueryRule} from the current state and the provided {@param update}.
//     * This function returns the current instance if the update is a noop.
//     *
//     * @param update The source of the update represented in bytes.
//     * @param xContentType The format of the bytes.
//     * @param bigArrays The {@link BigArrays} to use to recycle bytes array.
//     *
//     * @return The merged {@link QueryRule}.
//     */
//    QueryRule merge(BytesReference update, XContentType xContentType, BigArrays bigArrays) throws IOException {
//        final Tuple<XContentType, Map<String, Object>> sourceAndContent;
//        try (ReleasableBytesStreamOutput sourceBuffer = new ReleasableBytesStreamOutput(0, bigArrays.withCircuitBreaking())) {
//            try (XContentBuilder builder = XContentFactory.jsonBuilder(sourceBuffer)) {
//                toXContent(builder, EMPTY_PARAMS);
//            }
//            sourceAndContent = XContentHelper.convertToMap(sourceBuffer.bytes(), true, XContentType.JSON);
//        } catch (IOException e) {
//            throw new UncheckedIOException(e);
//        }
//        final Tuple<XContentType, Map<String, Object>> updateAndContent = XContentHelper.convertToMap(update, true, xContentType);
//        final Map<String, Object> newSourceAsMap = new HashMap<>(sourceAndContent.v2());
//        final boolean noop = XContentHelper.update(newSourceAsMap, updateAndContent.v2(), true) == false;
//        if (noop) {
//            return this;
//        }
//
//        try (ReleasableBytesStreamOutput newSourceBuffer = new ReleasableBytesStreamOutput(0, bigArrays.withCircuitBreaking())) {
//            try (XContentBuilder builder = XContentFactory.jsonBuilder(newSourceBuffer)) {
//                builder.value(newSourceAsMap);
//            }
//            return QueryRule.fromXContentBytes(name, newSourceBuffer.bytes(), XContentType.JSON);
//        }
//    }
}
