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
import java.util.List;
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

    private final List<QueryRuleCriteria> criteria;

    // TODO add criteria, actions


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
        QueryRuleType type,
        List<QueryRuleCriteria> criteria
    ) {
        if (Strings.isNullOrEmpty(id)) {
            throw new IllegalArgumentException("Query rule id cannot be null or blank");
        }
        this.id = id;

        Objects.requireNonNull(type, "Query rule type cannot be null");
        this.type = type;

        Objects.requireNonNull(criteria, "Query rule criteria cannot be null");
        if (criteria.isEmpty()) {
            throw new IllegalArgumentException("Query rule criteria cannot be empty");
        }
        this.criteria = criteria;
    }

    public QueryRule(StreamInput in) throws IOException {
        this.id = in.readString();
        this.type = QueryRuleType.queryRuleType(in.readString());
        this.criteria = in.readList(QueryRuleCriteria::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(type.toString());
        out.writeList(criteria);
    }

    private static final ConstructingObjectParser<QueryRule, String> PARSER = new ConstructingObjectParser<>(
        "query_rule",
        false,
        (params, resourceName) -> {
            final String id = (String) params[0];
            final QueryRuleType type = QueryRuleType.queryRuleType((String) params[1]);
            @SuppressWarnings("unchecked")
            final List<QueryRuleCriteria> criteria = (List<QueryRuleCriteria>) params[2];
            return new QueryRule(id, type, criteria);
        }
    );

    public static final ParseField ID_FIELD = new ParseField("rule_id");
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField CRITERIA_FIELD = new ParseField("criteria");

    static {
        PARSER.declareStringOrNull(optionalConstructorArg(), ID_FIELD);
        PARSER.declareStringOrNull(constructorArg(), TYPE_FIELD);
        PARSER.declareObjectArray(constructorArg(), (p, c) -> QueryRuleCriteria.fromXContent(p), CRITERIA_FIELD);
    }

    /**
     * Parses a {@link QueryRule} from its {@param xContentType} representation in bytes.
     *
     * @param source The bytes that represents the {@link QueryRule}.
     * @param xContentType The format of the representation.
     *
     * @return The parsed {@link QueryRule}.
     */
    public static QueryRule fromXContentBytes(BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
            return QueryRule.fromXContent(parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse: " + source.utf8ToString(), e);
        }
    }

    /**
     * Parses a {@link QueryRule} through the provided {@param parser}.
     * @param parser The {@link XContentType} parser.
     *
     * @return The parsed {@link QueryRule}.
     */
    public static QueryRule fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    /**
     * Converts the {@link QueryRule} to XContent.
     *
     * @return The {@link XContentBuilder} containing the serialized {@link QueryRule}.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(ID_FIELD.getPreferredName(), id);
            builder.field(TYPE_FIELD.getPreferredName(), type);
            builder.startArray(CRITERIA_FIELD.getPreferredName());
            {
                for (QueryRuleCriteria queryRuleCriteria : criteria) {
                    queryRuleCriteria.toXContent(builder, params);
                }
            }
            builder.endArray();
        }
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

    public List<QueryRuleCriteria> criteria() {
        return criteria;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryRule queryRule = (QueryRule) o;
        return Objects.equals(id, queryRule.id) && type == queryRule.type && Objects.equals(criteria, queryRule.criteria);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, criteria);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
