/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.index.search.QueryParserHelper;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.xpack.security.action.apikey.TransportQueryApiKeyAction.API_KEY_TYPE_RUNTIME_MAPPING_FIELD;

/**
 * A class to translate query level field names to index level field names.
 */
public class ApiKeyFieldNameTranslators {
    static final List<FieldNameTranslator> FIELD_NAME_TRANSLATORS;

    static {
        FIELD_NAME_TRANSLATORS = List.of(
            new ExactFieldNameTranslator(s -> "creator.principal", "username"),
            new ExactFieldNameTranslator(s -> "creator.realm", "realm_name"),
            new ExactFieldNameTranslator(s -> "name", "name"),
            new ExactFieldNameTranslator(s -> API_KEY_TYPE_RUNTIME_MAPPING_FIELD, "type"),
            new ExactFieldNameTranslator(s -> "creation_time", "creation"),
            new ExactFieldNameTranslator(s -> "expiration_time", "expiration"),
            new ExactFieldNameTranslator(s -> "api_key_invalidated", "invalidated"),
            new ExactFieldNameTranslator(s -> "invalidation_time", "invalidation"),
            // allows querying on all metadata values as keywords because "metadata_flattened" is a flattened field type
            new ExactFieldNameTranslator(s -> "metadata_flattened", "metadata"),
            new PrefixFieldNameTranslator(s -> "metadata_flattened." + s.substring("metadata.".length()), "metadata.")
        );
    }

    /**
     * Translate the query level field name to index level field names.
     * It throws an exception if the field name is not explicitly allowed.
     */
    public static String translate(String fieldName) {
        if (Regex.isSimpleMatchPattern(fieldName)) {
            throw new IllegalArgumentException("Field name pattern [" + fieldName + "] is not allowed for API Key query");
        }
        for (FieldNameTranslator translator : FIELD_NAME_TRANSLATORS) {
            if (translator.supports(fieldName)) {
                return translator.translate(fieldName);
            }
        }
        throw new IllegalArgumentException("Field [" + fieldName + "] is not allowed for API Key query");
    }

    /**
     * Translates a query level field name pattern to the matching index level field names.
     * The result can be the empty set, if the pattern doesn't match any of the allowed index level field names.
     * If the pattern is actually a concrete field name rather than a pattern,
     * it is also translated, but only if the query level field name is allowed, otherwise an exception is thrown.
     */
    public static Set<String> translatePattern(String fieldNameOrPattern) {
        Set<String> indexFieldNames = new HashSet<>();
        for (FieldNameTranslator translator : FIELD_NAME_TRANSLATORS) {
            if (translator.supports(fieldNameOrPattern)) {
                indexFieldNames.add(translator.translate(fieldNameOrPattern));
            }
        }
        // It's OK to "translate" to the empty set the concrete disallowed or unknown field names, because
        // the SimpleQueryString query type is lenient in the sense that it ignores unknown fields and field name patterns,
        // so this preprocessing can ignore them too.
        return indexFieldNames;
    }

    public static QueryBuilder translateQueryBuilderFields(QueryBuilder qb, Consumer<String> fieldNameVisitor) {
        if (qb instanceof final BoolQueryBuilder query) {
            final BoolQueryBuilder newQuery = QueryBuilders.boolQuery()
                .minimumShouldMatch(query.minimumShouldMatch())
                .adjustPureNegative(query.adjustPureNegative())
                .boost(query.boost());
            query.must().stream().map(q -> translateQueryBuilderFields(q, fieldNameVisitor)).forEach(newQuery::must);
            query.should().stream().map(q -> translateQueryBuilderFields(q, fieldNameVisitor)).forEach(newQuery::should);
            query.mustNot().stream().map(q -> translateQueryBuilderFields(q, fieldNameVisitor)).forEach(newQuery::mustNot);
            query.filter().stream().map(q -> translateQueryBuilderFields(q, fieldNameVisitor)).forEach(newQuery::filter);
            return newQuery;
        } else if (qb instanceof MatchAllQueryBuilder) {
            return qb;
        } else if (qb instanceof IdsQueryBuilder) {
            return qb;
        } else if (qb instanceof final TermQueryBuilder query) {
            final String translatedFieldName = translate(query.fieldName());
            fieldNameVisitor.accept(translatedFieldName);
            return QueryBuilders.termQuery(translatedFieldName, query.value())
                .caseInsensitive(query.caseInsensitive())
                .boost(query.boost());
        } else if (qb instanceof final ExistsQueryBuilder query) {
            final String translatedFieldName = translate(query.fieldName());
            fieldNameVisitor.accept(translatedFieldName);
            return QueryBuilders.existsQuery(translatedFieldName).boost(query.boost());
        } else if (qb instanceof final TermsQueryBuilder query) {
            if (query.termsLookup() != null) {
                throw new IllegalArgumentException("terms query with terms lookup is not supported for API Key query");
            }
            final String translatedFieldName = translate(query.fieldName());
            fieldNameVisitor.accept(translatedFieldName);
            return QueryBuilders.termsQuery(translatedFieldName, query.getValues()).boost(query.boost());
        } else if (qb instanceof final PrefixQueryBuilder query) {
            final String translatedFieldName = translate(query.fieldName());
            fieldNameVisitor.accept(translatedFieldName);
            return QueryBuilders.prefixQuery(translatedFieldName, query.value())
                .caseInsensitive(query.caseInsensitive())
                .rewrite(query.rewrite())
                .boost(query.boost());
        } else if (qb instanceof final WildcardQueryBuilder query) {
            final String translatedFieldName = translate(query.fieldName());
            fieldNameVisitor.accept(translatedFieldName);
            return QueryBuilders.wildcardQuery(translatedFieldName, query.value())
                .caseInsensitive(query.caseInsensitive())
                .rewrite(query.rewrite())
                .boost(query.boost());
        } else if (qb instanceof final MatchQueryBuilder query) {
            final String translatedFieldName = translate(query.fieldName());
            fieldNameVisitor.accept(translatedFieldName);
            final MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(translatedFieldName, query.value());
            if (query.operator() != null) {
                matchQueryBuilder.operator(query.operator());
            }
            if (query.analyzer() != null) {
                matchQueryBuilder.analyzer(query.analyzer());
            }
            if (query.fuzziness() != null) {
                matchQueryBuilder.fuzziness(query.fuzziness());
            }
            if (query.minimumShouldMatch() != null) {
                matchQueryBuilder.minimumShouldMatch(query.minimumShouldMatch());
            }
            if (query.fuzzyRewrite() != null) {
                matchQueryBuilder.fuzzyRewrite(query.fuzzyRewrite());
            }
            if (query.zeroTermsQuery() != null) {
                matchQueryBuilder.zeroTermsQuery(query.zeroTermsQuery());
            }
            matchQueryBuilder.prefixLength(query.prefixLength())
                .maxExpansions(query.maxExpansions())
                .fuzzyTranspositions(query.fuzzyTranspositions())
                .lenient(query.lenient())
                .autoGenerateSynonymsPhraseQuery(query.autoGenerateSynonymsPhraseQuery())
                .boost(query.boost());
            return matchQueryBuilder;
        } else if (qb instanceof final RangeQueryBuilder query) {
            if (query.relation() != null) {
                throw new IllegalArgumentException("range query with relation is not supported for API Key query");
            }
            final String translatedFieldName = translate(query.fieldName());
            fieldNameVisitor.accept(translatedFieldName);
            final RangeQueryBuilder newQuery = QueryBuilders.rangeQuery(translatedFieldName);
            if (query.format() != null) {
                newQuery.format(query.format());
            }
            if (query.timeZone() != null) {
                newQuery.timeZone(query.timeZone());
            }
            if (query.from() != null) {
                newQuery.from(query.from()).includeLower(query.includeLower());
            }
            if (query.to() != null) {
                newQuery.to(query.to()).includeUpper(query.includeUpper());
            }
            return newQuery.boost(query.boost());
        } else if (qb instanceof final SimpleQueryStringBuilder simpleQueryStringBuilder) {
            if (simpleQueryStringBuilder.fields().isEmpty()) {
                simpleQueryStringBuilder.field("*");
            }
            // override lenient if querying all the fields, because, due to different field mappings,
            // the query parsing will almost certainly fail otherwise
            if (QueryParserHelper.hasAllFieldsWildcard(simpleQueryStringBuilder.fields().keySet())) {
                simpleQueryStringBuilder.lenient(true);
            }
            Map<String, Float> requestedFields = new HashMap<>(simpleQueryStringBuilder.fields());
            simpleQueryStringBuilder.fields().clear();
            for (Map.Entry<String, Float> requestedFieldNameOrPattern : requestedFields.entrySet()) {
                for (String translatedField : translatePattern(requestedFieldNameOrPattern.getKey())) {
                    simpleQueryStringBuilder.fields()
                        .compute(
                            translatedField,
                            (k, v) -> (v == null) ? requestedFieldNameOrPattern.getValue() : v * requestedFieldNameOrPattern.getValue()
                        );
                    fieldNameVisitor.accept(translatedField);
                }
            }
            if (simpleQueryStringBuilder.fields().isEmpty()) {
                // A SimpleQueryStringBuilder with empty fields() will eventually produce a SimpleQueryString query
                // that accesses all the fields, including disallowed ones.
                // Instead, the behavior we're after is that a query that accesses only disallowed fields should
                // not match any docs.
                return new MatchNoneQueryBuilder();
            } else {
                return simpleQueryStringBuilder;
            }
        } else {
            throw new IllegalArgumentException("Query type [" + qb.getName() + "] is not supported for API Key query");
        }
    }

    abstract static class FieldNameTranslator {

        private final Function<String, String> translationFunc;

        protected FieldNameTranslator(Function<String, String> translationFunc) {
            this.translationFunc = translationFunc;
        }

        String translate(String fieldName) {
            return translationFunc.apply(fieldName);
        }

        abstract boolean supports(String fieldName);
    }

    static class ExactFieldNameTranslator extends FieldNameTranslator {
        private final String name;

        ExactFieldNameTranslator(Function<String, String> translationFunc, String name) {
            super(translationFunc);
            this.name = name;
        }

        @Override
        public boolean supports(String fieldNameOrPattern) {
            if (Regex.isSimpleMatchPattern(fieldNameOrPattern)) {
                return Regex.simpleMatch(fieldNameOrPattern, name);
            } else {
                return name.equals(fieldNameOrPattern);
            }
        }
    }

    static class PrefixFieldNameTranslator extends FieldNameTranslator {
        private final String prefix;

        PrefixFieldNameTranslator(Function<String, String> translationFunc, String prefix) {
            super(translationFunc);
            this.prefix = prefix;
        }

        @Override
        boolean supports(String fieldNamePrefix) {
            // a pattern can generally match a prefix in multiple ways
            // moreover, it's not possible to iterate the concrete fields matching the prefix
            if (Regex.isSimpleMatchPattern(fieldNamePrefix)) {
                // this means that e.g. `metadata.*` and `metadata.x*` are expanded to the empty list,
                // rather than be replaced with `metadata_flattened.*` and `metadata_flattened.x*`
                // (but, in any case, `metadata_flattened.*` and `metadata.x*` are going to be ignored)
                return false;
            }
            return fieldNamePrefix.startsWith(prefix);
        }
    }
}
