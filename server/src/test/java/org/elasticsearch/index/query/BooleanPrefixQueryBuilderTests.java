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

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.MockSynonymAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasSize;

public class BooleanPrefixQueryBuilderTests extends AbstractQueryTestCase<BooleanPrefixQueryBuilder> {

    @Override
    protected BooleanPrefixQueryBuilder doCreateTestQueryBuilder() {
        final String fieldName = randomFrom(STRING_FIELD_NAME, STRING_ALIAS_FIELD_NAME);
        final Object value = IntStream.rangeClosed(0, randomIntBetween(0, 3))
                .mapToObj(i -> randomAlphaOfLengthBetween(1, 10) + " ")
                .collect(Collectors.joining())
                .trim();

        final BooleanPrefixQueryBuilder queryBuilder = new BooleanPrefixQueryBuilder(fieldName, value);

        if (randomBoolean() && isTextField(fieldName)) {
            queryBuilder.analyzer(randomFrom("simple", "keyword", "whitespace"));
        }

        if (randomBoolean()) {
            queryBuilder.minimumShouldMatch(randomMinimumShouldMatch());
        }
        return queryBuilder;
    }

    @Override
    protected void doAssertLuceneQuery(BooleanPrefixQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        assertThat(query, notNullValue());
        assertThat(query, anyOf(instanceOf(BooleanQuery.class), instanceOf(PrefixQuery.class)));

        if (query instanceof PrefixQuery) {
            final PrefixQuery prefixQuery = (PrefixQuery) query;
            assertThat(prefixQuery.getPrefix().text(), equalToIgnoringCase((String) queryBuilder.value()));
        } else {
            final BooleanQuery booleanQuery = (BooleanQuery) query;
            assertThat(booleanQuery.clauses(), everyItem(hasProperty("occur", equalTo(BooleanClause.Occur.SHOULD))));

            // all queries except the last should be TermQuery or SynonymQuery
            assertThat(
                IntStream.range(0, booleanQuery.clauses().size() - 1)
                    .mapToObj(booleanQuery.clauses()::get)
                    .map(BooleanClause::getQuery)
                    .collect(Collectors.toSet()),
                everyItem(anyOf(instanceOf(TermQuery.class), instanceOf(SynonymQuery.class)))); // here

            // the last query should be PrefixQuery
            final Query shouldBePrefixQuery = booleanQuery.clauses().get(booleanQuery.clauses().size() - 1).getQuery();
            assertThat(shouldBePrefixQuery, instanceOf(PrefixQuery.class));

            if (queryBuilder.minimumShouldMatch() != null) {
                assertThat(booleanQuery.getMinimumNumberShouldMatch(),
                    equalTo(Queries.calculateMinShouldMatch(booleanQuery.clauses().size(), queryBuilder.minimumShouldMatch())));
            }
        }
    }

    public void testIllegalValues() {
        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new BooleanPrefixQueryBuilder(null, "value"));
            assertEquals("[boolean_prefix] requires fieldName", e.getMessage());
        }

        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new BooleanPrefixQueryBuilder("name", null));
            assertEquals("[boolean_prefix] requires query value", e.getMessage());
        }

        {
            final BooleanPrefixQueryBuilder builder = new BooleanPrefixQueryBuilder("name", "value");
            builder.analyzer("bogusAnalyzer");
            QueryShardException e = expectThrows(QueryShardException.class, () -> builder.toQuery(createShardContext()));
            assertThat(e.getMessage(), containsString("analyzer [bogusAnalyzer] not found"));
        }
    }

    public void testFromSimpleJson() throws IOException {
        final String simple =
            "{" +
                "\"boolean_prefix\": {" +
                    "\"fieldName\": \"fieldValue\"" +
                "}" +
            "}";
        final String expected =
            "{" +
                "\"boolean_prefix\": {" +
                    "\"fieldName\": {" +
                        "\"query\": \"fieldValue\"," +
                        "\"boost\": 1.0" +
                    "}" +
                "}" +
            "}";

        final BooleanPrefixQueryBuilder builder = (BooleanPrefixQueryBuilder) parseQuery(simple);
        checkGeneratedJson(expected, builder);
    }

    public void testFromJson() throws IOException {
        final String expected =
            "{" +
                "\"boolean_prefix\": {" +
                    "\"fieldName\": {" +
                        "\"query\": \"fieldValue\"," +
                        "\"analyzer\": \"simple\"," +
                        "\"minimum_should_match\": \"2\"," +
                        "\"boost\": 2.0" +
                    "}" +
                "}" +
            "}";

        final BooleanPrefixQueryBuilder builder = (BooleanPrefixQueryBuilder) parseQuery(expected);
        checkGeneratedJson(expected, builder);
    }

    public void testParseFailsWithMultipleFields() {
        {
            final String json =
                "{" +
                    "\"boolean_prefix\" : {" +
                        "\"field_name_1\" : {" +
                            "\"query\" : \"foo\"" +
                        "}," +
                        "\"field_name_2\" : {" +
                            "\"query\" : \"foo\"\n" +
                        "}" +
                    "}" +
                "}";
            final ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
            assertEquals("[boolean_prefix] query doesn't support multiple fields, found [field_name_1] and [field_name_2]", e.getMessage());
        }

        {
            final String simpleJson =
                "{" +
                    "\"boolean_prefix\" : {" +
                        "\"field_name_1\" : \"foo\"," +
                        "\"field_name_2\" : \"foo\"" +
                    "}" +
                "}";
            final ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(simpleJson));
            assertEquals("[boolean_prefix] query doesn't support multiple fields, found [field_name_1] and [field_name_2]", e.getMessage());
        }
    }

    public void testAnalysis() throws Exception {
        final MatchQuery matchQuery = new MatchQuery(createShardContext());
        final Query query = matchQuery.parse(MatchQuery.Type.BOOLEAN_PREFIX, STRING_FIELD_NAME, "foo bar baz");

        assertBooleanQuery(query, asList(
            new TermQuery(new Term(STRING_FIELD_NAME, "foo")),
            new TermQuery(new Term(STRING_FIELD_NAME, "bar")),
            new PrefixQuery(new Term(STRING_FIELD_NAME, "baz"))
        ));
    }

    public void testAnalysisGraph() throws Exception {
        final MatchQuery matchQuery = new MatchQuery(createShardContext());
        matchQuery.setAnalyzer(new MockSynonymAnalyzer());
        final Query query = matchQuery.parse(MatchQuery.Type.BOOLEAN_PREFIX, STRING_FIELD_NAME, "fox dogs red");

        assertBooleanQuery(query, asList(
            new TermQuery(new Term(STRING_FIELD_NAME, "fox")),
            new SynonymQuery(new Term(STRING_FIELD_NAME, "dogs"), new Term(STRING_FIELD_NAME, "dog")),
            new PrefixQuery(new Term(STRING_FIELD_NAME, "red"))
        ));
    }

    private static void assertBooleanQuery(Query actual, List<Query> expectedClauseQueries) {
        assertThat(actual, instanceOf(BooleanQuery.class));
        final BooleanQuery actualBooleanQuery = (BooleanQuery) actual;
        assertThat(actualBooleanQuery.clauses(), hasSize(expectedClauseQueries.size()));
        assertThat(actualBooleanQuery.clauses(), everyItem(hasProperty("occur", equalTo(BooleanClause.Occur.SHOULD))));

        for (int i = 0; i < actualBooleanQuery.clauses().size(); i++) {
            final Query clauseQuery = actualBooleanQuery.clauses().get(i).getQuery();
            assertThat(clauseQuery, equalTo(expectedClauseQueries.get(i)));
        }
    }
}
