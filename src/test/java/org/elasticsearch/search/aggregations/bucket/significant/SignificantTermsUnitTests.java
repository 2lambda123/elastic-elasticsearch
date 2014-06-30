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
package org.elasticsearch.search.aggregations.bucket.significant;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.search.child.TestSearchContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.*;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchLuceneTestCase;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.*;

import static org.hamcrest.Matchers.*;

/**
 *
 */
@ElasticsearchIntegrationTest.SuiteScopeTest
public class SignificantTermsUnitTests extends ElasticsearchLuceneTestCase {
    static class SignificantTermsTestSearchContext extends TestSearchContext {
        @Override
        public int numberOfShards() {
            return 1;
        }
    }

    // test that stream output can actually be read - does not replace bwc test
    @Test
    public void streamResponse() throws Exception {
        SignificanceHeuristicStreams.registerStream(MutualInformation.STREAM, MutualInformation.STREAM.getName());
        SignificanceHeuristicStreams.registerStream(DefaultHeuristic.STREAM, DefaultHeuristic.STREAM.getName());
        Version version = ElasticsearchIntegrationTest.randomVersion();
        InternalSignificantTerms[] sigTerms = getRandomSignificantTerms(getRandomSignificanceheuristic());

        // write
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        out.setVersion(version);

        sigTerms[0].writeTo(out);

        // read
        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        in.setVersion(version);

        sigTerms[1].readFrom(in);

        if (version.onOrAfter(Version.V_1_3_0)) {
            assertTrue(sigTerms[1].significanceHeuristic.equals(sigTerms[0].significanceHeuristic));
        } else {
            assertTrue(sigTerms[1].significanceHeuristic instanceof DefaultHeuristic);
        }
    }

    InternalSignificantTerms[] getRandomSignificantTerms(SignificanceHeuristic heuristic) {
        InternalSignificantTerms[] sTerms = new InternalSignificantTerms[2];
        ArrayList<InternalSignificantTerms.Bucket> buckets = new ArrayList<>();
        if (random().nextBoolean()) {
            BytesRef term = new BytesRef("123.0");
            buckets.add(new SignificantLongTerms.Bucket(1, 2, 3, 4, 123, InternalAggregations.EMPTY));
            sTerms[0] = new SignificantLongTerms(10, 20, "some_name", null, 1, 1, heuristic, buckets);
            sTerms[1] = new SignificantLongTerms();
        } else {

            BytesRef term = new BytesRef("someterm");
            buckets.add(new SignificantStringTerms.Bucket(term, 1, 2, 3, 4, InternalAggregations.EMPTY));
            sTerms[0] = new SignificantStringTerms(10, 20, "some_name", 1, 1, heuristic, buckets);
            sTerms[1] = new SignificantStringTerms();
        }
        return sTerms;
    }

    SignificanceHeuristic getRandomSignificanceheuristic() {
        if (random().nextBoolean()) {
            return new DefaultHeuristic();
        } else {
            return new MutualInformation().setIncludeNegatives(random().nextBoolean());
        }
    }

    // test that
    // 1. The output of the builders can actually be parsed
    // 2. The parser does not swallow parameters after a significance heuristic was defined
    @Test
    public void testBuilderAndParser() throws Exception {

        Set<SignificanceHeuristicParser> parsers = new HashSet<>();
        parsers.add(new DefaultHeuristic.DefaultHeuristicParser());
        parsers.add(new MutualInformation.MutualInformationParser());
        SignificanceHeuristicParserMapper heuristicParserMapper = new SignificanceHeuristicParserMapper(parsers);
        SearchContext searchContext = new SignificantTermsTestSearchContext();

        // test default with string
        XContentParser stParser = JsonXContent.jsonXContent.createParser("{\"field\":\"text\", \"default\":{}, \"min_doc_count\":200}");
        stParser.nextToken();
        SignificantTermsAggregatorFactory aggregatorFactory = (SignificantTermsAggregatorFactory) new SignificantTermsParser(heuristicParserMapper).parse("testagg", stParser, searchContext);
        stParser.nextToken();
        assertThat(aggregatorFactory.getBucketCountThresholds().getMinDocCount(), equalTo(200l));
        assertThat(stParser.currentToken(), equalTo(null));
        stParser.close();

        // test default with builders
        SignificantTermsBuilder stBuilder = new SignificantTermsBuilder("testagg");
        stBuilder.significanceHeuristic(new DefaultHeuristic.DefaultHeuristicBuilder()).field("text").minDocCount(200);
        XContentBuilder stXContentBuilder = XContentFactory.jsonBuilder();
        stBuilder.internalXContent(stXContentBuilder, null);
        stParser = JsonXContent.jsonXContent.createParser(stXContentBuilder.string());
        stParser.nextToken();
        aggregatorFactory = (SignificantTermsAggregatorFactory) new SignificantTermsParser(heuristicParserMapper).parse("testagg", stParser, searchContext);
        stParser.nextToken();
        assertThat(aggregatorFactory.getBucketCountThresholds().getMinDocCount(), equalTo(200l));
        assertThat(stParser.currentToken(), equalTo(null));
        stParser.close();

        // test mutual_information with string
        stParser = JsonXContent.jsonXContent.createParser("{\"field\":\"text\", \"mutual_information\":{\"include_negatives\": false}, \"min_doc_count\":200}");
        stParser.nextToken();
        aggregatorFactory = (SignificantTermsAggregatorFactory) new SignificantTermsParser(heuristicParserMapper).parse("testagg", stParser, searchContext);
        stParser.nextToken();
        assertThat(aggregatorFactory.getBucketCountThresholds().getMinDocCount(), equalTo(200l));
        assertTrue(!((MutualInformation) aggregatorFactory.getSignificanceHeuristic()).getIncludeNegatives());
        assertThat(stParser.currentToken(), equalTo(null));
        stParser.close();

        // test mutual_information with builders
        stBuilder = new SignificantTermsBuilder("testagg");
        stBuilder.significanceHeuristic(new MutualInformation.MutualInformationBuilder().setIncludeNegatives(false)).field("text").minDocCount(200);
        stXContentBuilder = XContentFactory.jsonBuilder();
        stBuilder.internalXContent(stXContentBuilder, null);
        stParser = JsonXContent.jsonXContent.createParser(stXContentBuilder.string());
        stParser.nextToken();
        aggregatorFactory = (SignificantTermsAggregatorFactory) new SignificantTermsParser(heuristicParserMapper).parse("testagg", stParser, searchContext);
        stParser.nextToken();
        assertThat(aggregatorFactory.getBucketCountThresholds().getMinDocCount(), equalTo(200l));
        assertTrue(!((MutualInformation) aggregatorFactory.getSignificanceHeuristic()).getIncludeNegatives());
        assertThat(stParser.currentToken(), equalTo(null));
        stParser.close();
    }

    public void testLEAsserts(SignificanceHeuristic heuristic) throws Exception {
        try {
            heuristic.getScore(2, 3, 1, 4);
            fail();
        } catch (AssertionError assertionError) {
            assertNotNull(assertionError.getMessage());
            assertTrue(assertionError.getMessage().contains("subsetFreq <= supersetFreq"));
        }
        try {
            heuristic.getScore(1, 4, 2, 3);
            fail();
        } catch (AssertionError assertionError) {
            assertNotNull(assertionError.getMessage());
            assertTrue(assertionError.getMessage().contains("subsetSize <= supersetSize"));
        }
        try {
            heuristic.getScore(2, 1, 3, 4);
            fail();
        } catch (AssertionError assertionError) {
            assertNotNull(assertionError.getMessage());
            assertTrue(assertionError.getMessage().contains("subsetFreq <= subsetSize"));
        }
        try {
            heuristic.getScore(1, 2, 4, 3);
            fail();
        } catch (AssertionError assertionError) {
            assertNotNull(assertionError.getMessage());
            assertTrue(assertionError.getMessage().contains("supersetFreq <= supersetSize"));
        }
        try {
            heuristic.getScore(1, 3, 4, 4);
            fail();
        } catch (AssertionError assertionError) {
            assertNotNull(assertionError.getMessage());
            assertTrue(assertionError.getMessage().contains("supersetFreq - subsetFreq <= supersetSize - subsetSize"));
        }
        try {
            int idx = random().nextInt(4);
            long[] values = {1, 2, 3, 4};
            values[idx] *= -1;
            heuristic.getScore(values[0], values[1], values[2], values[3]);
            fail();
        } catch (AssertionError assertionError) {
            assertNotNull(assertionError.getMessage());
            assertTrue(assertionError.getMessage().contains("subsetFreq >= 0 && subsetSize >= 0 && supersetFreq >= 0 && supersetSize >= 0"));
        }
    }

    @Test
    public void testAssertions() throws Exception {
        testLEAsserts(new DefaultHeuristic());
        testLEAsserts(new MutualInformation());
    }

    @Test
    public void scoreDefault() {
        SignificanceHeuristic heuristic = new DefaultHeuristic();
        assertThat(heuristic.getScore(1, 1, 1, 3), greaterThan(0.0));
        assertThat(heuristic.getScore(1, 1, 2, 3), lessThan(heuristic.getScore(1, 1, 1, 3)));
        assertThat(heuristic.getScore(0, 1, 2, 3), equalTo(0.0));
        double score = 0.0;
        try {
            long a = Math.abs(random().nextLong());
            long b = Math.abs(random().nextLong());
            long c = Math.abs(random().nextLong());
            long d = random().nextLong();
            score = heuristic.getScore(a, b, c, d);
        } catch (AssertionError e) {
        }
        assertThat(score, greaterThanOrEqualTo(0.0));
    }

    @Test
    public void scoreMutual() throws Exception {
        SignificanceHeuristic heuristic = new MutualInformation();
        ((MutualInformation) heuristic).setIncludeNegatives(true);
        assertThat(heuristic.getScore(1, 1, 1, 3), greaterThan(0.0));
        assertThat(heuristic.getScore(1, 1, 2, 3), lessThan(heuristic.getScore(1, 1, 1, 3)));
        assertThat(heuristic.getScore(2, 2, 2, 4), equalTo(1.0));
        assertThat(heuristic.getScore(0, 2, 2, 4), equalTo(1.0));
        assertThat(heuristic.getScore(2, 2, 4, 4), equalTo(0.0));
        assertThat(heuristic.getScore(1, 2, 2, 4), equalTo(0.0));
        assertThat(heuristic.getScore(3, 6, 9, 18), equalTo(0.0));

        double score = 0.0;
        try {
            long a = Math.abs(random().nextLong());
            long b = Math.abs(random().nextLong());
            long c = Math.abs(random().nextLong());
            long d = random().nextLong();
            score = heuristic.getScore(a, b, c, d);
        } catch (AssertionError e) {
        }
        assertThat(score, lessThanOrEqualTo(1.0));
        assertThat(score, greaterThanOrEqualTo(0.0));
        ((MutualInformation) heuristic).setIncludeNegatives(false);
        assertThat(heuristic.getScore(0, 1, 2, 3), equalTo(-1.0 * Double.MAX_VALUE));

    }
}
