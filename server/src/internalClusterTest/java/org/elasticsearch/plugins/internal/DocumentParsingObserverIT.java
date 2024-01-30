/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.FilterXContentParserWrapper;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.cborBuilder;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class DocumentParsingObserverIT extends ESIntegTestCase {

    private static String TEST_INDEX_NAME = "test-index-name";

    // the assertions are done in plugin which is static and will be created by ES server.
    // hence a static flag to make sure it is indeed used
    public static boolean hasWrappedParser;

    public void testDocumentIsReportedUponBulk() throws IOException {
        hasWrappedParser = false;
        client().index(
            new IndexRequest(TEST_INDEX_NAME).id("1").source(jsonBuilder().startObject().field("test", "I am sam i am").endObject())
        ).actionGet();
        assertTrue(hasWrappedParser);
        // there are more assertions in a TestDocumentParsingObserver

        hasWrappedParser = false;
        // the format of the request does not matter
        client().index(
            new IndexRequest(TEST_INDEX_NAME).id("2").source(cborBuilder().startObject().field("test", "I am sam i am").endObject())
        ).actionGet();
        assertTrue(hasWrappedParser);
        // there are more assertions in a TestDocumentParsingObserver

        hasWrappedParser = false;
        // white spaces does not matter
        client().index(new IndexRequest(TEST_INDEX_NAME).id("3").source("""
            {
            "test":

            "I am sam i am"
            }
            """, XContentType.JSON)).actionGet();
        assertTrue(hasWrappedParser);
        // there are more assertions in a TestDocumentParsingObserver
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestDocumentParsingObserverPlugin.class);
    }

    public static class TestDocumentParsingObserverPlugin extends Plugin implements DocumentParsingObserverPlugin, IngestPlugin {

        public TestDocumentParsingObserverPlugin() {}

        @Override
        public DocumentParsingObserverSupplier getDocumentParsingObserverSupplier() {
            return new DocumentParsingObserverSupplier() {
                @Override
                public DocumentParsingObserver getNewObserver() {
                    return new TestDocumentParsingObserver();
                }

                @Override
                public DocumentParsingObserver forAlreadyParsedInIngest(long normalisedBytesParsed) {
                    return null;
                }

                @Override
                public DocumentParsingReporter getDocumentParsingReporter() {
                    return new TestDocumentParsingReporter();
                }
            };
        }
    }

    public static class TestDocumentParsingReporter implements DocumentParsingReporter {

        @Override
        public void onCompleted(String indexName, long normalizedBytesParsed) {
            assertThat(indexName, equalTo(TEST_INDEX_NAME));
            assertThat(normalizedBytesParsed, equalTo(5L));
        }
    }

    public static class TestDocumentParsingObserver implements DocumentParsingObserver {
        long counter = 0;

        @Override
        public XContentParser wrapParser(XContentParser xContentParser) {
            hasWrappedParser = true;
            return new FilterXContentParserWrapper(xContentParser) {
                @Override
                public Token nextToken() throws IOException {
                    counter++;
                    return super.nextToken();
                }
            };
        }

        @Override
        public long normalisedBytesParsed() {
            return counter;
        }
    }
}
