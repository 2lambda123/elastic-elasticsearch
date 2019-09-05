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
package org.elasticsearch.repositories.azure;

import com.microsoft.azure.storage.Constants;
import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.RetryPolicyFactory;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.http.HttpStatus;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressForbidden(reason = "this test uses a HttpServer to emulate an Azure endpoint")
public class AzureBlobStoreRepositoryTests extends ESBlobStoreRepositoryIntegTestCase {

    private static HttpServer httpServer;

    @BeforeClass
    public static void startHttpServer() throws Exception {
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
    }

    @Before
    public void setUpHttpServer() {
        HttpHandler handler = new InternalHttpHandler();
        if (randomBoolean()) {
            handler = new ErroneousHttpHandler(handler, randomIntBetween(2, 3));
        }
        httpServer.createContext("/container", handler);
    }

    @AfterClass
    public static void stopHttpServer() {
        httpServer.stop(0);
        httpServer = null;
    }

    @After
    public void tearDownHttpServer() {
        httpServer.removeContext("/container");
    }

    @Override
    protected String repositoryType() {
        return AzureRepository.TYPE;
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder()
            .put(AzureRepository.Repository.CONTAINER_SETTING.getKey(), "container")
            .put(AzureStorageSettings.ACCOUNT_SETTING.getKey(), "test")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestAzureRepositoryPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final String key = Base64.getEncoder().encodeToString(randomAlphaOfLength(10).getBytes(StandardCharsets.UTF_8));
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(AzureStorageSettings.ACCOUNT_SETTING.getConcreteSettingForNamespace("test").getKey(), "account");
        secureSettings.setString(AzureStorageSettings.KEY_SETTING.getConcreteSettingForNamespace("test").getKey(), key);

        final InetSocketAddress address = httpServer.getAddress();
        final String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=http://"
            + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();

        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(AzureStorageSettings.ENDPOINT_SUFFIX_SETTING.getConcreteSettingForNamespace("test").getKey(), endpoint)
            .setSecureSettings(secureSettings)
            .build();
    }

    /**
     * AzureRepositoryPlugin that allows to set very low values for the Azure's client retry policy
     */
    public static class TestAzureRepositoryPlugin extends AzureRepositoryPlugin {

        public TestAzureRepositoryPlugin(Settings settings) {
            super(settings);
        }

        @Override
        AzureStorageService createAzureStoreService(final Settings settings) {
            return new AzureStorageService(settings) {
                @Override
                RetryPolicyFactory createRetryPolicy(final AzureStorageSettings azureStorageSettings) {
                    return new RetryExponentialRetry(1, 100, 500, azureStorageSettings.getMaxRetries());
                }
            };
        }
    }

    /**
     * Minimal HTTP handler that acts as an Azure compliant server
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an Azure endpoint")
    private static class InternalHttpHandler implements HttpHandler {

        private final Map<String, BytesReference> blobs = new ConcurrentHashMap<>();

        @Override
        public void handle(final HttpExchange exchange) throws IOException {
            final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();
            try {
                if (Regex.simpleMatch("PUT /container/*", request)) {
                    blobs.put(exchange.getRequestURI().toString(), Streams.readFully(exchange.getRequestBody()));
                    exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);

                } else if (Regex.simpleMatch("HEAD /container/*", request)) {
                    BytesReference blob = blobs.get(exchange.getRequestURI().toString());
                    if (blob == null) {
                        exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                        return;
                    }
                    exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(blob.length()));
                    exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);

                } else if (Regex.simpleMatch("GET /container/*", request)) {
                    final BytesReference blob = blobs.get(exchange.getRequestURI().toString());
                    if (blob == null) {
                        exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                        return;
                    }
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(blob.length()));
                    exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), blob.length());
                    blob.writeTo(exchange.getResponseBody());

                } else if (Regex.simpleMatch("DELETE /container/*", request)) {
                    Streams.readFully(exchange.getRequestBody());
                    blobs.entrySet().removeIf(blob -> blob.getKey().startsWith(exchange.getRequestURI().toString()));
                    exchange.sendResponseHeaders(RestStatus.ACCEPTED.getStatus(), -1);

                } else if (Regex.simpleMatch("GET /container?restype=container&comp=list*", request)) {
                    final Map<String, String> params = new HashMap<>();
                    RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);

                    final StringBuilder list = new StringBuilder();
                    list.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                    list.append("<EnumerationResults>");
                    final String prefix = params.get("prefix");
                    list.append("<Blobs>");
                    for (Map.Entry<String, BytesReference> blob : blobs.entrySet()) {
                        if (prefix == null || blob.getKey().startsWith("/container/" + prefix)) {
                            list.append("<Blob><Name>").append(blob.getKey().replace("/container/", "")).append("</Name>");
                            list.append("<Properties><Content-Length>").append(blob.getValue().length()).append("</Content-Length>");
                            list.append("<BlobType>BlockBlob</BlobType></Properties></Blob>");
                        }
                    }
                    list.append("</Blobs>");
                    list.append("</EnumerationResults>");

                    byte[] response = list.toString().getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/xml");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);

                } else {
                    exchange.sendResponseHeaders(RestStatus.BAD_REQUEST.getStatus(), -1);
                }
            } finally {
                exchange.close();
            }
        }
    }


    /**
     * HTTP handler that injects random Azure service errors
     *
     * Note: it is not a good idea to allow this handler to simulate too many errors as it would
     * slow down the test suite.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an Azure endpoint")
    private static class ErroneousHttpHandler implements HttpHandler {

        // first key is the remote address, second key is the HTTP request unique id provided by the SDK client,
        // value is the number of times the request has been seen
        private final Map<String, AtomicInteger> requests;
        private final HttpHandler delegate;
        private final int maxErrorsPerRequest;

        private ErroneousHttpHandler(final HttpHandler delegate, final int maxErrorsPerRequest) {
            this.requests = new ConcurrentHashMap<>();
            this.delegate = delegate;
            this.maxErrorsPerRequest = maxErrorsPerRequest;
            assert maxErrorsPerRequest > 1;
        }

        @Override
        public void handle(final HttpExchange exchange) throws IOException {
            final String requestId = exchange.getRequestHeaders().getFirst(Constants.HeaderConstants.CLIENT_REQUEST_ID_HEADER);
            assert Strings.hasText(requestId);

            final int count = requests.computeIfAbsent(requestId, req -> new AtomicInteger(0)).incrementAndGet();
            if (count >= maxErrorsPerRequest || randomBoolean()) {
                requests.remove(requestId);
                delegate.handle(exchange);
            } else {
                handleAsError(exchange, requestId);
            }
        }

        private void handleAsError(final HttpExchange exchange, final String requestId) throws IOException {
            Streams.readFully(exchange.getRequestBody());
            exchange.getResponseHeaders().add(Constants.HeaderConstants.CLIENT_REQUEST_ID_HEADER, requestId);
            exchange.sendResponseHeaders(HttpStatus.SC_INTERNAL_SERVER_ERROR, -1);
            exchange.close();
        }
    }
}
