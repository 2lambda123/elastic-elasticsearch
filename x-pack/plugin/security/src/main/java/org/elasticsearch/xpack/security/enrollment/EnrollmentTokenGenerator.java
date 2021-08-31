/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.ssl.SslKeyConfig;
import org.elasticsearch.common.ssl.SslUtil;
import org.elasticsearch.common.ssl.StoreKeyConfig;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.enrollment.KibanaEnrollmentAction;
import org.elasticsearch.xpack.core.security.action.enrollment.NodeEnrollmentAction;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.tool.CommandLineHttpClient;
import org.elasticsearch.xpack.security.tool.HttpResponse;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class EnrollmentTokenGenerator {
    protected static final String ENROLL_API_KEY_EXPIRATION = "30m";

    private final Environment environment;
    private final SSLService sslService;
    private final CommandLineHttpClient client;
    private final URL defaultUrl;

    public EnrollmentTokenGenerator(Environment environment) throws MalformedURLException {
        this(environment, new CommandLineHttpClient(environment));
        // no methods of this class should log because log configuration might not be available
        // if it is available, it's the server one, which is not cool because this must not interfere with the server
    }

    // protected for testing
    protected EnrollmentTokenGenerator(Environment environment, CommandLineHttpClient client) throws MalformedURLException {
        this.environment = environment;
        this.sslService = new SSLService(environment);
        this.client = client;
        this.defaultUrl = new URL(client.getDefaultURL());
    }

    public EnrollmentToken createNodeEnrollmentToken(String user, SecureString password) throws Exception {
        return this.create(user, password, NodeEnrollmentAction.NAME);
    }

    public EnrollmentToken createKibanaEnrollmentToken(String user, SecureString password) throws Exception {
        return this.create(user, password, KibanaEnrollmentAction.NAME);
    }

    public List<EnrollmentToken> createEnrollmentTokens(String user, SecureString password) throws Exception {
        if (false == XPackSettings.ENROLLMENT_ENABLED.get(environment.settings())) {
            throw new IllegalStateException("Cannot create enrollment tokens if [" + XPackSettings.ENROLLMENT_ENABLED.getKey() +
                    "] is false");
        }
        final String fingerprint = getCaFingerprint();
        final EnrollmentNodeInfo enrollmentNodeInfo = getNodeInfo(user, password);
        // always create the Kibana enrollment token
        final String kibanaEnrollmentApiKey = getApiKeyCredentials(user, password, KibanaEnrollmentAction.NAME);
        final EnrollmentToken kibanaEnrollmentToken = new EnrollmentToken(kibanaEnrollmentApiKey, fingerprint,
                enrollmentNodeInfo.getStackVersion(), enrollmentNodeInfo.getAddresses());
        if (enrollmentNodeInfo.loopbackOnlyBind()) {
            return List.of(kibanaEnrollmentToken);
        } else {
            final String nodeEnrollmentApiKey = getApiKeyCredentials(user, password, NodeEnrollmentAction.NAME);
            final EnrollmentToken nodeEnrollmentToken = new EnrollmentToken(nodeEnrollmentApiKey, fingerprint,
                    enrollmentNodeInfo.getStackVersion(), enrollmentNodeInfo.getAddresses());
            return List.of(kibanaEnrollmentToken, nodeEnrollmentToken);
        }
    }

    private EnrollmentToken create(String user, SecureString password, String action) throws Exception {
        if (false == XPackSettings.ENROLLMENT_ENABLED.get(environment.settings())) {
            throw new IllegalStateException("Cannot create enrollment tokens if [" + XPackSettings.ENROLLMENT_ENABLED.getKey() +
                    "] is false");
        }
        final String fingerprint = getCaFingerprint();
        final EnrollmentNodeInfo enrollmentNodeInfo = getNodeInfo(user, password);
        final String apiKey = getApiKeyCredentials(user, password, action);
        return new EnrollmentToken(apiKey, fingerprint, enrollmentNodeInfo.getStackVersion(), enrollmentNodeInfo.getAddresses());
    }

    private HttpResponse.HttpResponseBuilder responseBuilder(InputStream is) throws IOException {
        final HttpResponse.HttpResponseBuilder httpResponseBuilder = new HttpResponse.HttpResponseBuilder();
        if (is == null) {
            throw new IllegalStateException("Empty response from server");
        }
        String responseBody = Streams.readFully(is).utf8ToString();
        httpResponseBuilder.withResponseBody(responseBody);
        return httpResponseBuilder;
    }

    protected URL createAPIKeyUrl() throws MalformedURLException, URISyntaxException {
        return new URL(defaultUrl, (defaultUrl.toURI().getPath() + "/_security/api_key").replaceAll("/+", "/"));
    }

    protected URL getLocalNodeInfoUrl() throws MalformedURLException, URISyntaxException {
        return new URL(defaultUrl, (defaultUrl.toURI().getPath() + "/_nodes/_local/http,transport").replaceAll("/+", "/"));
    }

    @SuppressWarnings("unchecked")
    protected static List<String> getBoundAddresses(Map<String, Object> nodesInfoResponseBody, String addressType) {
        Map<?, ?> nodesInfo = (Map<?, ?>) nodesInfoResponseBody.get("nodes");
        assert nodesInfo.size() == 1;
        Map<?, ?> nodeInfo = (Map<?, ?>) nodesInfo.values().iterator().next();
        Map<?, ?> http = (Map<?, ?>) nodeInfo.get(addressType);
        Object boundAddress = http.get("bound_address");
        if (boundAddress == null || false == (boundAddress instanceof List<?>) || ((List<?>) boundAddress).isEmpty() ||
                false == (((List<?>) boundAddress).get(0) instanceof String)) {
            throw new IllegalStateException("Unrecognized [" + addressType + "] bound addresses format in response [" +
                    nodesInfoResponseBody + "]");
        }
        return (List<String>) boundAddress;
    }

    @SuppressWarnings("unchecked")
    static String getVersion(Map<String, Object> nodesInfoResponseBody) {
        Map<?, ?> nodesInfo = (Map<?, ?>) nodesInfoResponseBody.get("nodes");
        assert nodesInfo.size() == 1;
        Map<?, ?> nodeInfo = (Map<?, ?>) nodesInfo.values().iterator().next();
        Object stackVersion = nodeInfo.get("version");
        if (stackVersion == null || false == (stackVersion instanceof String)) {
            throw new IllegalStateException("Unrecognized node version format in response " + nodesInfoResponseBody);
        }
        return (String) stackVersion;
    }

    protected String getApiKeyCredentials(String user, SecureString password, String action) throws Exception {
        final CheckedSupplier<String, Exception> createApiKeyRequestBodySupplier = () -> {
            XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
            xContentBuilder.startObject()
                .field("name", "enrollment_token_API_key_" + UUIDs.base64UUID())
                .field("expiration", ENROLL_API_KEY_EXPIRATION)
                .startObject("role_descriptors")
                .startObject("create_enrollment_token")
                .array("cluster", action)
                .endObject()
                .endObject()
                .endObject();
            return Strings.toString(xContentBuilder);
        };

        final URL createApiKeyUrl = createAPIKeyUrl();
        final HttpResponse httpResponseApiKey = client.execute("POST", createApiKeyUrl, user, password,
            createApiKeyRequestBodySupplier, is -> responseBuilder(is));
        final int httpCode = httpResponseApiKey.getHttpStatus();

        if (httpCode != HttpURLConnection.HTTP_OK) {
            throw new IllegalStateException("Unexpected response code [" + httpCode + "] from calling POST "
                + createApiKeyUrl);
        }

        final String apiKey = Objects.toString(httpResponseApiKey.getResponseBody().get("api_key"), "");
        final String apiId = Objects.toString(httpResponseApiKey.getResponseBody().get("id"), "");
        if (Strings.isNullOrEmpty(apiKey) || Strings.isNullOrEmpty(apiId)) {
            throw new IllegalStateException("Could not create an api key.");
        }
        return apiId + ":" + apiKey;
    }

    protected EnrollmentNodeInfo getNodeInfo(String user, SecureString password) throws Exception {
        final URL httpInfoUrl = getLocalNodeInfoUrl();
        final HttpResponse httpResponseHttp = client.execute("GET", httpInfoUrl, user, password, () -> null, is -> responseBuilder(is));
        final int httpCode = httpResponseHttp.getHttpStatus();

        if (httpCode != HttpURLConnection.HTTP_OK) {
            throw new IllegalStateException("Unexpected response code [" + httpCode + "] from calling GET " + httpInfoUrl);
        }

        final List<String> httpBoundAddresses = getBoundAddresses(httpResponseHttp.getResponseBody(), "http");
        final List<String> enrollmentTokenAddresses = getFilteredAddresses(httpBoundAddresses);
        final String stackVersion = getVersion(httpResponseHttp.getResponseBody());
        final List<String> transportBoundAddresses = getBoundAddresses(httpResponseHttp.getResponseBody(), "transport");
        boolean areAllTransportLoopback = areAllLoopback(transportBoundAddresses);
        return new EnrollmentNodeInfo(enrollmentTokenAddresses, stackVersion, areAllTransportLoopback);
    }

    protected String getCaFingerprint() throws Exception {
        final SslKeyConfig keyConfig = sslService.getHttpTransportSSLConfiguration().getKeyConfig();
        if (keyConfig instanceof StoreKeyConfig == false) {
            throw new IllegalStateException("Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration is " +
                "not configured with a keystore");
        }
        final List<Tuple<PrivateKey, X509Certificate>> httpCaKeysAndCertificates =
            ((StoreKeyConfig) keyConfig).getKeys().stream()
                .filter(t -> t.v2().getBasicConstraints() != -1)
                .collect(Collectors.toList());
        if (httpCaKeysAndCertificates.isEmpty()) {
            throw new IllegalStateException("Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration " +
                "Keystore doesn't contain any PrivateKey entries where the associated certificate is a CA certificate");
        } else if (httpCaKeysAndCertificates.size() > 1) {
            throw new IllegalStateException("Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration " +
                "Keystore contains multiple PrivateKey entries where the associated certificate is a CA certificate");
        }
        return SslUtil.calculateFingerprint(httpCaKeysAndCertificates.get(0).v2(), "SHA-256");
    }

    static List<String> getFilteredAddresses(List<String> addresses) throws Exception {
        List<String> filtered_addresses = new ArrayList<>();
        for (String bound_address : addresses){
            URI uri = new URI("http://" + bound_address);
            InetAddress inet_address = InetAddress.getByName(uri.getHost());
            if (inet_address.isLoopbackAddress() != true) {
                filtered_addresses.add(bound_address);
            }
        }
        return filtered_addresses.isEmpty() ? addresses : filtered_addresses;
    }

    static boolean areAllLoopback(List<String> addresses) throws Exception {
        for (String address : addresses) {
            URI uri = new URI("http://" + address);
            InetAddress inetAddress = InetAddress.getByName(uri.getHost());
            if (false == inetAddress.isLoopbackAddress()) {
                return false;
            }
        }
        return true;
    }

    static class EnrollmentNodeInfo {
        private final List<String> addresses;
        private final String stackVersion;
        private final boolean loopbackOnlyBind;

        EnrollmentNodeInfo(List<String> addresses, String stackVersion, boolean loopbackOnlyBind) {
            this.addresses = addresses;
            this.stackVersion = stackVersion;
            this.loopbackOnlyBind = loopbackOnlyBind;
        }

        public List<String> getAddresses() {
            return addresses;
        }

        public String getStackVersion() {
            return stackVersion;
        }

        public boolean loopbackOnlyBind() {
            return loopbackOnlyBind;
        }
    }
}
