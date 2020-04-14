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

package org.elasticsearch.rest;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpRequest;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.common.unit.ByteSizeValue.parseBytesSizeValue;
import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;

public class RestRequest implements ToXContent.Params {

    // tchar pattern as defined by RFC7230 section 3.2.6
    private static final Pattern TCHAR_PATTERN = Pattern.compile("[a-zA-z0-9!#$%&'*+\\-.\\^_`|~]+");

    private static final AtomicLong requestIdGenerator = new AtomicLong();

    private final NamedXContentRegistry xContentRegistry;
    private final Map<String, String> params;
    private final Map<String, List<String>> headers;
    private final String rawPath;
    private final Set<String> consumedParams = new HashSet<>();
    private final SetOnce<XContentType> xContentType = new SetOnce<>();
    private final HttpChannel httpChannel;

    private HttpRequest httpRequest;

    private boolean contentConsumed = false;

    private final long requestId;

    public boolean isContentConsumed() {
        return contentConsumed;
    }

    protected RestRequest(NamedXContentRegistry xContentRegistry, Map<String, String> params, String path,
                          Map<String, List<String>> headers, HttpRequest httpRequest, HttpChannel httpChannel) {
        this(xContentRegistry, params, path, headers, httpRequest, httpChannel, requestIdGenerator.incrementAndGet());
    }

    private RestRequest(NamedXContentRegistry xContentRegistry, Map<String, String> params, String path,
                        Map<String, List<String>> headers, HttpRequest httpRequest, HttpChannel httpChannel, long requestId) {
        this(xContentRegistry, params, path, headers, httpRequest, httpChannel, requestId, true);

    }
    private RestRequest(NamedXContentRegistry xContentRegistry, Map<String, String> params, String path,
                        Map<String, List<String>> headers, HttpRequest httpRequest, HttpChannel httpChannel,
                        long requestId, boolean headersValidation) {
        final XContentType xContentType;
        try {
            xContentType = parseContentType(headers.get("Content-Type"));
        } catch (final IllegalArgumentException e) {
            throw new ContentTypeHeaderException(e);
        }
        if (xContentType != null) {
            this.xContentType.set(xContentType);
        }
        this.xContentRegistry = xContentRegistry;
        this.httpRequest = httpRequest;
        this.httpChannel = httpChannel;
        this.params = params;
        this.rawPath = path;
        this.headers = Collections.unmodifiableMap(headers);
        this.requestId = requestId;
        addCompatibleParameter(headersValidation);
    }

    protected RestRequest(RestRequest restRequest) {
        this(restRequest.getXContentRegistry(), restRequest.params(), restRequest.path(), restRequest.getHeaders(),
            restRequest.getHttpRequest(), restRequest.getHttpChannel(), restRequest.getRequestId());
    }

    /**
     * Invoke {@link HttpRequest#releaseAndCopy()} on the http request in this instance and replace a pooled http request
     * with an unpooled copy. This is supposed to be used before passing requests to {@link RestHandler} instances that can not safely
     * handle http requests that use pooled buffers as determined by {@link RestHandler#allowsUnsafeBuffers()}.
     */
    void ensureSafeBuffers() {
        httpRequest = httpRequest.releaseAndCopy();
    }

    /**
     * Creates a new REST request. This method will throw {@link BadParameterException} if the path cannot be
     * decoded
     *
     * @param xContentRegistry the content registry
     * @param httpRequest      the http request
     * @param httpChannel      the http channel
     * @throws BadParameterException      if the parameters can not be decoded
     * @throws ContentTypeHeaderException if the Content-Type header can not be parsed
     */
    public static RestRequest request(NamedXContentRegistry xContentRegistry, HttpRequest httpRequest, HttpChannel httpChannel) {
        Map<String, String> params = params(httpRequest.uri());
        String path = path(httpRequest.uri());
        return new RestRequest(xContentRegistry, params, path, httpRequest.getHeaders(), httpRequest, httpChannel,
            requestIdGenerator.incrementAndGet());
    }

    private void addCompatibleParameter(boolean headersValidation) {
        if(headersValidation){
//            Version compatible = compatibleWithVersion();
//            if (Version.minimumRestCompatibilityVersion().equals(compatible)) {
            if(isRequestingCompatibility()) {
                params().put(CompatibleConstants.COMPATIBLE_PARAMS_KEY,
                    String.valueOf(Version.minimumRestCompatibilityVersion().major));
                //use it so it won't fail request validation with unused parameter
                param(CompatibleConstants.COMPATIBLE_PARAMS_KEY);
            }
        }
    }
    private boolean isRequestingCompatibility() {
        String acceptHeader = header(CompatibleConstants.COMPATIBLE_ACCEPT_HEADER);
        String aVersion = XContentType.parseVersion(acceptHeader);
        byte acceptVersion = aVersion == null ? Version.CURRENT.major : Integer.valueOf(aVersion).byteValue();
        String contentTypeHeader = header(CompatibleConstants.COMPATIBLE_CONTENT_TYPE_HEADER);
        String cVersion = XContentType.parseVersion(contentTypeHeader);
        byte contentTypeVersion = cVersion == null ? Version.CURRENT.major : Integer.valueOf(cVersion).byteValue();

        if(Version.CURRENT.major < acceptVersion || Version.CURRENT.major - acceptVersion > 1 ){
//            throw new IllegalStateException("something about only 1 major version support");
            throw new CompatibleApiHeadersCombinationException(
                String.format(Locale.ROOT, "Request with a body and incompatible Accept and Content-Type header values. " +
                        "Accept=%s Content-Type=%s hasContent=%b %s %s %s", acceptHeader,
                    contentTypeHeader, hasContent(), path(), params.toString(), method().toString()));
        }
        if (hasContent()) {
            if(Version.CURRENT.major < contentTypeVersion || Version.CURRENT.major - contentTypeVersion > 1 ){
//                throw new IllegalStateException("something about only 1 major version support");
                throw new CompatibleApiHeadersCombinationException(
                    String.format(Locale.ROOT, "Request with a body and incompatible Accept and Content-Type header values. " +
                            "Accept=%s Content-Type=%s hasContent=%b %s %s %s", acceptHeader,
                        contentTypeHeader, hasContent(), path(), params.toString(), method().toString()));
            }

            if (contentTypeVersion != acceptVersion) {
//                throw new IllegalStateException("something about needing to match");
                throw new CompatibleApiHeadersCombinationException(
                    String.format(Locale.ROOT, "Request with a body and incompatible Accept and Content-Type header values. " +
                            "Accept=%s Content-Type=%s hasContent=%b %s %s %s", acceptHeader,
                        contentTypeHeader, hasContent(), path(), params.toString(), method().toString()));
            }
        }

        return contentTypeVersion < Version.CURRENT.major || acceptVersion < Version.CURRENT.major;
    }

    private Version compatibleWithVersion() {
        String currentVersion = String.valueOf(Version.CURRENT.major);
        String previousVersion = String.valueOf(Version.CURRENT.major - 1);

        String acceptHeader = header(CompatibleConstants.COMPATIBLE_ACCEPT_HEADER);
        String acceptVersion = XContentType.parseVersion(acceptHeader);
        String contentTypeHeader = header(CompatibleConstants.COMPATIBLE_CONTENT_TYPE_HEADER);
        String contentTypeVersion = XContentType.parseVersion(contentTypeHeader);

        //TODO not sure about this one as this does not cover text formats
        boolean isSupportedMediaTypeAccept = acceptHeader == null || XContentType.parseMediaType(acceptHeader) != null;

        boolean isSupportedMediaTypeContentType = contentTypeHeader == null || XContentType.parseMediaType(contentTypeHeader) != null;

        if (hasContent()) {
            //both headers versioned
            if (acceptVersion != null && contentTypeVersion != null) {
                // both Accept and Content-Type are versioned and set to a previous version
                if (previousVersion.equals(acceptVersion) && previousVersion.equals(contentTypeVersion)) {
                    return Version.minimumRestCompatibilityVersion();
                }
                // both Accept and Content-Type are versioned to a current version
                if (currentVersion.equals(acceptVersion) && currentVersion.equals(contentTypeVersion)) {
                    return Version.CURRENT;
                }
                // both headers are versioned but set to incorrect version
                throw new CompatibleApiHeadersCombinationException(
                    String.format(Locale.ROOT, "Request with a body and incompatible Accept and Content-Type header values. " +
                            "Accept=%s Content-Type=%s hasContent=%b %s %s %s", acceptHeader,
                        contentTypeHeader, hasContent(), path(), params.toString(), method().toString()));
            }
            // Content-Type is versioned but accept is not present
            if(previousVersion.equals(contentTypeVersion)
                && (acceptHeader == null || acceptHeader.equals("*/*") )) {
                return Version.minimumRestCompatibilityVersion();
            }
            // Content type is set (not versioned) but accept is not present. It will be defaulted to JSON
            if(isSupportedMediaTypeContentType &&
                (acceptHeader == null || acceptHeader.equals("*/*") )){//TODO when do we default this?
                return Version.CURRENT;
            }
            // both Accept and Content-Type are not a compatible format, but are supported and not empty
            if (isSupportedMediaTypeContentType && isSupportedMediaTypeAccept &&
                acceptHeader != null && contentTypeHeader != null) {
                return Version.CURRENT;
            }
        } else {
            if (acceptVersion != null) {
                //Accept header is versioned and set to previous
                if (previousVersion.equals(acceptVersion)) {
                    return Version.minimumRestCompatibilityVersion();
                }
                if (currentVersion.equals(acceptVersion)) {
                    return Version.CURRENT;
                }
                // Accept header is versioned but set to incorrect version
                throw new CompatibleApiHeadersCombinationException(
                    String.format(Locale.ROOT, "Request with a body and incompatible Accept and Content-Type header values. " +
                            "Accept=%s Content-Type=%s hasContent=%b %s %s %s", acceptHeader,
                        contentTypeHeader, hasContent(), path(), params.toString(), method().toString()));
            }
            //Accept header is not versioned but is supported
            if (isSupportedMediaTypeAccept) {
                return Version.CURRENT;
            }
        }

        throw new CompatibleApiHeadersCombinationException(
            String.format(Locale.ROOT, "Request with a body and incompatible Accept and Content-Type header values. " +
                    "Accept=%s Content-Type=%s hasContent=%b %s %s %s", acceptHeader,
                contentTypeHeader, hasContent(), path(), params.toString(), method().toString()));
    }

    private static Map<String, String> params(final String uri) {
        final Map<String, String> params = new HashMap<>();
        int index = uri.indexOf('?');
        if (index >= 0) {
            try {
                RestUtils.decodeQueryString(uri, index + 1, params);
            } catch (final IllegalArgumentException e) {
                throw new BadParameterException(e);
            }
        }
        return params;
    }

    private static String path(final String uri) {
        final int index = uri.indexOf('?');
        if (index >= 0) {
            return uri.substring(0, index);
        } else {
            return uri;
        }
    }

    /**
     * Creates a new REST request. The path is not decoded so this constructor will not throw a
     * {@link BadParameterException}.
     *
     * @param xContentRegistry the content registry
     * @param httpRequest      the http request
     * @param httpChannel      the http channel
     * @throws ContentTypeHeaderException if the Content-Type header can not be parsed
     */
    public static RestRequest requestWithoutParameters(NamedXContentRegistry xContentRegistry, HttpRequest httpRequest,
                                                       HttpChannel httpChannel) {
        Map<String, String> params = Collections.emptyMap();
        return new RestRequest(xContentRegistry, params, httpRequest.uri(), httpRequest.getHeaders(), httpRequest, httpChannel,
            requestIdGenerator.incrementAndGet(), false);
    }

    public static RestRequest requestWithoutContentType(NamedXContentRegistry xContentRegistry, HttpRequest httpRequest,
                                                       HttpChannel httpChannel) {
        HttpRequest httpRequestWithoutContentType = httpRequest.removeHeader("Content-Type");
        Map<String, String> params = params(httpRequest.uri());
        String path = path(httpRequest.uri());
        return new RestRequest(xContentRegistry, params, path, httpRequestWithoutContentType.getHeaders(),
            httpRequestWithoutContentType, httpChannel,
            requestIdGenerator.incrementAndGet(), false);
    }


    public static RestRequest requestNoValidation(NamedXContentRegistry xContentRegistry,
                                                  HttpRequest httpRequest,
                                                  HttpChannel httpChannel) {
        Map<String, String> params = Collections.emptyMap();
        HttpRequest httpRequestWithoutContentType = httpRequest.removeHeader("Content-Type");

        return new RestRequest(xContentRegistry, params, httpRequestWithoutContentType.uri(),
            httpRequestWithoutContentType.getHeaders(), httpRequestWithoutContentType, httpChannel,
            requestIdGenerator.incrementAndGet(), false);
    }


    public enum Method {
        GET, POST, PUT, DELETE, OPTIONS, HEAD, PATCH, TRACE, CONNECT
    }

    /**
     * Returns the HTTP method used in the REST request.
     *
     * @return the {@link Method} used in the REST request
     * @throws IllegalArgumentException if the HTTP method is invalid
     */
    public Method method() {
        return httpRequest.method();
    }

    /**
     * The uri of the rest request, with the query string.
     */
    public String uri() {
        return httpRequest.uri();
    }

    /**
     * The non decoded, raw path provided.
     */
    public String rawPath() {
        return rawPath;
    }

    /**
     * The path part of the URI (without the query string), decoded.
     */
    public final String path() {
        return RestUtils.decodeComponent(rawPath());
    }

    public boolean hasContent() {
        return contentLength() > 0;
    }

    public int contentLength() {
        return httpRequest.content().length();
    }

    public BytesReference content() {
        this.contentConsumed = true;
        return httpRequest.content();
    }

    /**
     * @return content of the request body or throw an exception if the body or content type is missing
     */
    public final BytesReference requiredContent() {
        if (hasContent() == false) {
            throw new ElasticsearchParseException("request body is required");
        } else if (xContentType.get() == null) {
            throw new IllegalStateException("unknown content type");
        }
        return content();
    }

    /**
     * Get the value of the header or {@code null} if not found. This method only retrieves the first header value if multiple values are
     * sent. Use of {@link #getAllHeaderValues(String)} should be preferred
     */
    public final String header(String name) {
        List<String> values = headers.get(name);
        if (values != null && values.isEmpty() == false) {
            return values.get(0);
        }
        return null;
    }

    /**
     * Get all values for the header or {@code null} if the header was not found
     */
    public final List<String> getAllHeaderValues(String name) {
        List<String> values = headers.get(name);
        if (values != null) {
            return Collections.unmodifiableList(values);
        }
        return null;
    }

    /**
     * Get all of the headers and values associated with the headers. Modifications of this map are not supported.
     */
    public final Map<String, List<String>> getHeaders() {
        return headers;
    }

    public final long getRequestId() {
        return requestId;
    }

    /**
     * The {@link XContentType} that was parsed from the {@code Content-Type} header. This value will be {@code null} in the case of
     * a request without a valid {@code Content-Type} header, a request without content ({@link #hasContent()}, or a plain text request
     */
    @Nullable
    public final XContentType getXContentType() {
        return xContentType.get();
    }

    public HttpChannel getHttpChannel() {
        return httpChannel;
    }

    public HttpRequest getHttpRequest() {
        return httpRequest;
    }

    public final boolean hasParam(String key) {
        return params.containsKey(key);
    }

    @Override
    public final String param(String key) {
        consumedParams.add(key);
        return params.get(key);
    }

    @Override
    public final String param(String key, String defaultValue) {
        consumedParams.add(key);
        String value = params.get(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    public Map<String, String> params() {
        return params;
    }

    /**
     * Returns a list of parameters that have been consumed. This method returns a copy, callers
     * are free to modify the returned list.
     *
     * @return the list of currently consumed parameters.
     */
    List<String> consumedParams() {
        return new ArrayList<>(consumedParams);
    }

    /**
     * Returns a list of parameters that have not yet been consumed. This method returns a copy,
     * callers are free to modify the returned list.
     *
     * @return the list of currently unconsumed parameters.
     */
    List<String> unconsumedParams() {
        return params
            .keySet()
            .stream()
            .filter(p -> !consumedParams.contains(p))
            .collect(Collectors.toList());
    }

    public float paramAsFloat(String key, float defaultValue) {
        String sValue = param(key);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Float.parseFloat(sValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Failed to parse float parameter [" + key + "] with value [" + sValue + "]", e);
        }
    }

    public int paramAsInt(String key, int defaultValue) {
        String sValue = param(key);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(sValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Failed to parse int parameter [" + key + "] with value [" + sValue + "]", e);
        }
    }

    public long paramAsLong(String key, long defaultValue) {
        String sValue = param(key);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(sValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Failed to parse long parameter [" + key + "] with value [" + sValue + "]", e);
        }
    }

    @Override
    public boolean paramAsBoolean(String key, boolean defaultValue) {
        String rawParam = param(key);
        // Treat empty string as true because that allows the presence of the url parameter to mean "turn this on"
        if (rawParam != null && rawParam.length() == 0) {
            return true;
        } else {
            return Booleans.parseBoolean(rawParam, defaultValue);
        }
    }

    @Override
    public Boolean paramAsBoolean(String key, Boolean defaultValue) {
        return Booleans.parseBoolean(param(key), defaultValue);
    }

    public TimeValue paramAsTime(String key, TimeValue defaultValue) {
        return parseTimeValue(param(key), defaultValue, key);
    }

    public ByteSizeValue paramAsSize(String key, ByteSizeValue defaultValue) {
        return parseBytesSizeValue(param(key), defaultValue, key);
    }

    public String[] paramAsStringArray(String key, String[] defaultValue) {
        String value = param(key);
        if (value == null) {
            return defaultValue;
        }
        return Strings.splitStringByCommaToArray(value);
    }

    public String[] paramAsStringArrayOrEmptyIfAll(String key) {
        String[] params = paramAsStringArray(key, Strings.EMPTY_ARRAY);
        if (Strings.isAllOrWildcard(params)) {
            return Strings.EMPTY_ARRAY;
        }
        return params;
    }

    /**
     * Get the {@link NamedXContentRegistry} that should be used to create parsers from this request.
     */
    public NamedXContentRegistry getXContentRegistry() {
        return xContentRegistry;
    }

    /**
     * A parser for the contents of this request if there is a body, otherwise throws an {@link ElasticsearchParseException}. Use
     * {@link #applyContentParser(CheckedConsumer)} if you want to gracefully handle when the request doesn't have any contents. Use
     * {@link #contentOrSourceParamParser()} for requests that support specifying the request body in the {@code source} param.
     */
    public final XContentParser contentParser() throws IOException {
        BytesReference content = requiredContent(); // will throw exception if body or content type missing
        return xContentType.get().xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, content.streamInput());
    }

    /**
     * If there is any content then call {@code applyParser} with the parser, otherwise do nothing.
     */
    public final void applyContentParser(CheckedConsumer<XContentParser, IOException> applyParser) throws IOException {
        if (hasContent()) {
            try (XContentParser parser = contentParser()) {
                applyParser.accept(parser);
            }
        }
    }

    /**
     * Does this request have content or a {@code source} parameter? Use this instead of {@link #hasContent()} if this
     * {@linkplain RestHandler} treats the {@code source} parameter like the body content.
     */
    public final boolean hasContentOrSourceParam() {
        return hasContent() || hasParam("source");
    }

    /**
     * A parser for the contents of this request if it has contents, otherwise a parser for the {@code source} parameter if there is one,
     * otherwise throws an {@link ElasticsearchParseException}. Use {@link #withContentOrSourceParamParserOrNull(CheckedConsumer)} instead
     * if you need to handle the absence request content gracefully.
     */
    public final XContentParser contentOrSourceParamParser() throws IOException {
        Tuple<XContentType, BytesReference> tuple = contentOrSourceParam();
        return tuple.v1().xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, tuple.v2().streamInput());
    }

    /**
     * Call a consumer with the parser for the contents of this request if it has contents, otherwise with a parser for the {@code source}
     * parameter if there is one, otherwise with {@code null}. Use {@link #contentOrSourceParamParser()} if you should throw an exception
     * back to the user when there isn't request content.
     */
    public final void withContentOrSourceParamParserOrNull(CheckedConsumer<XContentParser, IOException> withParser) throws IOException {
        if (hasContentOrSourceParam()) {
            Tuple<XContentType, BytesReference> tuple = contentOrSourceParam();
            BytesReference content = tuple.v2();
            XContentType xContentType = tuple.v1();
            try (InputStream stream = content.streamInput();
                 XContentParser parser = xContentType.xContent()
                     .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
                withParser.accept(parser);
            }
        } else {
            withParser.accept(null);
        }
    }

    /**
     * Get the content of the request or the contents of the {@code source} param or throw an exception if both are missing.
     * Prefer {@link #contentOrSourceParamParser()} or {@link #withContentOrSourceParamParserOrNull(CheckedConsumer)} if you need a parser.
     */
    public final Tuple<XContentType, BytesReference> contentOrSourceParam() {
        if (hasContentOrSourceParam() == false) {
            throw new ElasticsearchParseException("request body or source parameter is required");
        } else if (hasContent()) {
            return new Tuple<>(xContentType.get(), requiredContent());
        }
        String source = param("source");
        String typeParam = param("source_content_type");
        if (source == null || typeParam == null) {
            throw new IllegalStateException("source and source_content_type parameters are required");
        }
        BytesArray bytes = new BytesArray(source);
        final XContentType xContentType = parseContentType(Collections.singletonList(typeParam));
        if (xContentType == null) {
            throw new IllegalStateException("Unknown value for source_content_type [" + typeParam + "]");
        }
        return new Tuple<>(xContentType, bytes);
    }

    /**
     * Parses the given content type string for the media type. This method currently ignores parameters.
     */
    // TODO stop ignoring parameters such as charset...
    public static XContentType parseContentType(List<String> header) {
        if (header == null || header.isEmpty()) {
            return null;
        } else if (header.size() > 1) {
            throw new IllegalArgumentException("only one Content-Type header should be provided");
        }

        String rawContentType = header.get(0);
        final String[] elements = rawContentType.split("[ \t]*;");
        if (elements.length > 0) {
            final String[] splitMediaType = elements[0].split("/");
            if (splitMediaType.length == 2 && TCHAR_PATTERN.matcher(splitMediaType[0]).matches()
                && TCHAR_PATTERN.matcher(splitMediaType[1].trim()).matches()) {
                return XContentType.fromMediaType(elements[0]);
            } else {
                throw new IllegalArgumentException("invalid Content-Type header [" + rawContentType + "]");
            }
        }
        throw new IllegalArgumentException("empty Content-Type header");
    }

    public static class ContentTypeHeaderException extends RuntimeException {

        ContentTypeHeaderException(final IllegalArgumentException cause) {
            super(cause);
        }

    }

    public static class BadParameterException extends RuntimeException {

        BadParameterException(final IllegalArgumentException cause) {
            super(cause);
        }

    }

    public static class CompatibleApiHeadersCombinationException extends RuntimeException {

        CompatibleApiHeadersCombinationException(String cause) {
            super(cause);
        }

    }

}
