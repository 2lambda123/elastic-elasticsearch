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

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.xcontent.cbor.CborXContent;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.smile.SmileXContent;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;

import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The content type of {@link org.elasticsearch.common.xcontent.XContent}.
 */
public enum XContentType {

    /**
     * A JSON based content type.
     */
    JSON(0) {
        @Override
        public String mediaTypeWithoutParameters() {
            return "application/json";
        }

        @Override
        public String mediaType() {
            return "application/json; charset=UTF-8";
        }

        @Override
        public String shortName() {
            return "json";
        }

        @Override
        public XContent xContent() {
            return JsonXContent.jsonXContent;
        }
    },
    /**
     * The jackson based smile binary format. Fast and compact binary format.
     */
    SMILE(1) {
        @Override
        public String mediaTypeWithoutParameters() {
            return "application/smile";
        }

        @Override
        public String shortName() {
            return "smile";
        }

        @Override
        public XContent xContent() {
            return SmileXContent.smileXContent;
        }
    },
    /**
     * A YAML based content type.
     */
    YAML(2) {
        @Override
        public String mediaTypeWithoutParameters() {
            return "application/yaml";
        }

        @Override
        public String shortName() {
            return "yaml";
        }

        @Override
        public XContent xContent() {
            return YamlXContent.yamlXContent;
        }
    },
    /**
     * A CBOR based content type.
     */
    CBOR(3) {
        @Override
        public String mediaTypeWithoutParameters() {
            return "application/cbor";
        }

        @Override
        public String shortName() {
            return "cbor";
        }

        @Override
        public XContent xContent() {
            return CborXContent.cborXContent;
        }
    };

    /**
     * A regexp to allow parsing media types. It covers two use cases.
     * 1. Media type with a version - requires a custom vnd.elasticserach subtype and a compatible-with parameter
     * i.e. application/vnd.elasticsearch+json;compatible-with
     * 2. Media type without a version - for users not using compatible API i.e. application/json
     */
    private static final Pattern COMPATIBLE_API_HEADER_PATTERN = Pattern.compile(
            //type
        "^(application|text)/" +
            // custom subtype and a version: vnd.elasticsearch+json;compatible-with=7
            "((vnd\\.elasticsearch\\+([^;\\s]+)(\\s*;\\s*compatible-with=(\\d+)))" +
            "|([^;\\s]+))" + //subtype: json,yaml,etc some of these are defined in x-pack so can't be enumerated
            "(?:\\s*;\\s*(charset=UTF-8)?)?$",
        Pattern.CASE_INSENSITIVE);


    /*Pattern.compile(
        "^(application|text)/(vnd\\.elasticsearch\\+)?([^;]+)(\\s*;\\s*compatible-with=(\\d+))?$",
        Pattern.CASE_INSENSITIVE);
*/
    /**
     * Accepts either a format string, which is equivalent to {@link XContentType#shortName()} or a media type that optionally has
     * parameters and attempts to match the value to an {@link XContentType}. The comparisons are done in lower case format and this method
     * also supports a wildcard accept for {@code application/*}. This method can be used to parse the {@code Accept} HTTP header or a
     * format query string parameter. This method will return {@code null} if no match is found
     */
    public static XContentType fromMediaTypeOrFormat(String mediaTypeHeaderValue) {
        String mediaType = parseMediaType(mediaTypeHeaderValue);

        if (mediaType == null) {
            return null;
        }
        for (XContentType type : values()) {
            if (isSameMediaTypeOrFormatAs(mediaType, type)) {
                return type;
            }
        }
        final String lowercaseMediaType = mediaType.toLowerCase(Locale.ROOT);
        if (lowercaseMediaType.startsWith("application/*") || lowercaseMediaType.equals("*/*")) {
            return JSON;
        }

        return null;
    }

    /**
     * Attempts to match the given media type with the known {@link XContentType} values. This match is done in a case-insensitive manner.
     * The provided media type should not include any parameters. This method is suitable for parsing part of the {@code Content-Type}
     * HTTP header. This method will return {@code null} if no match is found
     */
    public static XContentType fromMediaType(String mediaTypeHeaderValue) {
        String mediaType = parseMediaType(mediaTypeHeaderValue);

        final String lowercaseMediaType = Objects.requireNonNull(mediaType, "mediaType cannot be null").toLowerCase(Locale.ROOT);
        for (XContentType type : values()) {
            if (type.mediaTypeWithoutParameters().equals(lowercaseMediaType)) {
                return type;
            }
        }
        // we also support newline delimited JSON: http://specs.okfnlabs.org/ndjson/
        if (lowercaseMediaType.toLowerCase(Locale.ROOT).equals("application/x-ndjson")) {
            return XContentType.JSON;
        }

        return null;
    }

    // package private for testing
    static String parseMediaType(String mediaType) {
        if (mediaType != null) {
            Matcher matcher = COMPATIBLE_API_HEADER_PATTERN.matcher(mediaType);
            if (matcher.find()) {
                if(matcher.group(2).toLowerCase(Locale.ROOT).startsWith("vnd.elasticsearch")){
                    return (matcher.group(1) + "/" + matcher.group(4)).toLowerCase(Locale.ROOT);
                }
                return (matcher.group(1) + "/" + matcher.group(7)).toLowerCase(Locale.ROOT);
            }
        }

        return null;
    }

    // package private for testing
    static String parseVersion(String mediaType){
        if(mediaType != null){
            Matcher matcher = COMPATIBLE_API_HEADER_PATTERN.matcher(mediaType);
            if (matcher.find() && matcher.group(2).toLowerCase(Locale.ROOT).startsWith("vnd.elasticsearch")) {
                return matcher.group(6);
            }
        }
        return null;
    }
    private static boolean isSameMediaTypeOrFormatAs(String stringType, XContentType type) {
        return type.mediaTypeWithoutParameters().equalsIgnoreCase(stringType) ||
                stringType.toLowerCase(Locale.ROOT).startsWith(type.mediaTypeWithoutParameters().toLowerCase(Locale.ROOT) + ";") ||
                type.shortName().equalsIgnoreCase(stringType);
    }

    private int index;

    XContentType(int index) {
        this.index = index;
    }

    public int index() {
        return index;
    }

    public String mediaType() {
        return mediaTypeWithoutParameters();
    }

    public abstract String shortName();

    public abstract XContent xContent();

    public abstract String mediaTypeWithoutParameters();

}
