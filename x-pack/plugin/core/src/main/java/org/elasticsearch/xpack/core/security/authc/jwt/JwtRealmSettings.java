/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc.jwt;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.ClaimSetting;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Settings for JWT realms.
 */
public class JwtRealmSettings {

    private JwtRealmSettings() {}

    public static final String TYPE = "jwt";

    public static final List<String> SUPPORTED_SIGNATURE_ALGORITHMS = List.of(
        "HS256",
        "HS384",
        "HS512",
        "RS256",
        "RS384",
        "RS512",
        "ES256",
        "ES384",
        "ES512",
        "PS256",
        "PS384",
        "PS512"
    );

    public static final String SUPPORTED_CLIENT_AUTHENTICATION_TYPE_SHARED_SECRET = "sharedsecret";
    public static final String SUPPORTED_CLIENT_AUTHENTICATION_TYPE_NONE = "none";
    public static final List<String> SUPPORTED_CLIENT_AUTHENTICATION_TYPE = List.of(
        SUPPORTED_CLIENT_AUTHENTICATION_TYPE_SHARED_SECRET,
        SUPPORTED_CLIENT_AUTHENTICATION_TYPE_NONE
    );

    // Default values and min/max constraints

    private static final TimeValue DEFAULT_ALLOWED_CLOCK_SKEW = TimeValue.timeValueSeconds(60);
    private static final List<String> DEFAULT_ALLOWED_SIGNATURE_ALGORITHMS = Collections.singletonList("RS256");
    private static final List<String> DEFAULT_ALLOWED_AUDIENCES = Collections.emptyList();
    private static final boolean DEFAULT_POPULATE_USER_METADATA = true;
    private static final String DEFAULT_CLIENT_AUTHENTICATION_TYPE = SUPPORTED_CLIENT_AUTHENTICATION_TYPE_SHARED_SECRET;
    private static final TimeValue DEFAULT_CACHE_TTL = TimeValue.timeValueMinutes(20);
    private static final int DEFAULT_CACHE_MAX_USERS = 100_000;
    private static final int MIN_CACHE_MAX_USERS = 0;
    private static final TimeValue DEFAULT_HTTP_CONNECT_TIMEOUT = TimeValue.timeValueSeconds(5);
    private static final TimeValue DEFAULT_HTTP_CONNECTION_READ_TIMEOUT = TimeValue.timeValueSeconds(5);
    private static final TimeValue DEFAULT_HTTP_SOCKET_TIMEOUT = TimeValue.timeValueSeconds(5);
    private static final int DEFAULT_HTTP_MAX_CONNECTIONS = 200;
    private static final int MIN_HTTP_MAX_CONNECTIONS = 0;
    private static final int DEFAULT_HTTP_MAX_ENDPOINT_CONNECTIONS = 200;
    private static final int MIN_HTTP_MAX_ENDPOINT_CONNECTIONS = 0;

    // All settings

    public static Set<Setting.AffixSetting<?>> getSettings() {
        final Set<Setting.AffixSetting<?>> set = Sets.newHashSet();
        // Standard realm settings: order, enabled
        set.addAll(RealmSettings.getStandardSettings(TYPE));
        // JWT Issuer settings
        set.addAll(List.of(ALLOWED_ISSUER, ALLOWED_SIGNATURE_ALGORITHMS, ALLOWED_CLOCK_SKEW, JWKSET_PATH));
        // JWT Audience settings
        set.addAll(List.of(ALLOWED_AUDIENCES));
        // JWT End-user settings
        set.addAll(
            List.of(
                CLAIMS_PRINCIPAL.getClaim(),
                CLAIMS_PRINCIPAL.getPattern(),
                CLAIMS_GROUPS.getClaim(),
                CLAIMS_GROUPS.getPattern(),
                POPULATE_USER_METADATA
            )
        );
        // Delegated authorization settings: authorization_realms
        set.addAll(DelegatedAuthorizationSettings.getSettings(TYPE));
        // JWT Cache settings
        set.addAll(List.of(CACHE_TTL, CACHE_MAX_USERS));
        // Standard HTTP settings for outgoing connections to get JWT issuer jwkset_path
        set.addAll(
            List.of(
                HTTP_CONNECT_TIMEOUT,
                HTTP_CONNECTION_READ_TIMEOUT,
                HTTP_SOCKET_TIMEOUT,
                HTTP_MAX_CONNECTIONS,
                HTTP_MAX_ENDPOINT_CONNECTIONS
            )
        );
        // Standard TLS connection settings for outgoing connections to get JWT issuer jwkset_path
        set.addAll(SSLConfigurationSettings.getRealmSettings(TYPE));
        return set;
    }

    // JWT issuer settings
    public static final Setting.AffixSetting<String> ALLOWED_ISSUER = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "allowed_issuer",
        key -> Setting.simpleString(key, value -> verifyNonNullNotEmpty(key, value, null), Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<TimeValue> ALLOWED_CLOCK_SKEW = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "allowed_clock_skew",
        key -> Setting.timeSetting(key, DEFAULT_ALLOWED_CLOCK_SKEW, Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<List<String>> ALLOWED_SIGNATURE_ALGORITHMS = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "allowed_signature_algorithms",
        key -> Setting.listSetting(
            key,
            DEFAULT_ALLOWED_SIGNATURE_ALGORITHMS,
            Function.identity(),
            values -> verifyNonNullNotEmpty(key, values, SUPPORTED_SIGNATURE_ALGORITHMS),
            Setting.Property.NodeScope
        )
    );

    public static final Setting.AffixSetting<String> JWKSET_PATH = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "jwkset_path",
        key -> Setting.simpleString(key, value -> verifyNonNullNotEmpty(key, value, null), Setting.Property.NodeScope)
    );
    // Note: <allowed_issuer>.issuer_hmac_key not defined here. It goes in the Elasticsearch keystore setting.

    // JWT audience settings

    public static final Setting.AffixSetting<List<String>> ALLOWED_AUDIENCES = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "allowed_audiences",
        key -> Setting.listSetting(
            key,
            DEFAULT_ALLOWED_AUDIENCES,
            Function.identity(),
            values -> verifyNonNullNotEmpty(key, values, null),
            Setting.Property.NodeScope
        )
    );

    // JWT end-user settings

    // Note: ClaimSetting is a wrapper for two individual settings: getClaim(), getPattern()
    public static final ClaimSetting CLAIMS_PRINCIPAL = new ClaimSetting(TYPE, "principal");
    public static final ClaimSetting CLAIMS_GROUPS = new ClaimSetting(TYPE, "groups");

    public static final Setting.AffixSetting<Boolean> POPULATE_USER_METADATA = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "populate_user_metadata",
        key -> Setting.boolSetting(key, DEFAULT_POPULATE_USER_METADATA, Setting.Property.NodeScope)
    );

    // Client TLS settings for incoming connections (subset of SSLConfigurationSettings)

    public static final Setting.AffixSetting<String> CLIENT_AUTHENTICATION_TYPE = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "client_authentication.type",
        key -> Setting.simpleString(key, DEFAULT_CLIENT_AUTHENTICATION_TYPE, value -> {
            if (SUPPORTED_CLIENT_AUTHENTICATION_TYPE.contains(value) == false) {
                throw new IllegalArgumentException(
                    "Invalid value [" + value + "] for [" + key + "]. Allowed values are " + SUPPORTED_CLIENT_AUTHENTICATION_TYPE + "}]."
                );
            }
        }, Setting.Property.NodeScope)
    );

    // Individual Cache settings

    public static final Setting.AffixSetting<TimeValue> CACHE_TTL = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "cache.ttl",
        key -> Setting.timeSetting(key, DEFAULT_CACHE_TTL, Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<Integer> CACHE_MAX_USERS = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "cache.max_users",
        key -> Setting.intSetting(key, DEFAULT_CACHE_MAX_USERS, MIN_CACHE_MAX_USERS, Setting.Property.NodeScope)
    );

    // Individual outgoing HTTP settings

    public static final Setting.AffixSetting<TimeValue> HTTP_CONNECT_TIMEOUT = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "http.connect_timeout",
        key -> Setting.timeSetting(key, DEFAULT_HTTP_CONNECT_TIMEOUT, Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<TimeValue> HTTP_CONNECTION_READ_TIMEOUT = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "http.connection_read_timeout",
        key -> Setting.timeSetting(key, DEFAULT_HTTP_CONNECTION_READ_TIMEOUT, Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<TimeValue> HTTP_SOCKET_TIMEOUT = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "http.socket_timeout",
        key -> Setting.timeSetting(key, DEFAULT_HTTP_SOCKET_TIMEOUT, Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<Integer> HTTP_MAX_CONNECTIONS = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "http.max_connections",
        key -> Setting.intSetting(key, DEFAULT_HTTP_MAX_CONNECTIONS, MIN_HTTP_MAX_CONNECTIONS, Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<Integer> HTTP_MAX_ENDPOINT_CONNECTIONS = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "http.max_endpoint_connections",
        key -> Setting.intSetting(key, DEFAULT_HTTP_MAX_ENDPOINT_CONNECTIONS, MIN_HTTP_MAX_ENDPOINT_CONNECTIONS, Setting.Property.NodeScope)
    );

    private static void verifyNonNullNotEmpty(final String key, final String value, final List<String> allowedValues) {
        if ((value == null) || (value.isEmpty())) {
            throw new IllegalArgumentException("Invalid null or empty value for [" + key + "].");
        }
        if (allowedValues != null) {
            if (allowedValues.contains(value) == false) {
                throw new IllegalArgumentException(
                    "Invalid value [" + value + "] for [" + key + "]. Allowed values are " + allowedValues + "}]."
                );
            }
        }
    }

    private static void verifyNonNullNotEmpty(final String key, final List<String> values, final List<String> allowedValues) {
        if ((values == null) || (values.isEmpty())) {
            if (allowedValues == null) {
                throw new IllegalArgumentException("Invalid null or empty value for [" + key + "].");
            } else {
                throw new IllegalArgumentException(
                    "Invalid null or empty list for [" + key + "]. Allowed values are " + allowedValues + "}]."
                );
            }
        }
        for (final String value : values) {
            verifyNonNullNotEmpty(key, value, allowedValues);
        }
    }
}
