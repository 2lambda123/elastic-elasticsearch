/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.oidc;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;


public class OpenIdConnectRealmSettings {

    private OpenIdConnectRealmSettings() {
    }

    private static final List<String> SUPPORTED_SIGNATURE_ALGORITHMS = Collections.unmodifiableList(
        Arrays.asList("HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "PS256", "PS384", "PS512"));
    private static final List<String> RESPONSE_TYPES = Arrays.asList("code", "id_token", "id_token token");
    public static final String TYPE = "oidc";

    public static final Setting.AffixSetting<String> RP_CLIENT_ID
        = RealmSettings.simpleString(TYPE, "rp.client_id", Setting.Property.NodeScope);
    public static final Setting.AffixSetting<SecureString> RP_CLIENT_SECRET
        = RealmSettings.secureString(TYPE, "rp.client_secret");
    public static final Setting.AffixSetting<String> RP_REDIRECT_URI
        = Setting.affixKeySetting(RealmSettings.realmSettingPrefix(TYPE), "rp.redirect_uri",
        key -> Setting.simpleString(key, v -> {
            try {
                new URI(v);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid value [" + v + "] for [" + key + "]. Not a valid URI.", e);
            }
        }, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<String> RP_POST_LOGOUT_REDIRECT_URI
        = Setting.affixKeySetting(RealmSettings.realmSettingPrefix(TYPE), "rp.post_logout_redirect_uri",
        key -> Setting.simpleString(key, v -> {
            try {
                new URI(v);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid value [" + v + "] for [" + key + "]. Not a valid URI.", e);
            }
        }, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<String> RP_RESPONSE_TYPE
        = Setting.affixKeySetting(RealmSettings.realmSettingPrefix(TYPE), "rp.response_type",
        key -> Setting.simpleString(key, v -> {
            if (RESPONSE_TYPES.contains(v) == false) {
                throw new IllegalArgumentException(
                    "Invalid value [" + v + "] for [" + key + "]. Allowed values are " + RESPONSE_TYPES + "");
            }
        }, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<String> RP_SIGNATURE_ALGORITHM
        = Setting.affixKeySetting(RealmSettings.realmSettingPrefix(TYPE), "rp.signature_algorithm",
        key -> new Setting<>(key, "RS256", Function.identity(), v -> {
            if (SUPPORTED_SIGNATURE_ALGORITHMS.contains(v) == false) {
                throw new IllegalArgumentException(
                    "Invalid value [" + v + "] for [" + key + "]. Allowed values are " + SUPPORTED_SIGNATURE_ALGORITHMS + "}]");
            }
        }, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<List<String>> RP_REQUESTED_SCOPES = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE), "rp.requested_scopes",
        key -> Setting.listSetting(key, Collections.singletonList("openid"), Function.identity(), Setting.Property.NodeScope));

    public static final Setting.AffixSetting<String> OP_AUTHORIZATION_ENDPOINT
        = Setting.affixKeySetting(RealmSettings.realmSettingPrefix(TYPE), "op.authorization_endpoint",
        key -> Setting.simpleString(key, v -> {
            try {
                new URI(v);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid value [" + v + "] for [" + key + "]. Not a valid URI.", e);
            }
        }, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<String> OP_TOKEN_ENDPOINT
        = Setting.affixKeySetting(RealmSettings.realmSettingPrefix(TYPE), "op.token_endpoint",
        key -> Setting.simpleString(key, v -> {
            try {
                new URI(v);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid value [" + v + "] for [" + key + "]. Not a valid URI.", e);
            }
        }, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<String> OP_USERINFO_ENDPOINT
        = Setting.affixKeySetting(RealmSettings.realmSettingPrefix(TYPE), "op.userinfo_endpoint",
        key -> Setting.simpleString(key, v -> {
            try {
                new URI(v);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid value [" + v + "] for [" + key + "]. Not a valid URI.", e);
            }
        }, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<String> OP_ENDSESSION_ENDPOINT
        = Setting.affixKeySetting(RealmSettings.realmSettingPrefix(TYPE), "op.endsession_endpoint",
        key -> Setting.simpleString(key, v -> {
            try {
                new URI(v);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid value [" + v + "] for [" + key + "]. Not a valid URI.", e);
            }
        }, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<String> OP_ISSUER
        = RealmSettings.simpleString(TYPE, "op.issuer", Setting.Property.NodeScope);
    public static final Setting.AffixSetting<String> OP_JWKSET_PATH
        = RealmSettings.simpleString(TYPE, "op.jwkset_path", Setting.Property.NodeScope);

    public static final Setting.AffixSetting<TimeValue> ALLOWED_CLOCK_SKEW
        = Setting.affixKeySetting(RealmSettings.realmSettingPrefix(TYPE), "allowed_clock_skew",
        key -> Setting.timeSetting(key, TimeValue.timeValueSeconds(60), Setting.Property.NodeScope));
    public static final Setting.AffixSetting<Boolean> POPULATE_USER_METADATA = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE), "populate_user_metadata",
        key -> Setting.boolSetting(key, true, Setting.Property.NodeScope));
    private static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(5);
    public static final Setting.AffixSetting<TimeValue> HTTP_CONNECT_TIMEOUT
        = Setting.affixKeySetting(RealmSettings.realmSettingPrefix(TYPE), "http.connect_timeout",
        key -> Setting.timeSetting(key, DEFAULT_TIMEOUT, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<TimeValue> HTTP_CONNECTION_READ_TIMEOUT
        = Setting.affixKeySetting(RealmSettings.realmSettingPrefix(TYPE), "http.connection_read_timeout",
        key -> Setting.timeSetting(key, DEFAULT_TIMEOUT, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<TimeValue> HTTP_SOCKET_TIMEOUT
        = Setting.affixKeySetting(RealmSettings.realmSettingPrefix(TYPE), "http.socket_timeout",
        key -> Setting.timeSetting(key, DEFAULT_TIMEOUT, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<Integer> HTTP_MAX_CONNECTIONS
        = Setting.affixKeySetting(RealmSettings.realmSettingPrefix(TYPE), "http.max_connections",
        key -> Setting.intSetting(key, 200, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<Integer> HTTP_MAX_ENDPOINT_CONNECTIONS
        = Setting.affixKeySetting(RealmSettings.realmSettingPrefix(TYPE), "http.max_endpoint_connections",
        key -> Setting.intSetting(key, 200, Setting.Property.NodeScope));

    public static final ClaimSetting PRINCIPAL_CLAIM = new ClaimSetting("principal");
    public static final ClaimSetting GROUPS_CLAIM = new ClaimSetting("groups");
    public static final ClaimSetting NAME_CLAIM = new ClaimSetting("name");
    public static final ClaimSetting DN_CLAIM = new ClaimSetting("dn");
    public static final ClaimSetting MAIL_CLAIM = new ClaimSetting("mail");

    public static Set<Setting.AffixSetting<?>> getSettings() {
        final Set<Setting.AffixSetting<?>> set = Sets.newHashSet(
            RP_CLIENT_ID, RP_REDIRECT_URI, RP_RESPONSE_TYPE, RP_REQUESTED_SCOPES, RP_CLIENT_SECRET, RP_SIGNATURE_ALGORITHM,
            RP_POST_LOGOUT_REDIRECT_URI, OP_AUTHORIZATION_ENDPOINT, OP_TOKEN_ENDPOINT, OP_USERINFO_ENDPOINT,
            OP_ENDSESSION_ENDPOINT, OP_ISSUER, OP_JWKSET_PATH, HTTP_CONNECT_TIMEOUT, HTTP_CONNECTION_READ_TIMEOUT, HTTP_SOCKET_TIMEOUT,
            HTTP_MAX_CONNECTIONS, HTTP_MAX_ENDPOINT_CONNECTIONS, ALLOWED_CLOCK_SKEW);
        set.addAll(DelegatedAuthorizationSettings.getSettings(TYPE));
        set.addAll(RealmSettings.getStandardSettings(TYPE));
        set.addAll(SSLConfigurationSettings.getRealmSettings(TYPE));
        set.addAll(PRINCIPAL_CLAIM.settings());
        set.addAll(GROUPS_CLAIM.settings());
        set.addAll(DN_CLAIM.settings());
        set.addAll(NAME_CLAIM.settings());
        set.addAll(MAIL_CLAIM.settings());
        return set;
    }

    /**
     * The OIDC realm offers a number of settings that rely on claim values that are populated by the OP in the ID Token or the User Info
     * response.
     * Each claim has 2 settings:
     * <ul>
     * <li>The name of the OpenID Connect claim to use</li>
     * <li>An optional java pattern (regex) to apply to that claim value in order to extract the substring that should be used.</li>
     * </ul>
     * For example, the Elasticsearch User Principal could be configured to come from the OpenID Connect standard claim "email",
     * and extract only the local-part of the user's email address (i.e. the name before the '@').
     * This class encapsulates those 2 settings.
     */
    public static final class ClaimSetting {
        public static final String CLAIMS_PREFIX = "claims.";
        public static final String CLAIM_PATTERNS_PREFIX = "claim_patterns.";

        private final Setting.AffixSetting<String> claim;
        private final Setting.AffixSetting<String> pattern;

        public ClaimSetting(String name) {
            claim = RealmSettings.simpleString(TYPE, CLAIMS_PREFIX + name, Setting.Property.NodeScope);
            pattern = RealmSettings.simpleString(TYPE, CLAIM_PATTERNS_PREFIX + name, Setting.Property.NodeScope);
        }

        public Collection<Setting.AffixSetting<?>> settings() {
            return Arrays.asList(getClaim(), getPattern());
        }

        public String name(RealmConfig config) {
            return getClaim().getConcreteSettingForNamespace(config.name()).getKey();
        }

        public Setting.AffixSetting<String> getClaim() {
            return claim;
        }

        public Setting.AffixSetting<String> getPattern() {
            return pattern;
        }
    }
}
