/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.oidc;


import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationResponse;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.Arrays;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.hamcrest.Matchers.equalTo;

public class OpenIdConnectRealmTests extends ESTestCase {

    private static final String REALM_NAME = "oidc1-realm";
    private Settings globalSettings;
    private Environment env;
    private ThreadContext threadContext;

    @Before
    public void setupEnv() {
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        env = TestEnvironment.newEnvironment(globalSettings);
        threadContext = new ThreadContext(globalSettings);
    }

    public void testIncorrectResponseTypeThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_NAME), "the op")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_URL), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "hybrid");
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()));
        });
        assertThat(exception.getMessage(), Matchers.containsString("[xpack.security.authc.realms.oidc.oidc1-realm.rp.response_type]." +
            " Allowed values are [code, id_token]"));
    }

    public void testMissingAuthorizationEndpointThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_NAME), "the op")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_URL), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        SettingsException exception = expectThrows(SettingsException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()));
        });
        assertThat(exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT)));
    }

    public void testInvalidAuthorizationEndpointThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "this is not a URI")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_URL), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_NAME), "the op")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()));
        });
        assertThat(exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT)));
    }

    public void testMissingTokenEndpointThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_URL), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_NAME), "the op")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        SettingsException exception = expectThrows(SettingsException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()));
        });
        assertThat(exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT)));
    }

    public void testInvalidTokenEndpointThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_URL), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "This is not a uri")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_NAME), "the op")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()));
        });
        assertThat(exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT)));
    }

    public void testMissingJwksUrlThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_NAME), "the op")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()));
        });
        assertThat(exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_URL)));
    }

    public void testInvalidJwksUrlThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_URL), "this is not a url")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "This is not a uri")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_NAME), "the op")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()));
        });
        assertThat(exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_URL)));
    }

    public void testMissingIssuerThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_NAME), "the op")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_URL), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        SettingsException exception = expectThrows(SettingsException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()));
        });
        assertThat(exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER)));
    }

    public void testMissingNameTypeThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_URL), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        SettingsException exception = expectThrows(SettingsException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()));
        });
        assertThat(exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_NAME)));
    }

    public void testMissingRedirectUriThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_URL), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_NAME), "the op")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        SettingsException exception = expectThrows(SettingsException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()));
        });
        assertThat(exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI)));
    }

    public void testMissingClientIdThrowsError() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_NAME), "the op")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_URL), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        SettingsException exception = expectThrows(SettingsException.class, () -> {
            new OpenIdConnectRealm(buildConfig(settingsBuilder.build()));
        });
        assertThat(exception.getMessage(),
            Matchers.containsString(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID)));
    }

    public void testBuilidingAuthenticationRequest() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_NAME), "the op")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_URL), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com/cb")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code")
            .putList(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REQUESTED_SCOPES),
                Arrays.asList("openid", "scope1", "scope2"));
        final OpenIdConnectRealm realm = new OpenIdConnectRealm(buildConfig(settingsBuilder.build()));
        final OpenIdConnectPrepareAuthenticationResponse response = realm.buildAuthenticationRequestUri();
        final String state = response.getState();
        final String nonce = response.getNonce();
        assertThat(response.getAuthenticationRequestUrl(),
            equalTo("https://op.example.com/login?scope=openid+scope1+scope2&response_type=code" +
                "&redirect_uri=https%3A%2F%2Frp.my.com%2Fcb&state=" + state + "&nonce=" + nonce + "&client_id=rp-my"));
    }


    public void testBuilidingAuthenticationRequestWithDefaultScope() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_NAME), "the op")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_URL), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com/cb")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code");
        final OpenIdConnectRealm realm = new OpenIdConnectRealm(buildConfig(settingsBuilder.build()));
        final OpenIdConnectPrepareAuthenticationResponse response = realm.buildAuthenticationRequestUri();
        final String state = response.getState();
        final String nonce = response.getNonce();
        assertThat(response.getAuthenticationRequestUrl(), equalTo("https://op.example.com/login?scope=openid&response_type=code" +
            "&redirect_uri=https%3A%2F%2Frp.my.com%2Fcb&state=" + state + "&nonce=" + nonce + "&client_id=rp-my"));
    }

    private RealmConfig buildConfig(Settings realmSettings) {
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put(realmSettings).build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        return new RealmConfig(new RealmConfig.RealmIdentifier("oidc", REALM_NAME), settings, env, threadContext);
    }
}
