/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc;

import com.google.common.collect.Sets;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.ApiKey;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.GetApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ApiKeyIntegTests extends SecurityIntegTestCase {
    private static final long DELETE_INTERVAL_MILLIS = 100L;

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true)
            .put(ApiKeyService.DELETE_INTERVAL.getKey(), TimeValue.timeValueMillis(DELETE_INTERVAL_MILLIS))
            .put(ApiKeyService.DELETE_TIMEOUT.getKey(), TimeValue.timeValueSeconds(5L))
            .build();
    }

    @Before
    public void waitForSecurityIndexWritable() throws Exception {
        assertSecurityIndexActive();
    }

    @After
    public void wipeSecurityIndex() throws InterruptedException {
        // get the api key service and wait until api key expiration is not in progress!
        awaitApiKeysRemoverCompletion();
        deleteSecurityIndex();
    }

    @Override
    public String configRoles() {
        return super.configRoles() + "\n" +
                "manage_api_key_role:\n" +
                "  cluster: [\"manage_api_key\"]\n" +
                "manage_own_api_key_role:\n" +
                "  global: { \"api_keys\":{\"manage\":{\"get\":true,\"invalidate\":true,\"create\":true," +
                                                        "\"users\":[],\"realms\":[]}} }\n" +
                "only_create_api_key_role:\n" +
                "  global: { \"api_keys\":{\"manage\":{\"get\":false,\"invalidate\":false,\"create\":true," +
                                                        "\"users\":[],\"realms\":[]}} }\n" +
                "only_create_get_own_api_key_role:\n" +
                "  global: { \"api_keys\":{\"manage\":{\"get\":true,\"invalidate\":false,\"create\":true," +
                                                        "\"users\":[],\"realms\":[]}} }\n" +
                "no_manage_api_key_role:\n" +
                "  indices:\n" +
                "     - names: '*'\n" +
                "       privileges:\n" +
                "         - all\n";
    }

    @Override
    public String configUsers() {
        final String usersPasswdHashed = new String(
                getFastStoredHashAlgoForTests().hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
        return super.configUsers() +
            "user_with_manage_api_key_role:" + usersPasswdHashed + "\n" +
            "user_with_only_create_api_key_role:" + usersPasswdHashed + "\n" +
            "user_with_only_create_get_api_key_role:" + usersPasswdHashed + "\n" +
            "user_with_owner_manage_api_key_role:" + usersPasswdHashed + "\n" +
            "user_with_no_manage_api_key_role:" + usersPasswdHashed + "\n";
    }

    @Override
    public String configUsersRoles() {
        return super.configUsersRoles() +
                "manage_api_key_role:user_with_manage_api_key_role\n" +
                "only_create_api_key_role:user_with_only_create_api_key_role\n" +
                "only_create_get_own_api_key_role:user_with_only_create_get_api_key_role\n" +
                "manage_own_api_key_role:user_with_owner_manage_api_key_role\n" +
                "no_manage_api_key_role:user_with_no_manage_api_key_role";
    }

    private void awaitApiKeysRemoverCompletion() throws InterruptedException {
        for (ApiKeyService apiKeyService : internalCluster().getInstances(ApiKeyService.class)) {
            final boolean done = awaitBusy(() -> apiKeyService.isExpirationInProgress() == false);
            assertTrue(done);
        }
    }

    public void testCreateApiKey() {
        final Instant start = Instant.now();
        final RoleDescriptor descriptor = new RoleDescriptor("role", new String[] { "monitor" }, null, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        final CreateApiKeyResponse response = securityClient.prepareCreateApiKey()
            .setName("test key")
            .setExpiration(TimeValue.timeValueHours(TimeUnit.DAYS.toHours(7L)))
            .setRoleDescriptors(Collections.singletonList(descriptor))
            .get();

        assertEquals("test key", response.getName());
        assertNotNull(response.getId());
        assertNotNull(response.getKey());
        Instant expiration = response.getExpiration();
        final long daysBetween = ChronoUnit.DAYS.between(start, expiration);
        assertThat(daysBetween, is(7L));

        // create simple api key
        final CreateApiKeyResponse simple = securityClient.prepareCreateApiKey().setName("simple").get();
        assertEquals("simple", simple.getName());
        assertNotNull(simple.getId());
        assertNotNull(simple.getKey());
        assertThat(simple.getId(), not(containsString(new String(simple.getKey().getChars()))));
        assertNull(simple.getExpiration());

        // use the first ApiKey for authorized action
        final String base64ApiKeyKeyValue = Base64.getEncoder().encodeToString(
            (response.getId() + ":" + response.getKey().toString()).getBytes(StandardCharsets.UTF_8));
        ClusterHealthResponse healthResponse = client()
            .filterWithHeader(Collections.singletonMap("Authorization", "ApiKey " + base64ApiKeyKeyValue))
            .admin()
            .cluster()
            .prepareHealth()
            .get();
        assertFalse(healthResponse.isTimedOut());

        // use the first ApiKey for an unauthorized action
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () ->
            client().filterWithHeader(Collections.singletonMap("Authorization", "ApiKey " + base64ApiKeyKeyValue))
                .admin()
                .cluster()
                .prepareUpdateSettings().setTransientSettings(Settings.builder().put(IPFilter.IP_FILTER_ENABLED_SETTING.getKey(), true))
                .get());
        assertThat(e.getMessage(), containsString("unauthorized"));
        assertThat(e.status(), is(RestStatus.FORBIDDEN));
    }

    public void testCreateApiKeyFailsWhenApiKeyWithSameNameAlreadyExists() throws InterruptedException, ExecutionException {
        String keyName = randomAlphaOfLength(5);
        List<CreateApiKeyResponse> responses = new ArrayList<>();
        {
            final RoleDescriptor descriptor = new RoleDescriptor("role", new String[] { "monitor" }, null, null);
            Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                    .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
            SecurityClient securityClient = new SecurityClient(client);
            final CreateApiKeyResponse response = securityClient.prepareCreateApiKey().setName(keyName).setExpiration(null)
                    .setRoleDescriptors(Collections.singletonList(descriptor)).get();
            assertNotNull(response.getId());
            assertNotNull(response.getKey());
            responses.add(response);
        }

        final RoleDescriptor descriptor = new RoleDescriptor("role", new String[] { "monitor" }, null, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> securityClient.prepareCreateApiKey()
            .setName(keyName)
            .setExpiration(TimeValue.timeValueHours(TimeUnit.DAYS.toHours(7L)))
            .setRoleDescriptors(Collections.singletonList(descriptor))
            .get());
        assertThat(e.getMessage(), equalTo("Error creating api key as api key with name ["+keyName+"] already exists"));

        // Now invalidate the API key
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyName(keyName), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        verifyInvalidateResponse(1, responses, invalidateResponse);

        // try to create API key with same name, should succeed now
        CreateApiKeyResponse createResponse = securityClient.prepareCreateApiKey().setName(keyName)
                .setExpiration(TimeValue.timeValueHours(TimeUnit.DAYS.toHours(7L)))
                .setRoleDescriptors(Collections.singletonList(descriptor)).get();
        assertNotNull(createResponse.getId());
        assertNotNull(createResponse.getKey());
    }

    public void testInvalidateApiKeysForRealm() throws InterruptedException, ExecutionException {
        int noOfApiKeys = randomIntBetween(3, 5);
        List<CreateApiKeyResponse> responses = createApiKeys(noOfApiKeys, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingRealmName("file"), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        verifyInvalidateResponse(noOfApiKeys, responses, invalidateResponse);
    }

    public void testInvalidateApiKeysForUser() throws Exception {
        int noOfApiKeys = randomIntBetween(3, 5);
        List<CreateApiKeyResponse> responses = createApiKeys(noOfApiKeys, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingUserName(SecuritySettingsSource.TEST_SUPERUSER), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        verifyInvalidateResponse(noOfApiKeys, responses, invalidateResponse);
    }

    public void testInvalidateApiKeysForRealmAndUser() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(1, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingRealmAndUserName("file", SecuritySettingsSource.TEST_SUPERUSER),
                listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        verifyInvalidateResponse(1, responses, invalidateResponse);
    }

    public void testInvalidateApiKeysForApiKeyId() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(1, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyId(responses.get(0).getId()), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        verifyInvalidateResponse(1, responses, invalidateResponse);
    }

    public void testInvalidateApiKeysForApiKeyName() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(1, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyName(responses.get(0).getName()), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        verifyInvalidateResponse(1, responses, invalidateResponse);
    }

    private void verifyInvalidateResponse(int noOfApiKeys, List<CreateApiKeyResponse> responses,
                                          InvalidateApiKeyResponse invalidateResponse) {
        assertThat(invalidateResponse.getInvalidatedApiKeys().size(), equalTo(noOfApiKeys));
        assertThat(invalidateResponse.getInvalidatedApiKeys(),
                containsInAnyOrder(responses.stream().map(r -> r.getId()).collect(Collectors.toList()).toArray(Strings.EMPTY_ARRAY)));
        assertThat(invalidateResponse.getPreviouslyInvalidatedApiKeys().size(), equalTo(0));
        assertThat(invalidateResponse.getErrors().size(), equalTo(0));
    }

    public void testInvalidatedApiKeysDeletedByRemover() throws Exception {
        Client client = waitForExpiredApiKeysRemoverTriggerReadyAndGetClient().filterWithHeader(
                Collections.singletonMap("Authorization", UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));

        List<CreateApiKeyResponse> createdApiKeys = createApiKeys(2, null);

        SecurityClient securityClient = new SecurityClient(client);

        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyId(createdApiKeys.get(0).getId()), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        assertThat(invalidateResponse.getInvalidatedApiKeys().size(), equalTo(1));
        assertThat(invalidateResponse.getPreviouslyInvalidatedApiKeys().size(), equalTo(0));
        assertThat(invalidateResponse.getErrors().size(), equalTo(0));

        PlainActionFuture<GetApiKeyResponse> getApiKeyResponseListener = new PlainActionFuture<>();
        securityClient.getApiKey(GetApiKeyRequest.usingRealmName("file"), getApiKeyResponseListener);
        assertThat(getApiKeyResponseListener.get().getApiKeyInfos().length, is(2));

        client = waitForExpiredApiKeysRemoverTriggerReadyAndGetClient().filterWithHeader(
                Collections.singletonMap("Authorization", UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        securityClient = new SecurityClient(client);

        // invalidate API key to trigger remover
        listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyId(createdApiKeys.get(1).getId()), listener);
        assertThat(listener.get().getInvalidatedApiKeys().size(), is(1));

        awaitApiKeysRemoverCompletion();

        refreshSecurityIndex();

        // Verify that 1st invalidated API key is deleted whereas the next one is not
        getApiKeyResponseListener = new PlainActionFuture<>();
        securityClient.getApiKey(GetApiKeyRequest.usingRealmName("file"), getApiKeyResponseListener);
        assertThat(getApiKeyResponseListener.get().getApiKeyInfos().length, is(1));
        ApiKey apiKey = getApiKeyResponseListener.get().getApiKeyInfos()[0];
        assertThat(apiKey.getId(), is(createdApiKeys.get(1).getId()));
        assertThat(apiKey.isInvalidated(), is(true));
    }

    private Client waitForExpiredApiKeysRemoverTriggerReadyAndGetClient() throws Exception {
        String nodeWithMostRecentRun = null;
        long apiKeyLastTrigger = -1L;
        for (String nodeName : internalCluster().getNodeNames()) {
            ApiKeyService apiKeyService = internalCluster().getInstance(ApiKeyService.class, nodeName);
            if (apiKeyService != null) {
                if (apiKeyService.lastTimeWhenApiKeysRemoverWasTriggered() > apiKeyLastTrigger) {
                    nodeWithMostRecentRun = nodeName;
                    apiKeyLastTrigger = apiKeyService.lastTimeWhenApiKeysRemoverWasTriggered();
               }
            }
        }
        final ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, nodeWithMostRecentRun);
        final long lastRunTime = apiKeyLastTrigger;
        assertBusy(() -> {
            assertThat(threadPool.relativeTimeInMillis() - lastRunTime, greaterThan(DELETE_INTERVAL_MILLIS));
        });
        return internalCluster().client(nodeWithMostRecentRun);
    }

    public void testExpiredApiKeysBehaviorWhenKeysExpired1WeekBeforeAnd1DayBefore() throws Exception {
        Client client = waitForExpiredApiKeysRemoverTriggerReadyAndGetClient().filterWithHeader(
                Collections.singletonMap("Authorization", UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));

        int noOfKeys = 4;
        List<CreateApiKeyResponse> createdApiKeys = createApiKeys(noOfKeys, null);
        Instant created = Instant.now();

        SecurityClient securityClient = new SecurityClient(client);

        PlainActionFuture<GetApiKeyResponse> getApiKeyResponseListener = new PlainActionFuture<>();
        securityClient.getApiKey(GetApiKeyRequest.usingRealmName("file"), getApiKeyResponseListener);
        assertThat(getApiKeyResponseListener.get().getApiKeyInfos().length, is(noOfKeys));

        // Expire the 1st key such that it cannot be deleted by the remover
        // hack doc to modify the expiration time to a day before
        Instant dayBefore = created.minus(1L, ChronoUnit.DAYS);
        assertTrue(Instant.now().isAfter(dayBefore));
        UpdateResponse expirationDateUpdatedResponse = client
                .prepareUpdate(SECURITY_MAIN_ALIAS, SINGLE_MAPPING_NAME, createdApiKeys.get(0).getId())
                .setDoc("expiration_time", dayBefore.toEpochMilli())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        assertThat(expirationDateUpdatedResponse.getResult(), is(DocWriteResponse.Result.UPDATED));

        // Expire the 2nd key such that it cannot be deleted by the remover
        // hack doc to modify the expiration time to the week before
        Instant weekBefore = created.minus(8L, ChronoUnit.DAYS);
        assertTrue(Instant.now().isAfter(weekBefore));
        expirationDateUpdatedResponse = client.prepareUpdate(SECURITY_MAIN_ALIAS, SINGLE_MAPPING_NAME, createdApiKeys.get(1).getId())
                .setDoc("expiration_time", weekBefore.toEpochMilli())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        assertThat(expirationDateUpdatedResponse.getResult(), is(DocWriteResponse.Result.UPDATED));

        // Invalidate to trigger the remover
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyId(createdApiKeys.get(2).getId()), listener);
        assertThat(listener.get().getInvalidatedApiKeys().size(), is(1));

        awaitApiKeysRemoverCompletion();

        refreshSecurityIndex();

        // Verify get API keys does not return expired and deleted key
        getApiKeyResponseListener = new PlainActionFuture<>();
        securityClient.getApiKey(GetApiKeyRequest.usingRealmName("file"), getApiKeyResponseListener);
        assertThat(getApiKeyResponseListener.get().getApiKeyInfos().length, is(3));

        Set<String> expectedKeyIds = Sets.newHashSet(createdApiKeys.get(0).getId(), createdApiKeys.get(2).getId(),
                createdApiKeys.get(3).getId());
        for (ApiKey apiKey : getApiKeyResponseListener.get().getApiKeyInfos()) {
            assertThat(apiKey.getId(), isIn(expectedKeyIds));
            if (apiKey.getId().equals(createdApiKeys.get(0).getId())) {
                // has been expired, not invalidated
                assertTrue(apiKey.getExpiration().isBefore(Instant.now()));
                assertThat(apiKey.isInvalidated(), is(false));
            } else if (apiKey.getId().equals(createdApiKeys.get(2).getId())) {
                // has not been expired as no expiration, but invalidated
                assertThat(apiKey.getExpiration(), is(nullValue()));
                assertThat(apiKey.isInvalidated(), is(true));
            } else if (apiKey.getId().equals(createdApiKeys.get(3).getId())) {
                // has not been expired as no expiration, not invalidated
                assertThat(apiKey.getExpiration(), is(nullValue()));
                assertThat(apiKey.isInvalidated(), is(false));
            } else {
                fail("unexpected API key " + apiKey);
            }
        }
    }

    private void refreshSecurityIndex() throws Exception {
        assertBusy(() -> {
            final RefreshResponse refreshResponse = client().admin().indices().prepareRefresh(SECURITY_MAIN_ALIAS).get();
            assertThat(refreshResponse.getFailedShards(), is(0));
        });
    }

    public void testActiveApiKeysWithNoExpirationNeverGetDeletedByRemover() throws Exception {
        List<CreateApiKeyResponse> responses = createApiKeys(2, null);

        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        // trigger expired keys remover
        securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyId(responses.get(1).getId()), listener);
        InvalidateApiKeyResponse invalidateResponse = listener.get();
        assertThat(invalidateResponse.getInvalidatedApiKeys().size(), equalTo(1));
        assertThat(invalidateResponse.getPreviouslyInvalidatedApiKeys().size(), equalTo(0));
        assertThat(invalidateResponse.getErrors().size(), equalTo(0));

        PlainActionFuture<GetApiKeyResponse> getApiKeyResponseListener = new PlainActionFuture<>();
        securityClient.getApiKey(GetApiKeyRequest.usingRealmName("file"), getApiKeyResponseListener);
        GetApiKeyResponse response = getApiKeyResponseListener.get();
        verifyGetResponse(2, responses, response, Collections.singleton(responses.get(0).getId()),
                Collections.singletonList(responses.get(1).getId()));
    }

    public void testGetApiKeysForRealm() throws InterruptedException, ExecutionException {
        int noOfApiKeys = randomIntBetween(3, 5);
        List<CreateApiKeyResponse> responses = createApiKeys(noOfApiKeys, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        boolean invalidate= randomBoolean();
        List<String> invalidatedApiKeyIds = null;
        Set<String> expectedValidKeyIds = null;
        if (invalidate) {
            PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
            securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyId(responses.get(0).getId()), listener);
            InvalidateApiKeyResponse invalidateResponse = listener.get();
            invalidatedApiKeyIds = invalidateResponse.getInvalidatedApiKeys();
            expectedValidKeyIds = responses.stream().filter(o -> !o.getId().equals(responses.get(0).getId())).map(o -> o.getId())
                    .collect(Collectors.toSet());
        } else {
            invalidatedApiKeyIds = Collections.emptyList();
            expectedValidKeyIds = responses.stream().map(o -> o.getId()).collect(Collectors.toSet());
        }

        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.getApiKey(GetApiKeyRequest.usingRealmName("file"), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(noOfApiKeys, responses, response,
                expectedValidKeyIds,
                invalidatedApiKeyIds);
    }

    public void testGetApiKeysForUser() throws Exception {
        int noOfApiKeys = randomIntBetween(3, 5);
        List<CreateApiKeyResponse> responses = createApiKeys(noOfApiKeys, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.getApiKey(GetApiKeyRequest.usingUserName(SecuritySettingsSource.TEST_SUPERUSER), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(noOfApiKeys, responses, response, responses.stream().map(o -> o.getId()).collect(Collectors.toSet()), null);
    }

    public void testGetApiKeysForRealmAndUser() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(1, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.getApiKey(GetApiKeyRequest.usingRealmAndUserName("file", SecuritySettingsSource.TEST_SUPERUSER),
                listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(1, responses, response, Collections.singleton(responses.get(0).getId()), null);
    }

    public void testGetApiKeysForApiKeyId() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(1, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.getApiKey(GetApiKeyRequest.usingApiKeyId(responses.get(0).getId()), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(1, responses, response, Collections.singleton(responses.get(0).getId()), null);
    }

    public void testGetApiKeysForApiKeyName() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> responses = createApiKeys(1, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        securityClient.getApiKey(GetApiKeyRequest.usingApiKeyName(responses.get(0).getName()), listener);
        GetApiKeyResponse response = listener.get();
        verifyGetResponse(1, responses, response, Collections.singleton(responses.get(0).getId()), null);
    }

    private void verifyGetResponse(int noOfApiKeys, List<CreateApiKeyResponse> responses, GetApiKeyResponse response,
                                   Set<String> validApiKeyIds,
                                   List<String> invalidatedApiKeyIds) {
        assertThat(response.getApiKeyInfos().length, equalTo(noOfApiKeys));
        List<String> expectedIds = responses.stream().filter(o -> validApiKeyIds.contains(o.getId())).map(o -> o.getId())
                .collect(Collectors.toList());
        List<String> actualIds = Arrays.stream(response.getApiKeyInfos()).filter(o -> o.isInvalidated() == false).map(o -> o.getId())
                .collect(Collectors.toList());
        assertThat(actualIds, containsInAnyOrder(expectedIds.toArray(Strings.EMPTY_ARRAY)));
        List<String> expectedNames = responses.stream().filter(o -> validApiKeyIds.contains(o.getId())).map(o -> o.getName())
                .collect(Collectors.toList());
        List<String> actualNames = Arrays.stream(response.getApiKeyInfos()).filter(o -> o.isInvalidated() == false).map(o -> o.getName())
                .collect(Collectors.toList());
        assertThat(actualNames, containsInAnyOrder(expectedNames.toArray(Strings.EMPTY_ARRAY)));
        Set<String> expectedUsernames = (validApiKeyIds.isEmpty()) ? Collections.emptySet()
                : Collections.singleton(SecuritySettingsSource.TEST_SUPERUSER);
        Set<String> actualUsernames = Arrays.stream(response.getApiKeyInfos()).filter(o -> o.isInvalidated() == false)
                .map(o -> o.getUsername()).collect(Collectors.toSet());
        assertThat(actualUsernames, containsInAnyOrder(expectedUsernames.toArray(Strings.EMPTY_ARRAY)));
        if (invalidatedApiKeyIds != null) {
            List<String> actualInvalidatedApiKeyIds = Arrays.stream(response.getApiKeyInfos()).filter(o -> o.isInvalidated())
                    .map(o -> o.getId()).collect(Collectors.toList());
            assertThat(invalidatedApiKeyIds, containsInAnyOrder(actualInvalidatedApiKeyIds.toArray(Strings.EMPTY_ARRAY)));
        }

    }

    public void testApiKeyWithMinimalRoleCanGetApiKeyInformation() {
        final RoleDescriptor descriptor = new RoleDescriptor("role", new String[] { "monitor" }, null, null);
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        final CreateApiKeyResponse response = securityClient.prepareCreateApiKey()
            .setName("test key")
            .setExpiration(TimeValue.timeValueHours(TimeUnit.DAYS.toHours(7L)))
            .setRoleDescriptors(Collections.singletonList(descriptor))
            .get();

        assertEquals("test key", response.getName());
        assertNotNull(response.getId());
        assertNotNull(response.getKey());

        // use the first ApiKey for authorized action
        final String base64ApiKeyKeyValue = Base64.getEncoder().encodeToString(
            (response.getId() + ":" + response.getKey().toString()).getBytes(StandardCharsets.UTF_8));
        client = client().filterWithHeader(Collections.singletonMap("Authorization", "ApiKey " + base64ApiKeyKeyValue));
        securityClient = new SecurityClient(client);
        PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
        GetApiKeyRequest request = GetApiKeyRequest.usingApiKeyId(response.getId());
        securityClient.getApiKey(request, listener);
        GetApiKeyResponse apiKeyResponse = listener.actionGet();
        assertThat(apiKeyResponse.getApiKeyInfos().length, is(1));
        assertThat(apiKeyResponse.getApiKeyInfos()[0].getId(), is(response.getId()));
        assertThat(apiKeyResponse.getApiKeyInfos()[0].getName(), is(response.getName()));
        assertThat(apiKeyResponse.getApiKeyInfos()[0].getExpiration(), is(response.getExpiration()));

        // request where API key id is missing must fail
        request = GetApiKeyRequest.usingApiKeyName("test key");
        PlainActionFuture<GetApiKeyResponse> failureListener = new PlainActionFuture<>();
        securityClient.getApiKey(request, failureListener);
        ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class, () -> failureListener.actionGet());
        assertErrorMessage(ese, "cluster:admin/xpack/security/api_key/get", SecuritySettingsSource.TEST_SUPERUSER);
    }

    public void testCreateApiKeyAuthorization() {
        // user_with_manage_api_key_role should be able to create API key
        List<CreateApiKeyResponse> responses = createApiKeys("user_with_manage_api_key_role", 1, null);
        assertThat(responses.get(0).getKey(), is(notNullValue()));

        // user_with_only_create_api_key_role should be able to create API key
        responses = createApiKeys("user_with_only_create_api_key_role", 1, null);
        assertThat(responses.get(0).getKey(), is(notNullValue()));

        // user_with_no_manage_api_key_role should not be able to create API key
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                .basicAuthHeaderValue("user_with_no_manage_api_key_role", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        RoleDescriptor roleDescriptor = new RoleDescriptor("role", new String[] { "monitor" }, null, null);
        ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class,
                () -> securityClient.prepareCreateApiKey().setName("test-key-" + randomAlphaOfLengthBetween(5, 9))
                        .setRoleDescriptors(Collections.singletonList(roleDescriptor)).get());
        assertErrorMessage(ese, "cluster:admin/xpack/security/api_key/create", "user_with_no_manage_api_key_role");
    }

    public void testGetApiKeyAuthorization() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> userWithManageApiKeyRoleApiKeys = createApiKeys("user_with_manage_api_key_role", 2, null);
        List<CreateApiKeyResponse> userWithOwnerManageApiKeyRoleApiKeys = createApiKeys("user_with_owner_manage_api_key_role", 2, null);
        List<CreateApiKeyResponse> userWithCreateApiKeyRoleApiKeys = createApiKeys("user_with_only_create_api_key_role", 1, null);

        // user_with_manage_api_key_role should be able to get any user's API Key
        {
            final Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                    .basicAuthHeaderValue("user_with_manage_api_key_role", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
            final SecurityClient securityClient = new SecurityClient(client);
            PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
            securityClient.getApiKey(GetApiKeyRequest.usingApiKeyId(userWithManageApiKeyRoleApiKeys.get(0).getId()), listener);
            GetApiKeyResponse response = listener.actionGet();
            assertThat(response.getApiKeyInfos().length, is(1));
            assertThat(response.getApiKeyInfos()[0].getId(), is(userWithManageApiKeyRoleApiKeys.get(0).getId()));

            listener = new PlainActionFuture<>();
            securityClient.getApiKey(GetApiKeyRequest.usingApiKeyId(userWithOwnerManageApiKeyRoleApiKeys.get(0).getId()), listener);
            response = listener.actionGet();
            assertThat(response.getApiKeyInfos().length, is(1));
            assertThat(response.getApiKeyInfos()[0].getId(), is(userWithOwnerManageApiKeyRoleApiKeys.get(0).getId()));
        }

        // user_with_owner_manage_api_key_role should be able to get its own API key but not any other
        // user's API key
        {
            final Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                    .basicAuthHeaderValue("user_with_owner_manage_api_key_role", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
            final SecurityClient securityClient = new SecurityClient(client);
            PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
            GetApiKeyRequest getApiKeyRequest = new GetApiKeyRequest("file", "user_with_owner_manage_api_key_role", null, null);
            securityClient.getApiKey(getApiKeyRequest, listener);
            GetApiKeyResponse response = listener.actionGet();
            assertThat(response.getApiKeyInfos().length, is(2));

            final PlainActionFuture<GetApiKeyResponse> getApiKeyOfOtherUserListener = new PlainActionFuture<>();
            securityClient.getApiKey(GetApiKeyRequest.usingApiKeyId(userWithManageApiKeyRoleApiKeys.get(0).getId()),
                    getApiKeyOfOtherUserListener);
            final ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class,
                    () -> getApiKeyOfOtherUserListener.actionGet());
            assertErrorMessage(ese, "cluster:admin/xpack/security/api_key/get", "user_with_owner_manage_api_key_role");
        }

        // user_with_only_create_api_key_role should not be allowed to get it's own API key but not any other user's API key
        {
            final Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                    .basicAuthHeaderValue("user_with_only_create_api_key_role", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
            final SecurityClient securityClient = new SecurityClient(client);
            PlainActionFuture<GetApiKeyResponse> listener = new PlainActionFuture<>();
            GetApiKeyRequest getApiKeyRequest = new GetApiKeyRequest(null, "user_with_only_create_api_key_role",
                    userWithCreateApiKeyRoleApiKeys.get(0).getId(), userWithCreateApiKeyRoleApiKeys.get(0).getName());
            securityClient.getApiKey(getApiKeyRequest, listener);
            ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class,
                    () -> listener.actionGet());
            assertErrorMessage(ese, "cluster:admin/xpack/security/api_key/get", "user_with_only_create_api_key_role");

            final PlainActionFuture<GetApiKeyResponse> getApiKeyOfOtherUserListener = new PlainActionFuture<>();
            securityClient.getApiKey(GetApiKeyRequest.usingApiKeyId(userWithManageApiKeyRoleApiKeys.get(0).getId()),
                    getApiKeyOfOtherUserListener);
            ese = expectThrows(ElasticsearchSecurityException.class,
                    () -> getApiKeyOfOtherUserListener.actionGet());
            assertErrorMessage(ese, "cluster:admin/xpack/security/api_key/get", "user_with_only_create_api_key_role");
        }
    }

    public void testInvalidateApiKeyAuthorization() throws InterruptedException, ExecutionException {
        List<CreateApiKeyResponse> userWithManageApiKeyRoleApiKeys = createApiKeys("user_with_manage_api_key_role", 2, null);
        List<CreateApiKeyResponse> userWithOwnerManageApiKeyRoleApiKeys = createApiKeys("user_with_owner_manage_api_key_role", 2, null);
        List<CreateApiKeyResponse> userWithCreateApiKeyRoleApiKeys = createApiKeys("user_with_only_create_api_key_role", 1, null);

        // user_with_manage_api_key_role should be able to invalidate any user's API Key
        InvalidateApiKeyResponse invalidateApiKeyResponse = invalidateApiKey("user_with_manage_api_key_role", null, null,
                userWithManageApiKeyRoleApiKeys.get(0).getName(), null);
        verifyInvalidateResponse(1, Collections.singletonList(userWithManageApiKeyRoleApiKeys.get(0)), invalidateApiKeyResponse);
        invalidateApiKeyResponse = invalidateApiKey("user_with_manage_api_key_role", null, null,
                userWithOwnerManageApiKeyRoleApiKeys.get(0).getName(), null);
        verifyInvalidateResponse(1, Collections.singletonList(userWithOwnerManageApiKeyRoleApiKeys.get(0)), invalidateApiKeyResponse);

        // user_with_owner_manage_api_key_role should be able to invalidate its own API key but not any other user's API key
        {
            final Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                    .basicAuthHeaderValue("user_with_owner_manage_api_key_role", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
            final SecurityClient securityClient = new SecurityClient(client);
            final PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
            InvalidateApiKeyRequest invalidateApiKeyRequest = new InvalidateApiKeyRequest("file", "user_with_owner_manage_api_key_role", null,
                    userWithOwnerManageApiKeyRoleApiKeys.get(1).getName());
            securityClient.invalidateApiKey(invalidateApiKeyRequest, listener);
            invalidateApiKeyResponse = listener.actionGet();
            verifyInvalidateResponse(1, Collections.singletonList(userWithOwnerManageApiKeyRoleApiKeys.get(1)), invalidateApiKeyResponse);

            final ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class,
                    () -> invalidateApiKey("user_with_owner_manage_api_key_role", null, "user_with_only_create_api_key_role",
                            userWithManageApiKeyRoleApiKeys.get(1).getName(), null));
            assertErrorMessage(ese, "cluster:admin/xpack/security/api_key/invalidate", "user_with_owner_manage_api_key_role");
        }

        // user_with_only_create_api_key_role should not be allowed to invalidate it's own API keys or any other users API keys
        {
            final Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                    .basicAuthHeaderValue("user_with_only_create_api_key_role", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
            final SecurityClient securityClient = new SecurityClient(client);

            ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class,
                    () -> invalidateApiKey("user_with_only_create_api_key_role", null, "user_with_only_create_api_key_role",
                            userWithManageApiKeyRoleApiKeys.get(1).getName(), null));
            assertErrorMessage(ese, "cluster:admin/xpack/security/api_key/invalidate", "user_with_only_create_api_key_role");

            final PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
            securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyName(userWithCreateApiKeyRoleApiKeys.get(0).getName()),
                    listener);
            ese = expectThrows(ElasticsearchSecurityException.class, () -> listener.actionGet());
            assertErrorMessage(ese, "cluster:admin/xpack/security/api_key/invalidate", "user_with_only_create_api_key_role");
        }
    }

    private List<CreateApiKeyResponse> createApiKeys(int noOfApiKeys, TimeValue expiration) {
        return createApiKeys(SecuritySettingsSource.TEST_SUPERUSER, noOfApiKeys, expiration);
    }

    private List<CreateApiKeyResponse> createApiKeys(String user, int noOfApiKeys, TimeValue expiration) {
        List<CreateApiKeyResponse> responses = new ArrayList<>();
        for (int i = 0; i < noOfApiKeys; i++) {
            final RoleDescriptor descriptor = new RoleDescriptor("role", new String[] { "monitor" }, null, null);
            Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                    .basicAuthHeaderValue(user, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
            SecurityClient securityClient = new SecurityClient(client);
            final CreateApiKeyResponse response = securityClient.prepareCreateApiKey()
                    .setName("test-key-" + randomAlphaOfLengthBetween(5, 9) + i).setExpiration(expiration)
                    .setRoleDescriptors(Collections.singletonList(descriptor)).get();
            assertNotNull(response.getId());
            assertNotNull(response.getKey());
            responses.add(response);
        }
        assertThat(responses.size(), is(noOfApiKeys));
        return responses;
    }

    private void assertErrorMessage(final ElasticsearchSecurityException ese, String action, String userName) {
        assertThat(ese.getMessage(), is("action [" + action + "] is unauthorized for user [" + userName + "]"));
    }

    private InvalidateApiKeyResponse invalidateApiKey(String executeActionAsUser, String realmName, String userName, String apiKeyName,
            String apiKeyId) {
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization",
                UsernamePasswordToken.basicAuthHeaderValue(executeActionAsUser, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        PlainActionFuture<InvalidateApiKeyResponse> listener = new PlainActionFuture<>();
        if (Strings.hasText(realmName) && Strings.hasText(userName)) {
            securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingRealmAndUserName(realmName, userName), listener);
        } else if (Strings.hasText(realmName)) {
            securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingRealmName(realmName), listener);
        } else if (Strings.hasText(userName)) {
            securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingUserName(userName), listener);
        } else if (Strings.hasText(apiKeyName)) {
            securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyName(apiKeyName), listener);
        } else if (Strings.hasText(apiKeyId)) {
            securityClient.invalidateApiKey(InvalidateApiKeyRequest.usingApiKeyId(apiKeyId), listener);
        }
        return listener.actionGet();
    }
}
