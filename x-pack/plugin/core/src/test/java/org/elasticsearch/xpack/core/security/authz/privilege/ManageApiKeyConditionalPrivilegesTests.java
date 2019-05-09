/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ManageApiKeyConditionalPrivilegesTests extends ESTestCase {
    private static final String CREATE_ACTION = "cluster:admin/xpack/security/api_key/create";
    private static final String GET_ACTION = "cluster:admin/xpack/security/api_key/get";
    private static final String INVALIDATE_ACTION = "cluster:admin/xpack/security/api_key/invalidate";

    private User user;
    private Authentication authentication = mock(Authentication.class);
    private Authentication.RealmRef authenticatedBy = mock(Authentication.RealmRef.class);

    @Before
    public void setup() {
        user = new User("user1");
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getName()).thenReturn("realm1");
        when(authenticatedBy.getType()).thenReturn("kerberos");
    }

    public void testManageAllPrivilege() {
        final ManageApiKeyConditionalPrivileges condPrivilege = ManageApiKeyConditionalPrivilegesBuilder.manageApiKeysUnrestricted();

        boolean accessAllowed = checkAccess(condPrivilege, CREATE_ACTION, new CreateApiKeyRequest(), authentication);
        assertThat(accessAllowed, is(true));

        accessAllowed = checkAccess(condPrivilege, GET_ACTION,
                GetApiKeyRequest.usingRealmAndUserName(randomAlphaOfLength(5), randomAlphaOfLength(5)), authentication);
        assertThat(accessAllowed, is(true));

        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION,
                InvalidateApiKeyRequest.usingRealmAndUserName(randomAlphaOfLength(5), randomAlphaOfLength(5)), authentication);
        assertThat(accessAllowed, is(true));

        // When request does not have user or realm name and conditional api key privileges for manage is used then it should be denied.
        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingApiKeyName(randomAlphaOfLength(5)), authentication);
        assertThat(accessAllowed, is(true));
        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION, InvalidateApiKeyRequest.usingApiKeyId(randomAlphaOfLength(5)),
                authentication);
        assertThat(accessAllowed, is(true));
    }

    public void testManagePrivilegeRestrictedForRealmsAndUsers() {
        final ManageApiKeyConditionalPrivileges condPrivilege = ManageApiKeyConditionalPrivilegesBuilder.builder().allowCreate().allowGet()
                .allowInvalidate().forRealms("realm1", "realm2").forUsers("user1", "user2").build();

        boolean accessAllowed = checkAccess(condPrivilege, CREATE_ACTION, new CreateApiKeyRequest(), authentication);
        assertThat(accessAllowed, is(true));

        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingRealmAndUserName("realm1", "user1"), authentication);
        assertThat(accessAllowed, is(true));

        accessAllowed = checkAccess(condPrivilege, GET_ACTION,
                GetApiKeyRequest.usingRealmAndUserName(randomAlphaOfLength(7), randomAlphaOfLength(7)), authentication);
        assertThat(accessAllowed, is(false));

        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION, InvalidateApiKeyRequest.usingRealmAndUserName("realm2", "user2"),
                authentication);
        assertThat(accessAllowed, is(true));

        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION,
                InvalidateApiKeyRequest.usingRealmAndUserName(randomAlphaOfLength(7), randomAlphaOfLength(7)), authentication);
        assertThat(accessAllowed, is(false));

    }

    public void testManagePrivilegeRestrictedReadOnlyForRealmsAndUsers() {
        final ManageApiKeyConditionalPrivileges condPrivilege = ManageApiKeyConditionalPrivilegesBuilder.builder().allowGet()
                .forRealms("realm1", "realm2").forUsers("user1", "user2").build();

        boolean accessAllowed = checkAccess(condPrivilege, CREATE_ACTION, new CreateApiKeyRequest(), authentication);
        assertThat(accessAllowed, is(false));

        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingRealmAndUserName("realm1", "user1"), authentication);
        assertThat(accessAllowed, is(true));

        accessAllowed = checkAccess(condPrivilege, GET_ACTION,
                GetApiKeyRequest.usingRealmAndUserName(randomAlphaOfLength(7), randomAlphaOfLength(7)), authentication);
        assertThat(accessAllowed, is(false));

        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION, InvalidateApiKeyRequest.usingRealmAndUserName("realm2", "user2"),
                authentication);
        assertThat(accessAllowed, is(false));

        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION,
                InvalidateApiKeyRequest.usingRealmAndUserName(randomAlphaOfLength(7), randomAlphaOfLength(7)), authentication);
        assertThat(accessAllowed, is(false));

    }

    public void testManagePrivilegeOwnerOnly() {
        final ManageApiKeyConditionalPrivileges condPrivilege = ManageApiKeyConditionalPrivilegesBuilder.manageApiKeysOnlyForOwner();

        boolean accessAllowed = checkAccess(condPrivilege, CREATE_ACTION, new CreateApiKeyRequest(), authentication);
        assertThat(accessAllowed, is(true));

        // Username and realm name is always required to evaluate condition if authenticated by user
        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingRealmAndUserName("realm1", "user1"), authentication);
        assertThat(accessAllowed, is(true));
        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingRealmAndUserName("realm1", randomAlphaOfLength(4)),
                authentication);
        assertThat(accessAllowed, is(false));
        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingRealmAndUserName(randomAlphaOfLength(4), "user1"),
                authentication);
        assertThat(accessAllowed, is(false));

        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION, InvalidateApiKeyRequest.usingRealmAndUserName("realm1", "user1"),
                authentication);
        assertThat(accessAllowed, is(true));
        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION,
                InvalidateApiKeyRequest.usingRealmAndUserName("realm2", randomAlphaOfLength(4)), authentication);
        assertThat(accessAllowed, is(false));
        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingApiKeyName("api-key-name"), authentication);
        assertThat(accessAllowed, is(false));
        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION, InvalidateApiKeyRequest.usingApiKeyName("api-key-name"),
                authentication);
        assertThat(accessAllowed, is(false));

        // API key id is always required to evaluate condition if authenticated by API key id
        when(authenticatedBy.getName()).thenReturn("_es_api_key");
        when(authenticatedBy.getType()).thenReturn("_es_api_key");
        when(authentication.getMetadata()).thenReturn(Map.of("_security_api_key_id", "user1-api-key-id"));

        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingApiKeyId("user1-api-key-id"), authentication);
        assertThat(accessAllowed, is(true));
        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingApiKeyId(randomAlphaOfLength(5)), authentication);
        assertThat(accessAllowed, is(false));
        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION, InvalidateApiKeyRequest.usingApiKeyId("user1-api-key-id"),
                authentication);
        assertThat(accessAllowed, is(true));
        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION, InvalidateApiKeyRequest.usingApiKeyId(randomAlphaOfLength(5)),
                authentication);
        assertThat(accessAllowed, is(false));
        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingApiKeyName("api-key-name"), authentication);
        assertThat(accessAllowed, is(false));
        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION, InvalidateApiKeyRequest.usingApiKeyName("api-key-name"),
                authentication);
        assertThat(accessAllowed, is(false));
    }

    public void testManagePrivilegeOwnerAndReadOnly() {
        final ManageApiKeyConditionalPrivileges condPrivilege = ManageApiKeyConditionalPrivilegesBuilder.builder().allowGet().forRealms()
                .forUsers().build();

        boolean accessAllowed = checkAccess(condPrivilege, CREATE_ACTION, new CreateApiKeyRequest(), authentication);
        assertThat(accessAllowed, is(false));

        // Username and realm name is always required to evaluate condition if authenticated by user
        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingRealmAndUserName("realm1", "user1"), authentication);
        assertThat(accessAllowed, is(true));
        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingRealmAndUserName("realm1", randomAlphaOfLength(4)),
                authentication);
        assertThat(accessAllowed, is(false));
        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingRealmAndUserName(randomAlphaOfLength(4), "user1"),
                authentication);
        assertThat(accessAllowed, is(false));
        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION, InvalidateApiKeyRequest.usingRealmAndUserName("realm1", "user1"),
                authentication);
        assertThat(accessAllowed, is(false));
        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION,
                InvalidateApiKeyRequest.usingRealmAndUserName("realm2", randomAlphaOfLength(4)), authentication);
        assertThat(accessAllowed, is(false));
        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingApiKeyName("api-key-name"), authentication);
        assertThat(accessAllowed, is(false));
        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION, InvalidateApiKeyRequest.usingApiKeyName("api-key-name"),
                authentication);
        assertThat(accessAllowed, is(false));

        // API key id is always required to evaluate condition if authenticated by API key id
        when(authenticatedBy.getName()).thenReturn("_es_api_key");
        when(authenticatedBy.getType()).thenReturn("_es_api_key");
        when(authentication.getMetadata()).thenReturn(Map.of("_security_api_key_id", "user1-api-key-id"));

        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingApiKeyId("user1-api-key-id"), authentication);
        assertThat(accessAllowed, is(true));
        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingApiKeyId(randomAlphaOfLength(5)), authentication);
        assertThat(accessAllowed, is(false));
        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION, InvalidateApiKeyRequest.usingApiKeyId("user1-api-key-id"),
                authentication);
        assertThat(accessAllowed, is(false));
        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION, InvalidateApiKeyRequest.usingApiKeyId(randomAlphaOfLength(5)),
                authentication);
        assertThat(accessAllowed, is(false));
        accessAllowed = checkAccess(condPrivilege, GET_ACTION, GetApiKeyRequest.usingApiKeyName("api-key-name"), authentication);
        assertThat(accessAllowed, is(false));
        accessAllowed = checkAccess(condPrivilege, INVALIDATE_ACTION, InvalidateApiKeyRequest.usingApiKeyName("api-key-name"),
                authentication);
        assertThat(accessAllowed, is(false));
    }

    private boolean checkAccess(ManageApiKeyConditionalPrivileges privilege, String action, TransportRequest request,
            Authentication authentication) {
        return privilege.getPrivilege().predicate().test(action) && privilege.getRequestPredicate().test(request, authentication);
    }

    public static class ManageApiKeyConditionalPrivilegesBuilder {
        private boolean createAllowed;
        private boolean getAllowed;
        private boolean invalidateAllowed;
        private Set<String> realms;
        private Set<String> users;

        public ManageApiKeyConditionalPrivilegesBuilder allowCreate() {
            this.createAllowed = true;
            return this;
        }

        public ManageApiKeyConditionalPrivilegesBuilder allowGet() {
            this.getAllowed = true;
            return this;
        }

        public ManageApiKeyConditionalPrivilegesBuilder allowInvalidate() {
            this.invalidateAllowed = true;
            return this;
        }

        public ManageApiKeyConditionalPrivilegesBuilder allowAllRealms() {
            this.realms = Set.of("*");
            return this;
        }

        public ManageApiKeyConditionalPrivilegesBuilder allowAllUsers() {
            this.users = Set.of("*");
            return this;
        }

        public ManageApiKeyConditionalPrivilegesBuilder forRealms(String... realms) {
            this.realms = Set.of(realms);
            return this;
        }

        public ManageApiKeyConditionalPrivilegesBuilder forUsers(String... users) {
            this.users = Set.of(users);
            return this;
        }

        public static ManageApiKeyConditionalPrivilegesBuilder builder() {
            return new ManageApiKeyConditionalPrivilegesBuilder();
        }

        public static ManageApiKeyConditionalPrivileges manageApiKeysUnrestricted() {
            return new ManageApiKeyConditionalPrivileges(true, true, true, Set.of("*"), Set.of("*"));
        }

        public static ManageApiKeyConditionalPrivileges manageApiKeysOnlyForOwner() {
            return new ManageApiKeyConditionalPrivileges(true, true, true, null, null);
        }

        public ManageApiKeyConditionalPrivileges build() {
            return new ManageApiKeyConditionalPrivileges(createAllowed, getAllowed, invalidateAllowed, realms, users);
        }
    }
}
