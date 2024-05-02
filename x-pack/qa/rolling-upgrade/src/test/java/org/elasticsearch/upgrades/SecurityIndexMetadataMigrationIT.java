/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class SecurityIndexMetadataMigrationIT extends AbstractUpgradeTestCase {

    public void testCreateUserForTestInOldCluster() throws Exception {
        assumeTrue("this test should only run against the old cluster", CLUSTER_TYPE == ClusterType.OLD);
        createUser(new String[] { "master-of-the-world", "some-other-role" }, Map.of("test_key", "test_value"));
    }

    public void testNothingIsMigratedInMixedCluster() {
        // TODO can we test this somehow?
        assumeTrue("this test should only run against the mixed cluster", CLUSTER_TYPE == ClusterType.MIXED);
    }

    public void testMetadataCanBeQueriedInUpgradedCluster() throws Exception {
        assumeTrue("this test should only run against the upgraded cluster", CLUSTER_TYPE == ClusterType.UPGRADED);
        final Request request = new Request(randomFrom("POST", "GET"), "/_security/_query/user");
        request.setJsonEntity("""
            {"query":{"term":{"metadata.test_key":"test_value"}}}""");
        final Response response = client().performRequest(request);
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> users = (List<Map<String, Object>>) responseMap.get("users");

        assertEquals(1, users.size());
    }

    private void createUser(String[] roles, Map<String, Object> metadata) throws IOException {

        final Request request = new Request("POST", "/_security/user/" + "test-user");
        BytesReference source = BytesReference.bytes(
            jsonBuilder().map(
                Map.of(
                    User.Fields.USERNAME.getPreferredName(),
                    "test-user",
                    User.Fields.ROLES.getPreferredName(),
                    roles,
                    User.Fields.FULL_NAME.getPreferredName(),
                    "Test User",
                    User.Fields.EMAIL.getPreferredName(),
                    "email@something.com",
                    User.Fields.METADATA.getPreferredName(),
                    metadata,
                    User.Fields.PASSWORD.getPreferredName(),
                    "100%-security-guaranteed",
                    User.Fields.ENABLED.getPreferredName(),
                    true
                )
            )
        );
        request.setJsonEntity(source.utf8ToString());
        Response response = adminClient().performRequest(request);
        assertOK(response);
        assertTrue((boolean) responseAsMap(response).get("created"));
    }
}
