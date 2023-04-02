/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests cross cluster search against a remote cluster which is connected using API Key which has both DLS and FLS permissions defined.
 */
public class RemoteClusterSecurityWithDlsAndFlsRestIT extends AbstractRemoteClusterSecurityWithDlsAndFlsRestIT {

    private static final String REMOTE_CLUSTER_DLS_FLS = REMOTE_CLUSTER_ALIAS + "_dls_fls";

    private static final AtomicReference<Map<String, Object>> API_KEY_REFERENCE = new AtomicReference<>();

    private static final String API_KEY_ROLE = """
        {
          "role": {
            "cluster": ["cross_cluster_access"],
            "index": [
              {
                  "names": ["remote_index*"],
                  "privileges": ["read", "read_cross_cluster"],
                  "query": {"bool": { "must_not": { "term" : {"field2" : "value2"}}}},
                  "field_security": {"grant": [ "field1", "field2" ]}
              }
            ]
          }
        }""";

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .apply(commonClusterConfig)
            .setting("remote_cluster_server.enabled", "true")
            .setting("remote_cluster.port", "0")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .apply(commonClusterConfig)
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .keystore(
                "cluster.remote." + REMOTE_CLUSTER_DLS_FLS + ".credentials",
                () -> createApiKeyForRemoteCluster(API_KEY_ROLE, API_KEY_REFERENCE)
            )
            .build();
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testCrossClusterSearchWithDlsAndFls() throws Exception {
        setupRemoteClusterTestCase(REMOTE_CLUSTER_DLS_FLS);

        final Request searchRequest = new Request(
            "GET",
            Strings.format(
                "/%s:%s/_search?ccs_minimize_roundtrips=%s",
                randomFrom(REMOTE_CLUSTER_DLS_FLS),
                randomFrom("remote_index*", "*"),
                randomBoolean()
            )
        );

        // Running a CCS request with a user without DLS/FLS should be restricted by cross cluster access API key.
        {
            final Response searchResponse = performRequestAgainstQueryingCluster(searchRequest, REMOTE_SEARCH_USER_NO_DLS_FLS);
            assertOK(searchResponse);
            assertSearchResponseContainsExpectedIndicesAndFields(
                searchResponse,
                new String[] { "remote_index1", "remote_index3", "remote_index4" },
                new String[] { "field1", "field2" }
            );
        }

        // Running a CCS request with a user with DLS and FLS should be intersected with cross cluster API key's DLS and FLS permissions.
        {
            final Response searchResponse = performRequestAgainstQueryingCluster(searchRequest, REMOTE_SEARCH_USER_DLS_FLS);
            assertOK(searchResponse);
            assertSearchResponseContainsExpectedIndicesAndFields(
                searchResponse,
                new String[] { "remote_index1" },
                new String[] { "field1", "field2" }
            );
        }

        // Running a CCS request with a user with DLS only.
        {
            final Response searchResponse = performRequestAgainstQueryingCluster(searchRequest, REMOTE_SEARCH_USER_DLS);
            assertOK(searchResponse);
            assertSearchResponseContainsExpectedIndicesAndFields(
                searchResponse,
                new String[] { "remote_index3", "remote_index4" },
                new String[] { "field1", "field2" }
            );
        }

        // Running a CCS request with a user with FLS only.
        {
            final Response searchResponse = performRequestAgainstQueryingCluster(searchRequest, REMOTE_SEARCH_USER_FLS);
            assertOK(searchResponse);
            assertSearchResponseContainsExpectedIndicesAndFields(
                searchResponse,
                new String[] { "remote_index1", "remote_index3", "remote_index4" },
                new String[] { "field1" }
            );
        }
    }
}
