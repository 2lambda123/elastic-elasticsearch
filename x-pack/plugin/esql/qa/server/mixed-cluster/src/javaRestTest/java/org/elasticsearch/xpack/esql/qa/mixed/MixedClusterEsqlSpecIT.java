/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import org.elasticsearch.Version;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.elasticsearch.xpack.ql.CsvSpecReader.CsvTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase.Mode.ASYNC;

public class MixedClusterEsqlSpecIT extends EsqlSpecTestCase {
    public static ElasticsearchCluster cluster = Clusters.mixedVersionCluster();
    public static ClosingTestRule<RestClient> client = new ClosingTestRule<>() {
        @Override
        protected RestClient provideObject() throws IOException {
            return startClient(cluster, Settings.builder().build());
        }
    };
    public static CsvLoader loader = new CsvLoader(client);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(cluster).around(client).around(loader);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    static final Version bwcVersion = Version.fromString(System.getProperty("tests.old_cluster_version"));

    public MixedClusterEsqlSpecIT(String fileName, String groupName, String testName, Integer lineNumber, CsvTestCase testCase, Mode mode) {
        super(fileName, groupName, testName, lineNumber, testCase, mode);
    }

    @Override
    protected void shouldSkipTest(String testName) {
        super.shouldSkipTest(testName);
        assumeTrue("Test " + testName + " is skipped on " + bwcVersion, isEnabled(testName, bwcVersion));
        if (mode == ASYNC) {
            assumeTrue("Async is not supported on " + bwcVersion, supportsAsync());
        }
    }

    @Override
    protected boolean supportsAsync() {
        return bwcVersion.onOrAfter(Version.V_8_13_0);
    }
}
