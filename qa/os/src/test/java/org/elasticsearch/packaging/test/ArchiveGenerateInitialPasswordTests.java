/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.apache.http.client.fluent.Request;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;
import org.junit.BeforeClass;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.packaging.util.Archives.installArchive;
import static org.elasticsearch.packaging.util.Archives.verifyArchiveInstallation;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeTrue;

public class ArchiveGenerateInitialPasswordTests extends PackagingTestCase {

    private static final Pattern PASSWORD_REGEX = Pattern.compile("Password for the (\\w+) user is: (.+)$", Pattern.MULTILINE);

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("archives only", distribution.isArchive());
    }

    public void test10Install() throws Exception {
        installation = installArchive(sh, distribution());
        // Enable security for these tests only where it is necessary, until we can enable it for all
        ServerUtils.enableSecurityFeatures(installation);
        verifyArchiveInstallation(installation, distribution());
    }

    public void test20NoAutoGenerationWhenBootstrapPassword() throws Exception {
        /* Windows issue awaits fix: https://github.com/elastic/elasticsearch/issues/49340 */
        assumeTrue("expect command isn't on Windows", distribution.platform != Distribution.Platform.WINDOWS);
        installation.executables().keystoreTool.run("create");
        installation.executables().keystoreTool.run("add --stdin bootstrap.password", "some-password-here");
        Shell.Result result = runElasticsearchStartCommand(null, false, true);
        Map<String, String> usersAndPasswords = parseUsersAndPasswords(result.stdout);
        assertThat(usersAndPasswords.isEmpty(), is(true));
        String response = ServerUtils.makeRequest(Request.Get("http://localhost:9200"), "elastic", "some-password-here", null);
        assertThat(response, containsString("You Know, for Search"));
    }

    public void test30NoAutoGenerationWhenAutoConfigurationDisabled() throws Exception {
        /* Windows issue awaits fix: https://github.com/elastic/elasticsearch/issues/49340 */
        assumeTrue("expect command isn't on Windows", distribution.platform != Distribution.Platform.WINDOWS);
        stopElasticsearch();
        installation.executables().keystoreTool.run("remove bootstrap.password");
        ServerUtils.disableSecurityAutoConfiguration(installation);
        Shell.Result result = runElasticsearchStartCommand(null, false, true);
        Map<String, String> usersAndPasswords = parseUsersAndPasswords(result.stdout);
        assertThat(usersAndPasswords.isEmpty(), is(true));
    }

    public void test40NoAutogenerationWhenDaemonizedWithTty() throws Exception {
        /* Windows issue awaits fix: https://github.com/elastic/elasticsearch/issues/49340 */
        assumeTrue("expect command isn't on Windows", distribution.platform != Distribution.Platform.WINDOWS);
        stopElasticsearch();
        ServerUtils.enableSecurityAutoConfiguration(installation);
        Shell.Result result = runElasticsearchStartCommand(null, true, true);
        Map<String, String> usersAndPasswords = parseUsersAndPasswords(result.stdout);
        assertThat(usersAndPasswords.isEmpty(), is(true));
    }

    public void test50NoAutogenerationWhenNotDaemonizedWithoutTty() throws Exception {
        /* Windows issue awaits fix: https://github.com/elastic/elasticsearch/issues/49340 */
        assumeTrue("expect command isn't on Windows", distribution.platform != Distribution.Platform.WINDOWS);
        stopElasticsearch();
        Shell.Result result = runElasticsearchStartCommand(null, false, false);
        Map<String, String> usersAndPasswords = parseUsersAndPasswords(result.stdout);
        assertThat(usersAndPasswords.isEmpty(), is(true));
    }

    public void test60NoAutogenerationWhenDaemonizedWithoutTty() throws Exception {
        /* Windows issue awaits fix: https://github.com/elastic/elasticsearch/issues/49340 */
        assumeTrue("expect command isn't on Windows", distribution.platform != Distribution.Platform.WINDOWS);
        stopElasticsearch();
        Shell.Result result = runElasticsearchStartCommand(null, true, false);
        Map<String, String> usersAndPasswords = parseUsersAndPasswords(result.stdout);
        assertThat(usersAndPasswords.isEmpty(), is(true));
    }

    public void test70VerifyAutogeneratedCredentials() throws Exception {
        /* Windows issue awaits fix: https://github.com/elastic/elasticsearch/issues/49340 */
        assumeTrue("expect command isn't on Windows", distribution.platform != Distribution.Platform.WINDOWS);
        stopElasticsearch();
        ServerUtils.enableSecurityAutoConfiguration(installation);
        Shell.Result result = runElasticsearchStartCommand(null, false, true);
        Map<String, String> usersAndPasswords = parseUsersAndPasswords(result.stdout);
        assertThat(usersAndPasswords.size(), equalTo(2));
        assertThat(usersAndPasswords.containsKey("elastic"), is(true));
        assertThat(usersAndPasswords.containsKey("kibana_system"), is(true));
        for (Map.Entry<String, String> userpass : usersAndPasswords.entrySet()) {
            String response = ServerUtils.makeRequest(Request.Get("http://localhost:9200"), userpass.getKey(), userpass.getValue(), null);
            assertThat(response, containsString("You Know, for Search"));
        }
    }

    public void test80PasswordAutogenerationOnlyOnce() throws Exception {
        /* Windows issue awaits fix: https://github.com/elastic/elasticsearch/issues/49340 */
        assumeTrue("expect command isn't on Windows", distribution.platform != Distribution.Platform.WINDOWS);
        stopElasticsearch();
        Shell.Result result = runElasticsearchStartCommand(null, false, true);
        Map<String, String> usersAndPasswords = parseUsersAndPasswords(result.stdout);
        assertThat(usersAndPasswords.isEmpty(), is(true));
    }

    private Map<String, String> parseUsersAndPasswords(String output) {
        Matcher matcher = PASSWORD_REGEX.matcher(output);
        assertNotNull(matcher);
        Map<String, String> usersAndPasswords = new HashMap<>();
        while (matcher.find()) {
            usersAndPasswords.put(matcher.group(1), matcher.group(2));
        }
        return usersAndPasswords;
    }
}
