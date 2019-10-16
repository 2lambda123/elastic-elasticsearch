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

package org.elasticsearch.packaging.test;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;
import com.carrotsearch.randomizedtesting.annotations.Timeout;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.packaging.util.Archives;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Docker;
import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.Installation;
import org.elasticsearch.packaging.util.Packages;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.Shell;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.elasticsearch.packaging.util.Cleanup.cleanEverything;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/**
 * Class that all packaging test cases should inherit from
 */
@RunWith(RandomizedRunner.class)
@TestMethodProviders({
    JUnit3MethodProvider.class
})
@Timeout(millis = 20 * 60 * 1000) // 20 min
@TestCaseOrdering(TestCaseOrdering.AlphabeticOrder.class)
public abstract class PackagingTestCase extends Assert {

    protected final Logger logger =  LogManager.getLogger(getClass());

    // the distribution being tested
    protected static final Distribution distribution;
    static {
        distribution = new Distribution(Paths.get(System.getProperty("tests.distribution")));
    }

    // the java installation already installed on the system
    protected static final String systemJavaHome;
    static {
        Shell sh = new Shell();
        if (Platforms.WINDOWS) {
            systemJavaHome = sh.run("$Env:SYSTEM_JAVA_HOME").stdout.trim();
        } else {
            assert Platforms.LINUX || Platforms.DARWIN;
            systemJavaHome = sh.run("echo $SYSTEM_JAVA_HOME").stdout.trim();
        }
    }

    // the current installation of the distribution being tested
    protected static Installation installation;

    private static boolean failed;

    @ClassRule
    public static final TestWatcher testFailureRule = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            failed = true;
        }
    };

    // a shell to run system commands with
    protected Shell sh;

    @Rule
    public final TestName testNameRule = new TestName();

    @BeforeClass
    public static void filterCompatible() {
        assumeTrue("only compatible distributions", distribution.packaging.compatible);
    }

    @BeforeClass
    public static void cleanup() throws Exception {
        installation = null;
        cleanEverything();
    }

    @Before
    public void setup() throws Exception {
        assumeFalse(failed); // skip rest of tests once one fails

        sh = newShell();
    }

    @After
    public void teardown() throws Exception {
        // move log file so we can avoid false positives when grepping for
        // messages in logs during test
        if (installation != null && Files.exists(installation.logs)) {
            Path logFile = installation.logs.resolve("elasticsearch.log");
            String prefix = this.getClass().getSimpleName() + "." + testNameRule.getMethodName();
            if (Files.exists(logFile)) {
                Path newFile = installation.logs.resolve(prefix + ".elasticsearch.log");
                FileUtils.mv(logFile, newFile);
            }
            for (Path rotatedLogFile : FileUtils.lsGlob(installation.logs, "elasticsearch*.tar.gz")) {
                Path newRotatedLogFile = installation.logs.resolve(prefix + "." + rotatedLogFile.getFileName());
                FileUtils.mv(rotatedLogFile, newRotatedLogFile);
            }
        }
    }

    /** The {@link Distribution} that should be tested in this case */
    protected static Distribution distribution() {
        return distribution;
    }

    protected Shell newShell() throws Exception {
        Shell sh = new Shell();
        if (distribution().hasJdk == false) {
            Platforms.onLinux(() -> {
                sh.getEnv().put("JAVA_HOME", systemJavaHome);
            });
            Platforms.onWindows(() -> {
                sh.getEnv().put("JAVA_HOME", systemJavaHome);
            });
        }
        return sh;
    }

    public Shell.Result startElasticsearch() throws Exception {
        if (distribution.isPackage()) {
            return Packages.startElasticsearch(sh);
        } else {
            assertTrue(distribution.isArchive());
            return Archives.startElasticsearch(installation, sh);
        }
    }

    public Shell.Result startElasticsearchStandardInputPassword(String password) {
        assertTrue("Only archives support passwords on standard input", distribution().isArchive());
        return Archives.startElasticsearch(installation, sh, password);
    }

    public Shell.Result startElasticsearchTtyPassword(String password) throws Exception {
        assertTrue("Only archives support passwords on TTY", distribution().isArchive());
        return Archives.startElasticsearchWithTty(installation, sh, password);
    }

    public void stopElasticsearch() throws Exception {
        distribution().packagingConditional()
            .forPackage(() -> Packages.stopElasticsearch(sh))
            .forArchive(() -> Archives.stopElasticsearch(installation, sh))
            .forDocker(/* TODO */ Platforms.NO_ACTION)
            .run();
    }

    public void awaitElasticsearchStartup(Shell.Result result) throws Exception {
        assertThat("Startup command should succeed", result.exitCode, equalTo(0));
        distribution().packagingConditional()
            .forPackage(() -> Packages.assertElasticsearchStarted(sh, installation))
            .forArchive(() -> Archives.assertElasticsearchStarted(installation, sh))
            .forDocker(Docker::waitForElasticsearchToStart)
            .run();
    }

    public void assertElasticsearchFailure(Shell.Result result, String expectedMessage) {

        if (Files.exists(installation.logs.resolve("elasticsearch.log"))) {

            // If log file exists, then we have bootstrapped our logging and the
            // error should be in the logs
            assertTrue("log file exists", Files.exists(installation.logs.resolve("elasticsearch.log")));
            String logfile = FileUtils.slurp(installation.logs.resolve("elasticsearch.log"));
            assertThat(logfile, containsString(expectedMessage));

        } else if (distribution().isPackage() && Platforms.isSystemd()) {

            // For systemd, retrieve the error from journalctl
            assertThat(result.stderr, containsString("Job for elasticsearch.service failed"));
            Shell.Result error = sh.run("journalctl --boot --unit elasticsearch.service");
            assertThat(error.stdout, containsString(expectedMessage));

        } else if (Platforms.WINDOWS == true) {

            // In Windows, we have written our stdout and stderr to files in order to run
            // in the background
            String wrapperPid = result.stdout.trim();
            sh.runIgnoreExitCode("Wait-Process -Timeout 15 -Id " + wrapperPid);
            sh.runIgnoreExitCode("Get-EventSubscriber | " +
                "where {($_.EventName -eq 'OutputDataReceived' -Or $_.EventName -eq 'ErrorDataReceived' |" +
                "Unregister-EventSubscriber -Force");
            assertThat(FileUtils.slurp(Archives.getPowershellErrorPath(installation)), containsString(expectedMessage));

        } else {

            // Otherwise, error should be on shell stderr
            assertThat(result.stderr, containsString(expectedMessage));

        }
    }
}
