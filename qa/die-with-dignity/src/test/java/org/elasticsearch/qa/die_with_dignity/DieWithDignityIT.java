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

package org.elasticsearch.qa.die_with_dignity;

import org.apache.http.ConnectionClosedException;
import org.apache.lucene.util.Constants;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.JsonLogLine;
import org.elasticsearch.common.logging.JsonLogsStream;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class DieWithDignityIT extends ESRestTestCase {

    public void testDieWithDignity() throws Exception {
        // deleting the PID file prevents stopping the cluster from failing since it occurs if and only if the PID file exists
        final Path pidFile = PathUtils.get(System.getProperty("pidfile"));
        final List<String> pidFileLines = Files.readAllLines(pidFile);
        assertThat(pidFileLines, hasSize(1));
        final int pid = Integer.parseInt(pidFileLines.get(0));
        Files.delete(pidFile);
        IOException e = expectThrows(IOException.class,
            () -> client().performRequest(new Request("GET", "/_die_with_dignity")));
        Matcher<IOException> failureMatcher = instanceOf(ConnectionClosedException.class);
        if (Constants.WINDOWS) {
            /*
             * If the other side closes the connection while we're waiting to fill our buffer
             * we can get IOException with the message below. It seems to only come up on
             * Windows and it *feels* like it could be a ConnectionClosedException but
             * upstream does not consider this a bug:
             * https://issues.apache.org/jira/browse/HTTPASYNC-134
             *
             * So we catch it here and consider it "ok".
             */
            failureMatcher = either(failureMatcher)
                .or(hasToString(containsString("An existing connection was forcibly closed by the remote host")));
        }
        assertThat(e, failureMatcher);

        // the Elasticsearch process should die and disappear from the output of jps
        assertBusy(() -> {
            final String jpsPath = PathUtils.get(System.getProperty("runtime.java.home"), "bin/jps").toString();
            final Process process = new ProcessBuilder().command(jpsPath).start();
            assertThat(process.waitFor(), equalTo(0));
            try (InputStream is = process.getInputStream();
                 BufferedReader in = new BufferedReader(new InputStreamReader(is, "UTF-8"))) {
                String line;
                while ((line = in.readLine()) != null) {
                    final int currentPid = Integer.parseInt(line.split("\\s+")[0]);
                    assertThat(line, pid, not(equalTo(currentPid)));
                }
            }
        });

        try {
            // parse the logs and ensure that Elasticsearch died with the expected cause
            Path path = PathUtils.get(System.getProperty("log"));
            try (Stream<JsonLogLine> stream = JsonLogsStream.from(path)) {
                final Iterator<JsonLogLine> it = stream.iterator();

                boolean fatalError = false;
                boolean fatalErrorInThreadExiting = false;

                while (it.hasNext() && (fatalError == false || fatalErrorInThreadExiting == false)) {
                    final JsonLogLine line = it.next();
                    if (isFatalError(line)) {
                        fatalError = true;
                    } else if (isFatalErrorInThreadExiting(line) || isErrorExceptionReceived(line)) {
                        fatalErrorInThreadExiting = true;
                        assertThat(line.stacktrace(),
                            hasItem(Matchers.containsString("java.lang.OutOfMemoryError: die with dignity")));
                    }
                }

                assertTrue(fatalError);
                assertTrue(fatalErrorInThreadExiting);
            }
        } catch (AssertionError ae) {
            Path path = PathUtils.get(System.getProperty("log"));
            debugLogs(path);
            throw ae;
        }
    }

    private boolean isErrorExceptionReceived(JsonLogLine line) {
        return line.level().equals("ERROR")
            && line.component().equals("o.e.h.AbstractHttpServerTransport")
            && line.nodeName().equals("node-0")
            && line.message().contains("caught exception while handling client http traffic");
    }

    private void debugLogs(Path path) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            Terminal terminal = Terminal.DEFAULT;
            reader.lines().forEach(line -> terminal.println(line));
        }
    }

    private boolean isFatalErrorInThreadExiting(JsonLogLine line) {
        return line.level().equals("ERROR")
            && line.component().equals("o.e.b.ElasticsearchUncaughtExceptionHandler")
            && line.nodeName().equals("node-0")
            && line.message().matches("fatal error in thread \\[Thread-\\d+\\], exiting$");
    }

    private boolean isFatalError(JsonLogLine line) {
        return line.level().equals("ERROR")
            && line.component().equals("o.e.ExceptionsHelper")
            && line.nodeName().equals("node-0")
            && line.message().contains("fatal error");
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // as the cluster is dead its state can not be wiped successfully so we have to bypass wiping the cluster
        return true;
    }

}
