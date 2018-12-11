/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Tests that compare the Elasticsearch JDBC client to some other JDBC client
 * after loading a specific set of test data.
 */
public abstract class SpecBaseIntegrationTestCase extends JdbcIntegrationTestCase {
    protected static final String PARAM_FORMATTING = "%2$s.test%3$s";

    protected final String fileName;
    protected final String groupName;
    protected final String testName;
    protected final Integer lineNumber;

    public SpecBaseIntegrationTestCase(String fileName, String groupName, String testName, Integer lineNumber) {
        this.fileName = fileName;
        this.groupName = groupName;
        this.testName = testName;
        this.lineNumber = lineNumber;
    }

    @Before
    public void setupTestDataIfNeeded() throws Exception {
        if (client().performRequest(new Request("HEAD", "/" + indexName())).getStatusLine().getStatusCode() == 404) {
            loadDataset(client());
        }
    }

    protected String indexName() {
        return "test_emp";
    }

    protected void loadDataset(RestClient client) throws Exception {
        DataLoader.loadEmpDatasetIntoEs(client);
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @AfterClass
    public static void wipeTestData() throws IOException {
        try {
            adminClient().performRequest(new Request("DELETE", "/*"));
        } catch (ResponseException e) {
            // 404 here just means we had no indexes
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    public final void test() throws Throwable {
        try {
            assumeFalse("Test marked as Ignored", testName.endsWith("-Ignore"));
            doTest();
        } catch (AssertionError ae) {
            throw reworkException(ae);
        }
    }

    /**
     * Implementations should pay attention on using
     * {@link #executeJdbcQuery(Connection, String)} (typically for
     * ES connections) and {@link #assertResults(ResultSet, ResultSet, boolean)}
     * which takes into account logging/debugging results (through
     * {@link #logEsResultSet()}.
     */
    protected abstract void doTest() throws Throwable;

    protected ResultSet executeJdbcQuery(Connection con, String query) throws SQLException {
        Statement statement = con.createStatement();
        statement.setFetchSize(fetchSize());
        return statement.executeQuery(query);
    }

    protected int fetchSize() {
        return between(1, 500);
    }

    // TODO: use UTC for now until deciding on a strategy for handling date extraction
    @Override
    protected Properties connectionProperties() {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("timezone", "UTC");
        return connectionProperties;
    }

    protected boolean logEsResultSet() {
        return false;
    }

    /*
     * Asserts the numeric results for floating point numbers in a leninent way, if chosen to. Usually,
     * we would want lening treatment for floating point numbers in sql-spec tests where the comparison is being made with H2.
     * Typically, H2 doesn't return floating point numbers for some functions (AVG() being on example), and we'd want
     * to be lenient about this, so that we can still compare the average values between H2 and ES-SQL.
     */
    protected void assertResults(ResultSet expected, ResultSet elastic, boolean lenientFloatingNumbers) throws SQLException {
        Logger log = logEsResultSet() ? logger : null;
        JdbcAssert.assertResultSets(expected, elastic, log, false, lenientFloatingNumbers);
    }

    private Throwable reworkException(Throwable th) {
        StackTraceElement[] stackTrace = th.getStackTrace();
        StackTraceElement[] redone = new StackTraceElement[stackTrace.length + 1];
        System.arraycopy(stackTrace, 0, redone, 1, stackTrace.length);
        redone[0] = new StackTraceElement(getClass().getName(), groupName + ".test" + testName, fileName, lineNumber);

        th.setStackTrace(redone);
        return th;
    }

    //
    // spec reader
    //

    // returns source file, groupName, testName, its line location, and the custom object (based on each test parser)
    protected static List<Object[]> readScriptSpec(String url, Parser parser) throws Exception {
        URL source = SpecBaseIntegrationTestCase.class.getResource(url);
        Objects.requireNonNull(source, "Cannot find resource " + url);

        String fileName = source.getFile().substring(source.getFile().lastIndexOf("/") + 1);
        String groupName = fileName.substring(fileName.lastIndexOf('/') + 1, fileName.lastIndexOf("."));

        Map<String, Integer> testNames = new LinkedHashMap<>();
        List<Object[]> testCases = new ArrayList<>();

        String testName = null;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(readFromJarUrl(source), StandardCharsets.UTF_8))) {
            String line;
            int lineNumber = 1;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                // ignore comments
                if (!line.isEmpty() && !line.startsWith("//")) {
                    // parse test name
                    if (testName == null) {
                        if (testNames.keySet().contains(line)) {
                            throw new IllegalStateException("Duplicate test name '" + line + "' at line " + lineNumber
                                    + " (previously seen at line " + testNames.get(line) + ")");
                        } else {
                            testName = Strings.capitalize(line);
                            testNames.put(testName, Integer.valueOf(lineNumber));
                        }
                    } else {
                        Object result = parser.parse(line);
                        // only if the parser is ready, add the object - otherwise keep on serving it lines
                        if (result != null) {
                            testCases.add(new Object[] { fileName, groupName, testName, Integer.valueOf(lineNumber), result });
                            testName = null;
                        }
                    }
                }
                lineNumber++;
            }
            if (testName != null) {
                throw new IllegalStateException("Read a test without a body at the end of [" + fileName + "].");
            }
        }
        assertNull("Cannot find spec for test " + testName, testName);

        return testCases;
    }

    public interface Parser {
        Object parse(String line);
    }

    @SuppressForbidden(reason = "test reads from jar")
    public static InputStream readFromJarUrl(URL source) throws IOException {
        return source.openStream();
    }
}
