/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transform.log4j;

import junit.framework.TestCase;

import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TransformLog4jConfigTests extends TestCase {

    /**
     * Check that the transformer doesn't explode when given an empty file.
     */
    public void testTransformEmptyConfig() {
        final List<String> transformed = TransformLog4jConfig.transformConfig(List.of());
        assertThat(transformed, is(empty()));
    }

    /**
     * Check that the transformer leaves non-appender lines alone.
     */
    public void testTransformEchoesNonAppenderLines() {
        List<String> input = List.of(
            "status = error",
            "",
            "##############################",
            "rootLogger.level = info",
            "example = \"broken\\",
            "    line\""
        );
        final List<String> transformed = TransformLog4jConfig.transformConfig(input);
        assertThat(transformed, equalTo(input));
    }

    /**
     * Check that the root logger appenders are filter to just the "rolling" appender
     */
    public void testTransformFiltersRootLogger() {
        List<String> input = List.of(
            "rootLogger.appenderRef.console.ref = console",
            "rootLogger.appenderRef.rolling.ref = rolling",
            "rootLogger.appenderRef.rolling_old.ref = rolling_old"
        );
        final List<String> transformed = TransformLog4jConfig.transformConfig(input);
        assertThat(transformed, hasSize(1));
        assertThat(transformed.get(0), equalTo("rootLogger.appenderRef.rolling.ref = rolling"));
    }

    /**
     * Check that any explicit 'console' or 'rolling_old' appenders are removed.
     */
    public void testTransformRemoveExplicitConsoleAndRollingOldAppenders() {
        List<String> input = List.of(
            "appender.console.type = Console",
            "appender.console.name = console",
            "appender.console.layout.type = PatternLayout",
            "appender.console.layout.pattern = [%d{ISO8601}][%-5p][%-25c{1.}] [%node_name]%marker %m%n",
            "appender.rolling_old.type = RollingFile",
            "appender.rolling_old.name = rolling_old",
            "appender.rolling_old.layout.type = PatternLayout",
            "appender.rolling_old.layout.pattern = [%d{ISO8601}][%-5p][%-25c{1.}] [%node_name]%marker %m%n"
        );
        final List<String> transformed = TransformLog4jConfig.transformConfig(input);
        assertThat(transformed, is(empty()));
    }

    /**
     * Check that rolling file appenders are converted to console appenders.
     */
    public void testTransformConvertsRollingToConsole() {
        List<String> input = List.of("appender.rolling.type = RollingFile", "appender.rolling.name = rolling");
        final List<String> transformed = TransformLog4jConfig.transformConfig(input);

        List<String> expected = List.of("appender.rolling.type = Console", "appender.rolling.name = rolling");
        assertThat(transformed, equalTo(expected));
    }

    /**
     * Check that rolling file appenders have redundant properties removed.
     */
    public void testTransformRemovedRedundantProperties() {
        List<String> input = List.of(
            "appender.rolling.fileName = ${sys:es.logs.base_path}/${sys:es.logs.cluster_name}_server.json",
            "appender.rolling.layout.type = ECSJsonLayout",
            "appender.rolling.layout.type_name = server",
            "appender.rolling.filePattern = ${sys:es.logs.base_path}/${sys:es.logs.cluster_name}-%d{yyyy-MM-dd}-%i.json.gz",
            "appender.rolling.policies.type = Policies",
            "appender.rolling.strategy.type = DefaultRolloverStrategy"
        );
        final List<String> transformed = TransformLog4jConfig.transformConfig(input);

        List<String> expected = List.of("appender.rolling.layout.type = ECSJsonLayout", "appender.rolling.layout.type_name = server");
        assertThat(transformed, equalTo(expected));
    }

    /**
     * Check that rolling file appenders have redundant properties removed.
     */
    public void testTransformSkipsPropertiesWithLineBreaks() {
        List<String> input = List.of(
            "appender.rolling.fileName = ${sys:es.logs.base_path}${sys:file.separator}\\",
            "    ${sys:es.logs.cluster_name}_server.json",
            "appender.rolling.layout.type = ECSJsonLayout"
        );
        final List<String> transformed = TransformLog4jConfig.transformConfig(input);

        List<String> expected = List.of("appender.rolling.layout.type = ECSJsonLayout");
        assertThat(transformed, equalTo(expected));
    }
}
