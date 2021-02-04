/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.client.ClientVersion;

public class VersionTests extends ESTestCase {
    public void testVersionIsCurrent() {
        /* This test will only work properly in gradle because in gradle we run the tests
         * using the jar. */
        assertEquals(org.elasticsearch.Version.CURRENT.major, ClientVersion.CURRENT.major);
        assertEquals(org.elasticsearch.Version.CURRENT.minor, ClientVersion.CURRENT.minor);
        assertEquals(org.elasticsearch.Version.CURRENT.revision, ClientVersion.CURRENT.revision);
    }
}
