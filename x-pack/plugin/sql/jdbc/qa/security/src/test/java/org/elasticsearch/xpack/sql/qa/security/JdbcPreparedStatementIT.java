/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.security;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.sql.qa.jdbc.PreparedStatementTestCase;

import java.util.Properties;

import static org.elasticsearch.xpack.sql.qa.security.JdbcConnectionIT.SSL_ENABLED;
import static org.elasticsearch.xpack.sql.qa.security.JdbcConnectionIT.securitySettings;

public class JdbcPreparedStatementIT extends PreparedStatementTestCase {

    @Override
    protected Settings restClientSettings() {
        return securitySettings();
    }

    @Override
    protected String getProtocol() {
        return SSL_ENABLED ? "https" : "http";
    }

    @Override
    protected Properties connectionProperties() {
        Properties sp = super.connectionProperties();
        sp.putAll(JdbcSecurityUtils.adminProperties());
        return sp;
    }
}
