/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackSettings;

import static org.hamcrest.Matchers.containsString;

public class PasswordHashingAlgorithmBootstrapCheckTests extends ESTestCase {

    public void testPasswordHashingAlgorithmBootstrapCheck() {
        Settings settings = Settings.EMPTY;
        assertFalse(new PasswordHashingAlgorithmBootstrapCheck().check(new BootstrapContext(settings, null)).isFailure());

        settings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), "PBKDF2_10000").build();
        assertFalse(new PasswordHashingAlgorithmBootstrapCheck().check(new BootstrapContext(settings, null)).isFailure());

        settings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), "PBKDF2").build();
        assertFalse(new PasswordHashingAlgorithmBootstrapCheck().check(new BootstrapContext(settings, null)).isFailure());

        settings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), "BCRYPT").build();
        assertFalse(new PasswordHashingAlgorithmBootstrapCheck().check(new BootstrapContext(settings, null)).isFailure());

        settings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), "BCRYPT11").build();
        assertFalse(new PasswordHashingAlgorithmBootstrapCheck().check(new BootstrapContext(settings, null)).isFailure());

        settings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), "SHA1").build();
        assertTrue(new PasswordHashingAlgorithmBootstrapCheck().check(new BootstrapContext(settings, null)).isFailure());
        assertThat(new PasswordHashingAlgorithmBootstrapCheck().check(new BootstrapContext(settings, null)).getMessage(),
            containsString(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey()));

        settings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), "MD5").build();
        assertTrue(new PasswordHashingAlgorithmBootstrapCheck().check(new BootstrapContext(settings, null)).isFailure());
        assertThat(new PasswordHashingAlgorithmBootstrapCheck().check(new BootstrapContext(settings, null)).getMessage(),
            containsString(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey()));

        settings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), "SSHA256").build();
        assertTrue(new PasswordHashingAlgorithmBootstrapCheck().check(new BootstrapContext(settings, null)).isFailure());
        assertThat(new PasswordHashingAlgorithmBootstrapCheck().check(new BootstrapContext(settings, null)).getMessage(),
            containsString(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey()));

        settings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), "Argon").build();
        assertTrue(new PasswordHashingAlgorithmBootstrapCheck().check(new BootstrapContext(settings, null)).isFailure());
        assertThat(new PasswordHashingAlgorithmBootstrapCheck().check(new BootstrapContext(settings, null)).getMessage(),
            containsString(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey()));

    }
}
