/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import org.elasticsearch.jdk.JavaVersion;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AlgorithmParameters;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.interfaces.ECPrivateKey;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.ECParameterSpec;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.StringContains.containsString;

public class PemUtilsTests extends ESTestCase {

    private static final Supplier<char[]> EMPTY_PASSWORD = () -> new char[0];
    private static final Supplier<char[]> TESTNODE_PASSWORD = "testnode"::toCharArray;

    public void testReadPKCS8RsaKey() throws Exception {
        Key key = getKeyFromKeystore("RSA");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/rsa_key_pkcs8_plain.pem"), EMPTY_PASSWORD);
        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadPKCS8RsaKeyWithBagAttrs() throws Exception {
        Key key = getKeyFromKeystore("RSA");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/testnode_with_bagattrs.pem"), EMPTY_PASSWORD);
        assertThat(privateKey, equalTo(key));
    }

    public void testReadPKCS8DsaKey() throws Exception {
        Key key = getKeyFromKeystore("DSA");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/dsa_key_pkcs8_plain.pem"), EMPTY_PASSWORD);
        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadEcKeyCurves() throws Exception {
        String curve = randomFrom("secp256r1", "secp384r1", "secp521r1");
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/private_" + curve + ".pem"), ""::toCharArray);
        assertThat(privateKey, instanceOf(ECPrivateKey.class));
        ECParameterSpec parameterSpec = ((ECPrivateKey) privateKey).getParams();
        ECGenParameterSpec algorithmParameterSpec = new ECGenParameterSpec(curve);
        AlgorithmParameters algoParameters = AlgorithmParameters.getInstance("EC");
        algoParameters.init(algorithmParameterSpec);
        assertThat(parameterSpec, equalTo(algoParameters.getParameterSpec(ECParameterSpec.class)));
    }

    public void testReadPKCS8EcKey() throws Exception {
        Key key = getKeyFromKeystore("EC");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/ec_key_pkcs8_plain.pem"), EMPTY_PASSWORD);
        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadEncryptedPKCS8PBES1Key() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBE KeySpec is not available", inFipsJvm());
        Key key = getKeyFromKeystore("RSA");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(
            getDataPath("/certs/pem-utils/key_pkcs8_encrypted_pbes1_des.pem"),
            TESTNODE_PASSWORD
        );
        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadEncryptedPKCS8PBES2AESKey() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBE KeySpec is not available", inFipsJvm());

        Key key = getKeyFromKeystore("RSA");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));

        final Path keyPath = getDataPath("/certs/pem-utils/key_pkcs8_encrypted_pbes2_aes.pem");
        try {
            PrivateKey privateKey = PemUtils.readPrivateKey(keyPath, TESTNODE_PASSWORD);
            assertThat(privateKey, notNullValue());
            assertThat(privateKey, equalTo(key));
        } catch (SslConfigException exception) {
            if (isJdk8()) {
                // We know that PBES2 is not supported on OpenJDK8
                assertThat(exception.getMessage(), containsString("key_pkcs8_encrypted_pbes2_aes.pem"));
                Throwable cause = exception.getCause();
                assertThat(cause, instanceOf(GeneralSecurityException.class));
                assertThat(
                    cause.getMessage(),
                    startsWith("PKCS#8 Private Key is encrypted with PBES2 which is not supported on this JDK [8")
                );
                assertThat(cause.getCause(), instanceOf(IOException.class));
            } else {
                throw exception;
            }
        }
    }

    public void testReadEncryptedPKCS8PBES2DESKey() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBE KeySpec is not available", inFipsJvm());

        // Sun JSE cannot read keys encrypted with PBES2 DES (but does support AES with PBES2 and DES with PBES1)
        // Rather than add our own support for this we just detect that our error message is clear and meaningful
        final SslConfigException exception = expectThrows(
            SslConfigException.class,
            () -> PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/key_pkcs8_encrypted_pbes2_des.pem"), TESTNODE_PASSWORD)
        );
        assertThat(exception.getMessage(), containsString("key_pkcs8_encrypted_pbes2_des.pem"));
        Throwable cause = exception.getCause();
        assertThat(cause, instanceOf(GeneralSecurityException.class));
        final Throwable rootCause = cause.getCause();
        assertThat(rootCause, instanceOf(IOException.class));

        assertThat(
            cause.getMessage(),
            equalTo("PKCS#8 Private Key is encrypted with unsupported PBES2 algorithm [1.3.14.3.2.7] (DES-CBC)")
        );
        if (isJdk8() == false) {
            assertThat(cause.getCause().getMessage(), startsWith("PBE parameter parsing error"));
        }
    }

    public void testReadDESEncryptedPKCS1Key() throws Exception {
        Key key = getKeyFromKeystore("RSA");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/testnode.pem"), TESTNODE_PASSWORD);
        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadAESEncryptedPKCS1Key() throws Exception {
        Key key = getKeyFromKeystore("RSA");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        String bits = randomFrom("128", "192", "256");
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/testnode-aes" + bits + ".pem"), TESTNODE_PASSWORD);

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadPKCS1RsaKey() throws Exception {
        Key key = getKeyFromKeystore("RSA");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/testnode-unprotected.pem"), TESTNODE_PASSWORD);

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadOpenSslDsaKey() throws Exception {
        Key key = getKeyFromKeystore("DSA");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/dsa_key_openssl_plain.pem"), EMPTY_PASSWORD);

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadOpenSslDsaKeyWithParams() throws Exception {
        Key key = getKeyFromKeystore("DSA");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(
            getDataPath("/certs/pem-utils/dsa_key_openssl_plain_with_params.pem"),
            EMPTY_PASSWORD
        );

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadEncryptedOpenSslDsaKey() throws Exception {
        Key key = getKeyFromKeystore("DSA");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/dsa_key_openssl_encrypted.pem"), TESTNODE_PASSWORD);

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadOpenSslEcKey() throws Exception {
        Key key = getKeyFromKeystore("EC");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/ec_key_openssl_plain.pem"), EMPTY_PASSWORD);

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadOpenSslEcKeyWithParams() throws Exception {
        Key key = getKeyFromKeystore("EC");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(
            getDataPath("/certs/pem-utils/ec_key_openssl_plain_with_params.pem"),
            EMPTY_PASSWORD
        );

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadEncryptedOpenSslEcKey() throws Exception {
        Key key = getKeyFromKeystore("EC");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/ec_key_openssl_encrypted.pem"), TESTNODE_PASSWORD);

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadUnsupportedKey() {
        final Path path = getDataPath("/certs/pem-utils/key_unsupported.pem");
        SslConfigException e = expectThrows(SslConfigException.class, () -> PemUtils.readPrivateKey(path, TESTNODE_PASSWORD));
        assertThat(e.getMessage(), containsString("file does not contain a supported key format"));
        assertThat(e.getMessage(), containsString(path.toAbsolutePath().toString()));
    }

    public void testReadPemCertificateAsKey() {
        final Path path = getDataPath("/certs/pem-utils/testnode.crt");
        SslConfigException e = expectThrows(SslConfigException.class, () -> PemUtils.readPrivateKey(path, TESTNODE_PASSWORD));
        assertThat(e.getMessage(), containsString("file does not contain a supported key format"));
        assertThat(e.getMessage(), containsString(path.toAbsolutePath().toString()));
    }

    public void testReadCorruptedKey() {
        final Path path = getDataPath("/certs/pem-utils/corrupted_key_pkcs8_plain.pem");
        SslConfigException e = expectThrows(SslConfigException.class, () -> PemUtils.readPrivateKey(path, TESTNODE_PASSWORD));
        assertThat(e.getMessage(), containsString("private key"));
        assertThat(e.getMessage(), containsString("cannot be parsed"));
        assertThat(e.getMessage(), containsString(path.toAbsolutePath().toString()));
        assertThat(e.getCause().getMessage(), containsString("PEM footer is invalid or missing"));
    }

    public void testReadEmptyFile() {
        final Path path = getDataPath("/certs/pem-utils/empty.pem");
        SslConfigException e = expectThrows(SslConfigException.class, () -> PemUtils.readPrivateKey(path, TESTNODE_PASSWORD));
        assertThat(e.getMessage(), containsString("file is empty"));
        assertThat(e.getMessage(), containsString(path.toAbsolutePath().toString()));
    }

    private Key getKeyFromKeystore(String algo) throws Exception {
        Path keystorePath = getDataPath("/certs/pem-utils/testnode.jks");
        try (InputStream in = Files.newInputStream(keystorePath)) {
            KeyStore keyStore = KeyStore.getInstance("jks");
            keyStore.load(in, "testnode".toCharArray());
            return keyStore.getKey("testnode_" + algo, "testnode".toCharArray());
        }
    }

    private boolean isJdk8() {
        return JavaVersion.current().getVersion().get(0) == 8;
    }
}
