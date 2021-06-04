/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import org.elasticsearch.common.Nullable;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A variety of utility methods for working with or constructing {@link KeyStore} instances.
 */
public final class KeyStoreUtil {

    private KeyStoreUtil() {
        throw new IllegalStateException("Utility class should not be instantiated");
    }

    /**
     * Make a best guess about the "type" (see {@link KeyStore#getType()}) of the keystore file located at the given {@code Path}.
     * This method only references the <em>file name</em> of the keystore, it does not look at its contents.
     */
    static String inferKeyStoreType(String path) {
        String name = path == null ? "" : path.toLowerCase(Locale.ROOT);
        if (name.endsWith(".p12") || name.endsWith(".pfx") || name.endsWith(".pkcs12")) {
            return "PKCS12";
        } else {
            return "jks";
        }
    }

    /**
     * Read the given keystore file.
     *
     * @throws SslConfigException       If there is a problem reading from the provided path
     * @throws GeneralSecurityException If there is a problem with the keystore contents
     */
    public static KeyStore readKeyStore(Path path, String ksType, char[] password) throws GeneralSecurityException, IOException {
        KeyStore keyStore = KeyStore.getInstance(ksType);
        if (path != null) {
            try (InputStream in = Files.newInputStream(path)) {
                keyStore.load(in, password);
            }
        }
        return keyStore;
    }

    /**
     * Construct an in-memory keystore with a single key entry.
     *
     * @param certificateChain A certificate chain (ordered from subject to issuer)
     * @param privateKey       The private key that corresponds to the subject certificate (index 0 of {@code certificateChain})
     * @param password         The password for the private key
     * @throws GeneralSecurityException If there is a problem with the provided certificates/key
     */
    public static KeyStore buildKeyStore(Collection<Certificate> certificateChain, PrivateKey privateKey, char[] password)
        throws GeneralSecurityException {
        KeyStore keyStore = buildNewKeyStore();
        keyStore.setKeyEntry("key", privateKey, password, certificateChain.toArray(new Certificate[0]));
        return keyStore;
    }

    /**
     * Construct an in-memory keystore with multiple trusted cert entries.
     *
     * @param certificates The root certificates to trust
     */
    public static KeyStore buildTrustStore(Iterable<Certificate> certificates) throws GeneralSecurityException {
        assert certificates != null : "Cannot create keystore with null certificates";
        KeyStore store = buildNewKeyStore();
        int counter = 0;
        for (Certificate certificate : certificates) {
            store.setCertificateEntry("cert-" + counter, certificate);
            counter++;
        }
        return store;
    }

    private static KeyStore buildNewKeyStore() throws GeneralSecurityException {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try {
            keyStore.load(null, null);
        } catch (IOException e) {
            // This should never happen so callers really shouldn't be forced to deal with it themselves.
            throw new SslConfigException("Unexpected error initializing a new in-memory keystore", e);
        }
        return keyStore;
    }

    /**
     * Returns a {@link X509ExtendedKeyManager} that is built from the provided private key and certificate chain
     */
    public static X509ExtendedKeyManager createKeyManager(Certificate[] certificateChain, PrivateKey privateKey, char[] password)
        throws GeneralSecurityException, IOException {
        KeyStore keyStore = buildKeyStore(List.of(certificateChain), privateKey, password);
        return createKeyManager(keyStore, password, KeyManagerFactory.getDefaultAlgorithm());
    }

    /**
     * Creates a {@link X509ExtendedKeyManager} based on the key material in the provided {@link KeyStore}
     */
    public static X509ExtendedKeyManager createKeyManager(KeyStore keyStore, char[] password,
                                                          String algorithm) throws GeneralSecurityException {
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(algorithm);
        kmf.init(keyStore, password);
        KeyManager[] keyManagers = kmf.getKeyManagers();
        for (KeyManager keyManager : keyManagers) {
            if (keyManager instanceof X509ExtendedKeyManager) {
                return (X509ExtendedKeyManager) keyManager;
            }
        }
        throw new SslConfigException("failed to find a X509ExtendedKeyManager in the key manager factory for [" + algorithm
            + "] and keystore [" + keyStore + "]");
    }

    /**
     * Creates a {@link X509ExtendedTrustManager} based on the trust material in the provided {@link KeyStore}
     */
    public static X509ExtendedTrustManager createTrustManager(@Nullable KeyStore trustStore, String algorithm)
        throws NoSuchAlgorithmException, KeyStoreException {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(algorithm);
        tmf.init(trustStore);
        TrustManager[] trustManagers = tmf.getTrustManagers();
        for (TrustManager trustManager : trustManagers) {
            if (trustManager instanceof X509ExtendedTrustManager) {
                return (X509ExtendedTrustManager) trustManager;
            }
        }
        throw new SslConfigException("failed to find a X509ExtendedTrustManager in the trust manager factory for [" + algorithm
            + "] and truststore [" + trustStore + "]");
    }

    /**
     * Creates a {@link X509ExtendedTrustManager} based on the provided certificates
     *
     * @param certificates the certificates to trust
     * @return a trust manager that trusts the provided certificates
     */
    public static X509ExtendedTrustManager createTrustManager(Collection<Certificate> certificates) throws GeneralSecurityException {
        KeyStore store = buildTrustStore(certificates);
        return createTrustManager(store, TrustManagerFactory.getDefaultAlgorithm());
    }

    static Stream<KeyStoreEntry> stream(KeyStore keyStore,
                                        Function<GeneralSecurityException, ? extends RuntimeException> exceptionHandler) {
        try {
            return Collections.list(keyStore.aliases()).stream().map(a -> new KeyStoreEntry(keyStore, a, exceptionHandler));
        } catch (KeyStoreException e) {
            throw exceptionHandler.apply(e);
        }
    }

    static class KeyStoreEntry {
        private final KeyStore store;
        private final String alias;
        private final Function<GeneralSecurityException, ? extends RuntimeException> exceptionHandler;

        KeyStoreEntry(KeyStore store, String alias, Function<GeneralSecurityException, ? extends RuntimeException> exceptionHandler) {
            this.store = store;
            this.alias = alias;
            this.exceptionHandler = exceptionHandler;
        }

        public String getAlias() {
            return alias;
        }

        public X509Certificate getX509Certificate() {
            try {
                final Certificate c = store.getCertificate(alias);
                if (c instanceof X509Certificate) {
                    return (X509Certificate) c;
                } else {
                    return null;
                }
            } catch (KeyStoreException e) {
                throw exceptionHandler.apply(e);
            }
        }

        public boolean isKeyEntry() {
            try {
                return store.isKeyEntry(alias);
            } catch (KeyStoreException e) {
                throw exceptionHandler.apply(e);
            }
        }

        public PrivateKey getKey(char[] password) {
            try {
                final Key key = store.getKey(alias, password);
                if (key instanceof PrivateKey) {
                    return (PrivateKey) key;
                }
                return null;
            } catch (GeneralSecurityException e) {
                throw exceptionHandler.apply(e);
            }
        }

        public List<? extends X509Certificate> getX509CertificateChain() {
            try {
                final Certificate[] certificates = store.getCertificateChain(alias);
                if (certificates == null || certificates.length == 0) {
                    return List.of();
                }
                return Stream.of(certificates)
                    .filter(c -> c instanceof X509Certificate)
                    .map(X509Certificate.class::cast)
                    .collect(Collectors.toUnmodifiableList());
            } catch (KeyStoreException e) {
                throw exceptionHandler.apply(e);
            }
        }

    }


}
