/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;

import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * An {@link AuthenticationToken} to hold JWT authentication related content.
 */
public class JwtAuthenticationToken implements AuthenticationToken {
    private static final Logger LOGGER = LogManager.getLogger(JwtAuthenticationToken.class);

    // Stored members
    protected SecureString endUserSignedJwt; // required
    protected SecureString clientAuthenticationSharedSecret; // optional, nullable
    protected String principal; // Defaults to "iss/aud/sub", with an ordered "aud" list

    /**
     * Store a mandatory JWT and optional Shared Secret. Parse the JWT, and extract the header, claims set, and signature.
     * Compute a token principal, for use as a realm order cache key. Default for OIDC ID Tokens is iss/aud/sub.
     * For other JWTs, the endUserIdClaimNames supports using other claims to replace sub.
     * Throws IllegalArgumentException if endUserIfClaimNames is empty, JWT is missing, or if JWT parsing fails.
     * @param endUserIdClaimNames Ordered list of string claims to use for endUserIdValue. The first claim in that list is used (ex: sub).
     * @param endUserSignedJwt Base64Url-encoded JWT for End-user authentication. Required by all JWT realms.
     * @param clientAuthenticationSharedSecret URL-safe Shared Secret for Client authentication. Required by some JWT realms.
     */
    public JwtAuthenticationToken(
        final List<String> endUserIdClaimNames,
        final SecureString endUserSignedJwt,
        @Nullable final SecureString clientAuthenticationSharedSecret
    ) {
        if (endUserIdClaimNames.isEmpty()) {
            throw new IllegalArgumentException("JWT token endUserId claim names list must be non-empty");
        } else if (endUserSignedJwt.isEmpty()) {
            throw new IllegalArgumentException("JWT bearer token must be non-empty");
        } else if ((clientAuthenticationSharedSecret != null) && (clientAuthenticationSharedSecret.isEmpty())) {
            throw new IllegalArgumentException("Client shared secret must be non-empty");
        }
        this.endUserSignedJwt = endUserSignedJwt; // required
        this.clientAuthenticationSharedSecret = clientAuthenticationSharedSecret; // optional, nullable

        JWTClaimsSet jwtClaimsSet;
        try {
            jwtClaimsSet = SignedJWT.parse(this.endUserSignedJwt.toString()).getJWTClaimsSet();
        } catch (ParseException e) {
            throw new IllegalArgumentException("Failed to parse JWT bearer token", e);
        }

        // get and validate iss and aud claims
        final String issuer = jwtClaimsSet.getIssuer();
        final List<String> audiences = jwtClaimsSet.getAudience();
        if (Strings.hasText(issuer) == false) {
            throw new IllegalArgumentException("Issuer claim 'iss' is missing.");
        } else if ((audiences == null) || (audiences.isEmpty())) {
            throw new IllegalArgumentException("Audiences claim 'aud' is missing.");
        }

        // get and validate sub claim, or the first configured backup claim (if sub is absent)
        final String endUserIdClaimValue = resolveEndUserId(jwtClaimsSet, endUserIdClaimNames);
        this.principal = issuer + "/" + String.join(",", new TreeSet<>(audiences)) + "/" + endUserIdClaimValue;
    }

    private String resolveEndUserId(final JWTClaimsSet jwtClaimsSet, final List<String> endUserIdClaimNames) {
        for (final String endUserIdClaimName : endUserIdClaimNames) {
            final Object claimValue = jwtClaimsSet.getClaim(endUserIdClaimName);
            if (claimValue instanceof String endUserIdClaimValue) {
                if (endUserIdClaimValue.isEmpty()) {
                    throw new IllegalArgumentException(
                        "User identifier claim '" + endUserIdClaimName + "' exists but cannot be used due to empty string value"
                    );
                }
                LOGGER.trace("Found endUserId claim name [{}] with value [{}]", endUserIdClaimName, endUserIdClaimValue);
                return endUserIdClaimValue;
            } else if (claimValue != null) {
                throw new IllegalArgumentException(
                    "User identifier claim '"
                        + endUserIdClaimName
                        + "' exists but cannot be used due to non-string value type '"
                        + claimValue.getClass().getCanonicalName()
                        + "'"
                );
            }
        }

        // at this point, none of the endUserIdClaimNames were found
        // throw an exception with a detailed log message about available claims with string values
        final String allClaimNamesWithStringValues = jwtClaimsSet.getClaims()
            .entrySet()
            .stream()
            .filter(e -> e.getValue() instanceof String)
            .map(Map.Entry::getKey)
            .collect(Collectors.joining(","));
        throw new IllegalArgumentException(
            "None of these end user identifier claims were found in the JWT Claims Set ["
                + String.join(",", endUserIdClaimNames)
                + "] - available claims with string values are ["
                + allClaimNamesWithStringValues
                + "]"
        );
    }

    @Override
    public String principal() {
        return this.principal;
    }

    @Override
    public SecureString credentials() {
        return null;
    }

    public SecureString getEndUserSignedJwt() {
        return this.endUserSignedJwt;
    }

    public SecureString getClientAuthenticationSharedSecret() {
        return this.clientAuthenticationSharedSecret;
    }

    @Override
    public void clearCredentials() {
        this.endUserSignedJwt.close();
        this.endUserSignedJwt = null;
        if (this.clientAuthenticationSharedSecret != null) {
            this.clientAuthenticationSharedSecret.close();
            this.clientAuthenticationSharedSecret = null;
        }
        this.principal = null;
    }

    @Override
    public String toString() {
        return JwtAuthenticationToken.class.getSimpleName() + "=" + this.principal;
    }
}
