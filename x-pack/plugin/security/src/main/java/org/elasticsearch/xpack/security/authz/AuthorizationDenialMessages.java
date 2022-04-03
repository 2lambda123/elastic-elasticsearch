/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.RestrictedIndices;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;

import java.util.Collection;

import static org.elasticsearch.common.Strings.collectionToCommaDelimitedString;
import static org.elasticsearch.xpack.security.authz.AuthorizationService.isIndexAction;

class AuthorizationDenialMessages {

    private AuthorizationDenialMessages() {}

    static String runAsDenied(Authentication authentication) {
        String userText = authenticatedUserText(authentication);

        String runAsUserText = authentication.getUser().isRunAs() ? authentication.getUser().principal() : "";

        return userText + " is unauthorized to run as [" + runAsUserText + "]";
    }

    static String indexActionDenied(
        Authentication authentication,
        String action,
        TransportRequest request,
        Collection<String> deniedIndices,
        RestrictedIndices restrictedNames
    ) {
        return actionDenied(
            authentication,
            action,
            request,
            AuthorizationEngine.IndexAuthorizationResult.getFailureDescription(deniedIndices, restrictedNames)
        );
    }

    static String requiresOperatorPrivileges(Authentication authentication, String action, TransportRequest request) {
        return actionDenied(authentication, action, request, "because it requires operator privileges");
    }

    static String actionDenied(Authentication authentication, String action, TransportRequest request) {
        return actionDenied(authentication, action, request, null);
    }

    static String actionDenied(Authentication authentication, String action, TransportRequest request, @Nullable String context) {
        String userText = authenticatedUserText(authentication);

        if (authentication.getUser().isRunAs()) {
            userText = userText + " run as [" + authentication.getUser().principal() + "]";
        }

        // The run-as user is always from a realm. So it must have roles that can be printed.
        // If the user is not run-as, we cannot print the roles if it's an API key or a service account (both do not have
        // roles, but privileges)
        if (false == authentication.isServiceAccount() && false == authentication.isApiKey()) {
            userText = userText + " with roles [" + Strings.arrayToCommaDelimitedString(authentication.getUser().roles()) + "]";
        }

        String message = "action [" + action + "] is unauthorized for " + userText;
        if (context != null) {
            message = message + " " + context;
        }

        if (ClusterPrivilegeResolver.isClusterAction(action)) {
            final Collection<String> privileges = ClusterPrivilegeResolver.findPrivilegesThatGrant(action, request, authentication);
            if (privileges != null && privileges.size() > 0) {
                message = message
                    + ", this action is granted by the cluster privileges ["
                    + collectionToCommaDelimitedString(privileges)
                    + "]";
            }
        } else if (isIndexAction(action)) {
            final Collection<String> privileges = IndexPrivilege.findPrivilegesThatGrant(action);
            if (privileges != null && privileges.size() > 0) {
                message = message
                    + ", this action is granted by the index privileges ["
                    + collectionToCommaDelimitedString(privileges)
                    + "]";
            }
        }

        return message;
    }

    private static String authenticatedUserText(Authentication authentication) {
        String userText = (authentication.isAuthenticatedWithServiceAccount() ? "service account" : "user")
            + " ["
            + authentication.getUser().authenticatedUser().principal()
            + "]";
        if (authentication.isAuthenticatedAsApiKey()) {
            final String apiKeyId = (String) authentication.getMetadata().get(AuthenticationField.API_KEY_ID_KEY);
            assert apiKeyId != null : "api key id must be present in the metadata";
            userText = "API key id [" + apiKeyId + "] of " + userText;
        }
        return userText;
    }

}
