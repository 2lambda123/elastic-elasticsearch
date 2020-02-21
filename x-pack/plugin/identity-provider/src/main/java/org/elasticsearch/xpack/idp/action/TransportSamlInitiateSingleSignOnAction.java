/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.idp.saml.authn.SuccessfulAuthenticationResponseMessageBuilder;
import org.elasticsearch.xpack.idp.saml.authn.UserServiceAuthentication;
import org.elasticsearch.xpack.idp.saml.idp.CloudIdp;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.elasticsearch.xpack.idp.saml.support.SamlUtils;
import org.opensaml.saml.saml2.core.Response;

import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TransportSamlInitiateSingleSignOnAction
    extends HandledTransportAction<SamlInitiateSingleSignOnRequest, SamlInitiateSingleSignOnResponse> {

    private final ThreadPool threadPool;
    private final Environment env;
    private final Logger logger = LogManager.getLogger(TransportSamlInitiateSingleSignOnAction.class);

    @Inject
    public TransportSamlInitiateSingleSignOnAction(ThreadPool threadPool, TransportService transportService,
                                                   ActionFilters actionFilters, Environment environment) {
        super(SamlInitiateSingleSignOnAction.NAME, transportService, actionFilters, SamlInitiateSingleSignOnRequest::new);
        this.threadPool = threadPool;
        this.env = environment;
    }

    @Override
    protected void doExecute(Task task, SamlInitiateSingleSignOnRequest request,
                             ActionListener<SamlInitiateSingleSignOnResponse> listener) {
        final ThreadContext threadContext = threadPool.getThreadContext();
        final SamlFactory samlFactory = new SamlFactory();
        final SamlIdentityProvider idp = new CloudIdp(env, env.settings());
        try {
            // TODO: Adjust this once secondary auth code is merged in master and use the authentication object of the user
            // Authentication authentication = authenticationService.getSecondaryAuth();
            Authentication serviceAccountAuthentication = new AuthenticationContextSerializer().readFromContext(threadContext);
            SamlServiceProvider sp = idp.getRegisteredServiceProvider(request.getSpEntityId());
            if (null == sp) {
                final String message =
                    "Service Provider with Entity ID [" + request.getSpEntityId() + "] is not registered with this Identity Provider";
                logger.debug(message);
                listener.onFailure(new IllegalArgumentException(message));
                return;
            }
            final UserServiceAuthentication user = buildUserFromAuthentication(serviceAccountAuthentication, sp);
            final SuccessfulAuthenticationResponseMessageBuilder builder = new SuccessfulAuthenticationResponseMessageBuilder(samlFactory,
                Clock.systemUTC(), idp);
            final Response response = builder.build(user, null);
            listener.onResponse(new SamlInitiateSingleSignOnResponse(user.getServiceProvider().getAssertionConsumerService().toString(),
                SamlUtils.getXmlContent(response),
                user.getServiceProvider().getEntityId()));
        } catch (IOException e) {
            listener.onFailure(new IllegalArgumentException(e.getMessage()));
        }
    }

    private UserServiceAuthentication buildUserFromAuthentication(Authentication authentication, SamlServiceProvider sp) {
        final User authenticatedUser = authentication.getUser();
        //TBD Where we will be sourcing the information from, use roles for easier testing now
        final Set<String> groups = new HashSet<>(Arrays.asList(authenticatedUser.roles()));
        return new UserServiceAuthentication(authenticatedUser.principal(), groups, sp);
    }
}
