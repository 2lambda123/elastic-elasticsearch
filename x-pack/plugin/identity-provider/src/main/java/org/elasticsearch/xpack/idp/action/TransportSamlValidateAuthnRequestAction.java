/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.env.Environment;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.idp.saml.authn.SamlAuthnRequestValidator;
import org.elasticsearch.xpack.idp.saml.idp.CloudIdp;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;

public class TransportSamlValidateAuthnRequestAction extends HandledTransportAction<SamlValidateAuthnRequestRequest,
    SamlValidateAuthnRequestResponse> {

    private final Environment env;

    @Inject
    public TransportSamlValidateAuthnRequestAction(TransportService transportService, ActionFilters actionFilters,
                                                   Environment environment) {
        super(SamlValidateAuthnRequestAction.NAME, transportService, actionFilters, SamlValidateAuthnRequestRequest::new);
        this.env = environment;
    }

    @Override
    protected void doExecute(Task task, SamlValidateAuthnRequestRequest request,
                             ActionListener<SamlValidateAuthnRequestResponse> listener) {
        final SamlIdentityProvider idp = new CloudIdp(env, env.settings());
        final SamlFactory samlFactory = new SamlFactory();
        final SamlAuthnRequestValidator validator = new SamlAuthnRequestValidator(samlFactory, idp);
        try {
            validator.processQueryString(request.getQueryString(), listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
