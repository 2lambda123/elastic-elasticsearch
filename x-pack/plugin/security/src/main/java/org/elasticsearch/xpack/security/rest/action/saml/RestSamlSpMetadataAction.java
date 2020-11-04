/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.rest.action.saml;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.security.action.saml.SamlSpMetadataAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlSpMetadataRequest;
import org.elasticsearch.xpack.core.security.action.saml.SamlSpMetadataResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestSamlSpMetadataAction extends SamlBaseRestHandler {

    public RestSamlSpMetadataAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(
            new Route(GET, "/_security/saml/metadata/{realm}"));
    }

    @Override
    public String getName() {
        return "security_saml_metadata_action";
    }

    @Override
    public RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final SamlSpMetadataRequest SamlSpRequest = new SamlSpMetadataRequest(request.param("realm"));
        return channel -> client.execute(SamlSpMetadataAction.INSTANCE, SamlSpRequest,
            new RestBuilderListener<SamlSpMetadataResponse>(channel) {
            @Override
            public RestResponse buildResponse(SamlSpMetadataResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field("metadata", response.getXMLString());
                builder.endObject();
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }
}
