/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.ml.dataframe.analyses.DataFrameAnalysesUtils;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;

import java.io.IOException;
import java.util.function.Supplier;

public class TransportPutDataFrameAnalyticsAction
    extends HandledTransportAction<PutDataFrameAnalyticsAction.Request, PutDataFrameAnalyticsAction.Response> {

    private final XPackLicenseState licenseState;
    private final DataFrameAnalyticsConfigProvider configProvider;
    private final ThreadPool threadPool;
    private final SecurityContext securityContext;
    private final Client client;

    @Inject
    public TransportPutDataFrameAnalyticsAction(Settings settings, TransportService transportService, ActionFilters actionFilters,
                                                XPackLicenseState licenseState, Client client, ThreadPool threadPool,
                                                DataFrameAnalyticsConfigProvider configProvider) {
        super(PutDataFrameAnalyticsAction.NAME, transportService, actionFilters,
            (Supplier<PutDataFrameAnalyticsAction.Request>) PutDataFrameAnalyticsAction.Request::new);
        this.licenseState = licenseState;
        this.configProvider = configProvider;
        this.threadPool = threadPool;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings) ?
            new SecurityContext(settings, threadPool.getThreadContext()) : null;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, PutDataFrameAnalyticsAction.Request request,
                             ActionListener<PutDataFrameAnalyticsAction.Response> listener) {
        if (licenseState.isMachineLearningAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
            return;
        }
        validateConfig(request.getConfig());
        if (licenseState.isAuthAllowed()) {
            final String username = securityContext.getUser().principal();
            RoleDescriptor.IndicesPrivileges sourceIndexPrivileges = RoleDescriptor.IndicesPrivileges.builder()
                .indices(request.getConfig().getSource())
                .privileges("read")
                .build();
            RoleDescriptor.IndicesPrivileges destIndexPrivileges = RoleDescriptor.IndicesPrivileges.builder()
                .indices(request.getConfig().getDest())
                .privileges("read", "index", "create_index")
                .build();

            HasPrivilegesRequest privRequest = new HasPrivilegesRequest();
            privRequest.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
            privRequest.username(username);
            privRequest.clusterPrivileges(Strings.EMPTY_ARRAY);
            privRequest.indexPrivileges(sourceIndexPrivileges, destIndexPrivileges);

            ActionListener<HasPrivilegesResponse> privResponseListener = ActionListener.wrap(
                r -> handlePrivsResponse(username, request, r, listener),
                listener::onFailure);

            client.execute(HasPrivilegesAction.INSTANCE, privRequest, privResponseListener);
        } else {
            configProvider.put(request.getConfig(), threadPool.getThreadContext().getHeaders(), ActionListener.wrap(
                indexResponse -> listener.onResponse(new PutDataFrameAnalyticsAction.Response(request.getConfig())),
                listener::onFailure
            ));
        }
    }

    private void handlePrivsResponse(String username, PutDataFrameAnalyticsAction.Request request,
                                     HasPrivilegesResponse response,
                                     ActionListener<PutDataFrameAnalyticsAction.Response> listener) throws IOException {
        if (response.isCompleteMatch()) {
            configProvider.put(request.getConfig(), threadPool.getThreadContext().getHeaders(), ActionListener.wrap(
                indexResponse -> listener.onResponse(new PutDataFrameAnalyticsAction.Response(request.getConfig())),
                listener::onFailure
            ));
        } else {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            for (ResourcePrivileges index : response.getIndexPrivileges()) {
                builder.field(index.getResource());
                builder.map(index.getPrivileges());
            }
            builder.endObject();

            listener.onFailure(Exceptions.authorizationError("Cannot create data frame analytics [{}]" +
                    " because user {} lacks permissions on the indices: {}",
                request.getConfig().getId(), username, Strings.toString(builder)));
        }
    }

    private void validateConfig(DataFrameAnalyticsConfig config) {
        if (MlStrings.isValidId(config.getId()) == false) {
            throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.INVALID_ID, DataFrameAnalyticsConfig.ID,
                config.getId()));
        }
        if (!MlStrings.hasValidLengthForId(config.getId())) {
            throw ExceptionsHelper.badRequestException("id [{}] is too long; must not contain more than {} characters", config.getId(),
                MlStrings.ID_LENGTH_LIMIT);
        }
        if (config.getSource().isEmpty()) {
            throw ExceptionsHelper.badRequestException("[{}] must be non-empty", DataFrameAnalyticsConfig.SOURCE);
        }
        if (config.getDest().isEmpty()) {
            throw ExceptionsHelper.badRequestException("[{}] must be non-empty", DataFrameAnalyticsConfig.DEST);
        }
        DataFrameAnalysesUtils.readAnalyses(config.getAnalyses());
    }
}
