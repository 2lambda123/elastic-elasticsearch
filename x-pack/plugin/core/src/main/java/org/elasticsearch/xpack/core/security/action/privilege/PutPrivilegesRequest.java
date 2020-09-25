/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;
import org.elasticsearch.xpack.core.security.xcontent.XContentUtils.AuditToXContentParams;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request object to put a one or more application privileges.
 */
public final class PutPrivilegesRequest extends ActionRequest implements ApplicationPrivilegesRequest, WriteRequest<PutPrivilegesRequest>
        , ToXContentObject {

    private List<ApplicationPrivilegeDescriptor> privileges;
    private RefreshPolicy refreshPolicy = RefreshPolicy.IMMEDIATE;

    public PutPrivilegesRequest(StreamInput in) throws IOException {
        super(in);
        privileges = Collections.unmodifiableList(in.readList(ApplicationPrivilegeDescriptor::new));
        refreshPolicy = RefreshPolicy.readFrom(in);
    }

    public PutPrivilegesRequest() {
        privileges = Collections.emptyList();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (privileges.isEmpty()) {
            validationException = addValidationError("At least one application privilege must be provided", validationException);
        } else {
            for (ApplicationPrivilegeDescriptor privilege : privileges) {
                try {
                    ApplicationPrivilege.validateApplicationName(privilege.getApplication());
                } catch (IllegalArgumentException e) {
                    validationException = addValidationError(e.getMessage(), validationException);
                }
                try {
                    ApplicationPrivilege.validatePrivilegeName(privilege.getName());
                } catch (IllegalArgumentException e) {
                    validationException = addValidationError(e.getMessage(), validationException);
                }
                if (privilege.getActions().isEmpty()) {
                    validationException = addValidationError("Application privileges must have at least one action", validationException);
                }
                for (String action : privilege.getActions()) {
                    if (action.indexOf('/') == -1 && action.indexOf('*') == -1 && action.indexOf(':') == -1) {
                        validationException = addValidationError("action [" + action + "] must contain one of [ '/' , '*' , ':' ]",
                            validationException);
                    }
                    try {
                        ApplicationPrivilege.validatePrivilegeOrActionName(action);
                    } catch (IllegalArgumentException e) {
                        validationException = addValidationError(e.getMessage(), validationException);
                    }
                }
                if (MetadataUtils.containsReservedMetadata(privilege.getMetadata())) {
                    validationException = addValidationError("metadata keys may not start with [" + MetadataUtils.RESERVED_PREFIX
                        + "] (in privilege " + privilege.getApplication() + ' ' + privilege.getName() + ")", validationException);
                }
            }
        }
        return validationException;
    }

    /**
     * Should this request trigger a refresh ({@linkplain RefreshPolicy#IMMEDIATE}, the default), wait for a refresh (
     * {@linkplain RefreshPolicy#WAIT_UNTIL}), or proceed ignore refreshes entirely ({@linkplain RefreshPolicy#NONE}).
     */
    @Override
    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public PutPrivilegesRequest setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    public List<ApplicationPrivilegeDescriptor> getPrivileges() {
        return privileges;
    }

    public void setPrivileges(Collection<ApplicationPrivilegeDescriptor> privileges) {
        this.privileges = List.copyOf(privileges);
    }

    @Override
    public Collection<String> getApplicationNames() {
        return Collections.unmodifiableSet(privileges.stream()
            .map(ApplicationPrivilegeDescriptor::getApplication)
            .collect(Collectors.toSet()));
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{[" + privileges.stream().map(Strings::toString).collect(Collectors.joining(","))
            + "];" + refreshPolicy + "}";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(privileges);
        refreshPolicy.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
                .field("privileges", privileges);
        if (params.paramAsBoolean(AuditToXContentParams.INCLUDE_REFRESH_POLICY, false)) {
            builder.field("refresh_policy", refreshPolicy.toString());
        }
        return builder.endObject();
    }
}
