/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.MemoizedSupplier;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.io.IOException;

public class DlsFlsRequestCacheDifferentiator implements CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> {

    private static final Logger logger = LogManager.getLogger(DlsFlsRequestCacheDifferentiator.class);

    private final XPackLicenseState licenseState;
    private final SetOnce<ThreadContext> threadContextHolder;

    public DlsFlsRequestCacheDifferentiator(XPackLicenseState licenseState, SetOnce<ThreadContext> threadContextHolder) {
        this.licenseState = licenseState;
        this.threadContextHolder = threadContextHolder;
    }

    @Override
    public void accept(ShardSearchRequest request, StreamOutput out) throws IOException {
        if (false == licenseState.isSecurityEnabled()) {
            return;
        }
        var licenseChecker = new MemoizedSupplier<>(() -> licenseState.checkFeature(XPackLicenseState.Feature.SECURITY_DLS_FLS));
        final IndicesAccessControl indicesAccessControl =
            threadContextHolder.get().getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
        final String indexName = request.shardId().getIndexName();
        IndicesAccessControl.IndexAccessControl indexAccessControl = indicesAccessControl.getIndexPermissions(indexName);
        if (indexAccessControl != null) {
            final boolean flsEnabled = indexAccessControl.getFieldPermissions().hasFieldLevelSecurity();
            final boolean dlsEnabled = indexAccessControl.getDocumentPermissions().hasDocumentLevelPermissions();
            if ((flsEnabled || dlsEnabled) && licenseChecker.get()) {
                logger.debug("index [{}] with field level access controls [{}] " +
                        "document level access controls [{}]. Differentiating request cache key",
                    indexName, flsEnabled, dlsEnabled);
                indexAccessControl.writeCacheKey(out);
            }
        }
    }
}
