/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;

/**
 * An internal plugin that will return a DocumentParsingObserverFactory.
 */
public interface DocumentParsingObserverPlugin {

    /**
     * @return a documentParsingObserverFactory to allow observing parsing events
     */
    DocumentParsingObserverFactory getDocumentParsingObserverSupplier();
}
