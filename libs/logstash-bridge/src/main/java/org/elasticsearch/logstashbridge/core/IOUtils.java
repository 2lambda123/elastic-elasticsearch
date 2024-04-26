/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.logstashbridge.core;

import java.io.Closeable;

public class IOUtils {
    public static void closeWhileHandlingException(Iterable<? extends Closeable> objects) {
        org.elasticsearch.core.IOUtils.closeWhileHandlingException(objects);
    }

    public static void closeWhileHandlingException(Closeable closeable) {
        org.elasticsearch.core.IOUtils.closeWhileHandlingException(closeable);
    }
}
