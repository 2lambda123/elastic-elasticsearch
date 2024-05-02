/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.logstashbridge.plugins;

import org.elasticsearch.logstashbridge.StableAPI;
import org.elasticsearch.logstashbridge.ingest.Processor;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public interface IngestPlugin {
    Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters);

    static Wrapped wrap(final org.elasticsearch.plugins.IngestPlugin delegate) {
        return new Wrapped(delegate);
    }

    class Wrapped extends StableAPI.Proxy<org.elasticsearch.plugins.IngestPlugin> implements IngestPlugin, Closeable {

        private Wrapped(final org.elasticsearch.plugins.IngestPlugin delegate) {
            super(delegate);
        }

        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            return StableAPI.wrap(this.delegate.getProcessors(parameters.unwrap()), Processor.Factory::wrap);
        }

        @Override
        public org.elasticsearch.plugins.IngestPlugin unwrap() {
            return this.delegate;
        }

        @Override
        public void close() throws IOException {
            if (this.delegate instanceof Closeable closeableDelegate) {
                closeableDelegate.close();
            }
        }
    }
}
