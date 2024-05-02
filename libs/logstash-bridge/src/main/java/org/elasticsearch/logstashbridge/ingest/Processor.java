/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.logstashbridge.ingest;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.logstashbridge.StableAPI;
import org.elasticsearch.logstashbridge.env.Environment;
import org.elasticsearch.logstashbridge.script.ScriptService;
import org.elasticsearch.logstashbridge.threadpool.ThreadPool;

import java.util.Map;
import java.util.function.BiConsumer;

public interface Processor extends StableAPI<org.elasticsearch.ingest.Processor> {
    String getType();

    String getTag();

    String getDescription();

    boolean isAsync();

    void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) throws Exception;

    static Processor wrap(org.elasticsearch.ingest.Processor delegate) {
        return new Wrapped(delegate);
    }

    class Wrapped extends StableAPI.Proxy<org.elasticsearch.ingest.Processor> implements Processor {
        public Wrapped(org.elasticsearch.ingest.Processor delegate) {
            super(delegate);
        }

        @Override
        public String getType() {
            return unwrap().getType();
        }

        @Override
        public String getTag() {
            return unwrap().getTag();
        }

        @Override
        public String getDescription() {
            return unwrap().getDescription();
        }

        @Override
        public boolean isAsync() {
            return unwrap().isAsync();
        }

        @Override
        public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) throws Exception {
            delegate.execute(StableAPI.unwrapNullable(ingestDocument), (id, e) -> handler.accept(IngestDocument.wrap(id), e));
        }
    }

    class Parameters extends StableAPI.Proxy<org.elasticsearch.ingest.Processor.Parameters> {

        public Parameters(Environment environment, ScriptService scriptService, ThreadPool threadPool) {
            this(
                new org.elasticsearch.ingest.Processor.Parameters(
                    environment.unwrap(),
                    scriptService.unwrap(),
                    null,
                    threadPool.unwrap().getThreadContext(),
                    threadPool.unwrap()::relativeTimeInMillis,
                    (delay, command) -> threadPool.unwrap()
                        .schedule(command, TimeValue.timeValueMillis(delay), threadPool.unwrap().generic()),
                    null,
                    null,
                    threadPool.unwrap().generic()::execute,
                    IngestService.createGrokThreadWatchdog(environment.unwrap(), threadPool.unwrap())
                )
            );
        }

        private Parameters(org.elasticsearch.ingest.Processor.Parameters delegate) {
            super(delegate);
        }

        @Override
        public org.elasticsearch.ingest.Processor.Parameters unwrap() {
            return this.delegate;
        }
    }

    interface Factory extends StableAPI<org.elasticsearch.ingest.Processor.Factory> {
        Processor create(Map<String, Processor.Factory> registry, String processorTag, String description, Map<String, Object> config)
            throws Exception;

        static Factory wrap(org.elasticsearch.ingest.Processor.Factory delegate) {
            return new Wrapped(delegate);
        }

        @Override
        default org.elasticsearch.ingest.Processor.Factory unwrap() {
            final Factory stableAPIFactory = this;
            return (registry, tag, description, config) -> stableAPIFactory.create(
                StableAPI.wrap(registry, Factory::wrap),
                tag,
                description,
                config
            ).unwrap();
        }

        class Wrapped extends StableAPI.Proxy<org.elasticsearch.ingest.Processor.Factory> implements Factory {
            private Wrapped(org.elasticsearch.ingest.Processor.Factory delegate) {
                super(delegate);
            }

            @Override
            public Processor create(Map<String, Factory> registry, String processorTag, String description, Map<String, Object> config)
                throws Exception {
                return Processor.wrap(this.delegate.create(StableAPI.unwrap(registry), processorTag, description, config));
            }

            @Override
            public org.elasticsearch.ingest.Processor.Factory unwrap() {
                return this.delegate;
            }
        }
    }

}
