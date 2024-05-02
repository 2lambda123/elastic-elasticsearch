/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.logstashbridge.ingest;

import org.elasticsearch.logstashbridge.StableAPI;
import org.elasticsearch.logstashbridge.script.ScriptService;

import java.util.Map;
import java.util.function.BiConsumer;

public class Pipeline extends StableAPI.Proxy<org.elasticsearch.ingest.Pipeline> {
    public static Pipeline wrap(org.elasticsearch.ingest.Pipeline pipeline) {
        return new Pipeline(pipeline);
    }

    public static Pipeline create(
        String id,
        Map<String, Object> config,
        Map<String, Processor.Factory> processorFactories,
        ScriptService scriptService
    ) throws Exception {
        return wrap(
            org.elasticsearch.ingest.Pipeline.create(
                id,
                config,
                StableAPI.unwrap(processorFactories),
                StableAPI.unwrapNullable(scriptService)
            )
        );
    }

    public Pipeline(org.elasticsearch.ingest.Pipeline delegate) {
        super(delegate);
    }

    public String getId() {
        return delegate.getId();
    }

    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        this.delegate.execute(
            StableAPI.unwrapNullable(ingestDocument),
            (unwrapped, e) -> { handler.accept(IngestDocument.wrap(unwrapped), e); }
        );
    }
}
