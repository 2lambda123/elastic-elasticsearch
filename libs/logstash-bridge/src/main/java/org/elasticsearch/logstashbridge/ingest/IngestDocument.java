/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.logstashbridge.ingest;

import org.elasticsearch.ingest.LogstashInternalBridge;
import org.elasticsearch.logstashbridge.StableAPI;
import org.elasticsearch.logstashbridge.script.Metadata;
import org.elasticsearch.logstashbridge.script.TemplateScript;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public class IngestDocument extends StableAPI.Proxy<org.elasticsearch.ingest.IngestDocument> {

    public static String INGEST_KEY = org.elasticsearch.ingest.IngestDocument.INGEST_KEY;

    public static IngestDocument wrap(org.elasticsearch.ingest.IngestDocument ingestDocument) {
        if (ingestDocument == null) {
            return null;
        }
        return new IngestDocument(ingestDocument);
    }

    public IngestDocument(Map<String, Object> sourceAndMetadata, Map<String, Object> ingestMetadata) {
        this(new org.elasticsearch.ingest.IngestDocument(sourceAndMetadata, ingestMetadata));
    }

    private IngestDocument(org.elasticsearch.ingest.IngestDocument inner) {
        super(inner);
    }

    public Metadata getMetadata() {
        return new Metadata(delegate.getMetadata());
    }

    public Map<String, Object> getSource() {
        return delegate.getSource();
    }

    public boolean updateIndexHistory(String index) {
        return delegate.updateIndexHistory(index);
    }

    public Set<String> getIndexHistory() {
        return Set.copyOf(delegate.getIndexHistory());
    }

    public boolean isReroute() {
        return LogstashInternalBridge.isReroute(delegate);
    }

    public void resetReroute() {
        LogstashInternalBridge.resetReroute(delegate);
    }

    public Map<String, Object> getIngestMetadata() {
        return Map.copyOf(delegate.getIngestMetadata());
    }

    public <T> T getFieldValue(String fieldName, Class<T> type) {
        return delegate.getFieldValue(fieldName, type);
    }

    public <T> T getFieldValue(String fieldName, Class<T> type, boolean ignoreMissing) {
        return delegate.getFieldValue(fieldName, type, ignoreMissing);
    }

    public String renderTemplate(TemplateScript.Factory templateScriptFactory) {
        return delegate.renderTemplate(templateScriptFactory.unwrap());
    }

    public void setFieldValue(String path, Object value) {
        delegate.setFieldValue(path, value);
    }

    public void removeField(String path) {
        delegate.removeField(path);
    }

    // public void executePipeline(Pipeline pipeline, BiConsumer<IngestDocument, Exception> handler) {
    public void executePipeline(Pipeline pipeline, BiConsumer<IngestDocument, Exception> handler) {
        this.delegate.executePipeline(pipeline.unwrap(), (unwrapped, e) -> { handler.accept(IngestDocument.wrap(unwrapped), e); });
    }
}
