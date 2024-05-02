/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.logstashbridge.ingest;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.logstashbridge.StableAPI;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;

public class PipelineConfiguration extends StableAPI.Proxy<org.elasticsearch.ingest.PipelineConfiguration> {
    public PipelineConfiguration(org.elasticsearch.ingest.PipelineConfiguration delegate) {
        super(delegate);
    }

    public PipelineConfiguration(final String pipelineId, final String jsonEncodedConfig) {
        this(new org.elasticsearch.ingest.PipelineConfiguration(pipelineId, new BytesArray(jsonEncodedConfig), XContentType.JSON));
    }

    public String getId() {
        return delegate.getId();
    }

    public Map<String, Object> getConfigAsMap() {
        return delegate.getConfigAsMap();
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj instanceof PipelineConfiguration other) {
            return delegate.equals(other.delegate);
        } else {
            return false;
        }
    }
}
