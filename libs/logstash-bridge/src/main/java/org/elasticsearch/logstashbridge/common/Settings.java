/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.logstashbridge.common;

import org.elasticsearch.logstashbridge.StableAPI;

public class Settings extends StableAPI.Proxy<org.elasticsearch.common.settings.Settings> {

    public static Settings wrap(org.elasticsearch.common.settings.Settings delegate) {
        return new Settings(delegate);
    }

    public static Builder builder() {
        return Builder.wrap(org.elasticsearch.common.settings.Settings.builder());
    }

    public Settings(org.elasticsearch.common.settings.Settings delegate) {
        super(delegate);
    }

    @Override
    public org.elasticsearch.common.settings.Settings unwrap() {
        return this.delegate;
    }

    public static class Builder extends StableAPI.Proxy<org.elasticsearch.common.settings.Settings.Builder> {
        static Builder wrap(org.elasticsearch.common.settings.Settings.Builder delegate) {
            return new Builder(delegate);
        }

        private Builder(org.elasticsearch.common.settings.Settings.Builder delegate) {
            super(delegate);
        }

        public Builder put(String key, String value) {
            this.delegate.put(key, value);
            return this;
        }

        public Settings build() {
            return new Settings(this.delegate.build());
        }
    }
}
