/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.logstashbridge.env;

import org.elasticsearch.logstashbridge.StableAPI;
import org.elasticsearch.logstashbridge.common.Settings;

import java.nio.file.Path;

public class Environment extends StableAPI.Proxy<org.elasticsearch.env.Environment> {
    public static Environment wrap(org.elasticsearch.env.Environment delegate) {
        return new Environment(delegate);
    }

    public Environment(Settings settings, Path configPath) {
        this(new org.elasticsearch.env.Environment(settings.unwrap(), configPath));
    }

    private Environment(org.elasticsearch.env.Environment delegate) {
        super(delegate);
    }

    @Override
    public org.elasticsearch.env.Environment unwrap() {
        return this.delegate;
    }
}
