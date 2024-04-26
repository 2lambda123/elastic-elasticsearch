/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.logstashbridge.threadpool;

import org.elasticsearch.logstashbridge.StableAPI;
import org.elasticsearch.logstashbridge.common.Settings;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.concurrent.TimeUnit;

public class ThreadPool extends StableAPI.Proxy<org.elasticsearch.threadpool.ThreadPool> {

    public ThreadPool(Settings settings) {
        this(new org.elasticsearch.threadpool.ThreadPool(settings.unwrap(), MeterRegistry.NOOP));
    }

    public ThreadPool(org.elasticsearch.threadpool.ThreadPool delegate) {
        super(delegate);
    }

    public static boolean terminate(ThreadPool pool, long timeout, TimeUnit timeUnit) {
        return org.elasticsearch.threadpool.ThreadPool.terminate(pool.unwrap(), timeout, timeUnit);
    }

    public long relativeTimeInMillis() {
        return delegate.relativeTimeInMillis();
    }

    public long absoluteTimeInMillis() {
        return delegate.absoluteTimeInMillis();
    }
}
