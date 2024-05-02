/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.logstashbridge.script;

import org.elasticsearch.logstashbridge.StableAPI;
import org.elasticsearch.logstashbridge.common.Settings;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.painless.PainlessScriptEngine;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.script.IngestConditionalScript;
import org.elasticsearch.script.IngestScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.mustache.MustacheScriptEngine;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

public class ScriptService extends StableAPI.Proxy<org.elasticsearch.script.ScriptService> implements Closeable {
    public ScriptService wrap(org.elasticsearch.script.ScriptService delegate) {
        return new ScriptService(delegate);
    }

    public ScriptService(Settings settings, LongSupplier timeProvider) {
        super(getScriptService(settings.unwrap(), timeProvider));
    }

    public ScriptService(org.elasticsearch.script.ScriptService delegate) {
        super(delegate);
    }

    private static org.elasticsearch.script.ScriptService getScriptService(
        org.elasticsearch.common.settings.Settings settings,
        LongSupplier timeProvider
    ) {
        final List<Whitelist> painlessBaseWhitelist = getPainlessBaseWhiteList();
        final Map<ScriptContext<?>, List<Whitelist>> scriptContexts = Map.of(
            IngestScript.CONTEXT,
            painlessBaseWhitelist,
            IngestConditionalScript.CONTEXT,
            painlessBaseWhitelist
        );
        final Map<String, ScriptEngine> scriptEngines = Map.of(
            PainlessScriptEngine.NAME,
            new PainlessScriptEngine(settings, scriptContexts),
            MustacheScriptEngine.NAME,
            new MustacheScriptEngine()
        );
        return new org.elasticsearch.script.ScriptService(settings, scriptEngines, ScriptModule.CORE_CONTEXTS, timeProvider);
    }

    private static List<Whitelist> getPainlessBaseWhiteList() {
        return PainlessPlugin.baseWhiteList();
    }

    @Override
    public void close() throws IOException {
        this.delegate.close();
    }
}
