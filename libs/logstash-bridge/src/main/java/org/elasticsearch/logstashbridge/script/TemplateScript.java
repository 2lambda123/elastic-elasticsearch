/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.logstashbridge.script;

import org.elasticsearch.logstashbridge.StableAPI;

public class TemplateScript {
    public static class Factory extends StableAPI.Proxy<org.elasticsearch.script.TemplateScript.Factory> {
        public static Factory wrap(org.elasticsearch.script.TemplateScript.Factory delegate) {
            return new Factory(delegate);
        }

        public Factory(org.elasticsearch.script.TemplateScript.Factory delegate) {
            super(delegate);
        }

        @Override
        public org.elasticsearch.script.TemplateScript.Factory unwrap() {
            return this.delegate;
        }
    }
}
