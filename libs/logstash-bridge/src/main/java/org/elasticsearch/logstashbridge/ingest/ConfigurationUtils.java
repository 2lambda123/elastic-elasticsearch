/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.logstashbridge.ingest;

import org.elasticsearch.logstashbridge.script.ScriptService;
import org.elasticsearch.logstashbridge.script.TemplateScript;

import java.util.Map;

public class ConfigurationUtils {
    public static TemplateScript.Factory compileTemplate(
        String processorType,
        String processorTag,
        String propertyName,
        String propertyValue,
        ScriptService scriptService
    ) {
        return new TemplateScript.Factory(
            org.elasticsearch.ingest.ConfigurationUtils.compileTemplate(
                processorType,
                processorTag,
                propertyName,
                propertyValue,
                scriptService.unwrap()
            )
        );
    }

    public static String readStringProperty(
        String processorType,
        String processorTag,
        Map<String, Object> configuration,
        String propertyName
    ) {
        return org.elasticsearch.ingest.ConfigurationUtils.readStringProperty(processorType, processorTag, configuration, propertyName);
    }

    public static Boolean readBooleanProperty(
        String processorType,
        String processorTag,
        Map<String, Object> configuration,
        String propertyName,
        boolean defaultValue
    ) {
        return org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty(
            processorType,
            processorTag,
            configuration,
            propertyName,
            defaultValue
        );
    }
}
