/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.analysis;

import org.elasticsearch.example.analysis.lucene.ReplaceCharToNumber;
import org.elasticsearch.plugin.analysis.api.CharFilterFactory;
import org.elasticsearch.plugin.api.NamedComponent;
import org.elasticsearch.plugin.api.settings.InjectSettings;

import java.io.Reader;

@NamedComponent( "example_char_filter")
public class ExampleCharFilterFactory implements CharFilterFactory {
    private final String oldChar;
    private final int newNumber;

    @InjectSettings
    public ExampleCharFilterFactory(ExampleAnalysisSettings settings) {
        oldChar = settings.oldChar();
        newNumber = settings.newNumber();
    }

    @Override
    public Reader create(Reader reader) {
        return new ReplaceCharToNumber(reader, oldChar, newNumber);
    }
}


