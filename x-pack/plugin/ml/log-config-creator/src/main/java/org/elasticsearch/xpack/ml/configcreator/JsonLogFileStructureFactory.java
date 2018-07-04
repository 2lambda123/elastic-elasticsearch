/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.Terminal.Verbosity;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.io.StringReader;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.json.JsonXContent.jsonXContent;

public class JsonLogFileStructureFactory implements LogFileStructureFactory {

    private final Terminal terminal;

    public JsonLogFileStructureFactory(Terminal terminal) {
        this.terminal = Objects.requireNonNull(terminal);
    }

    /**
     * This format matches if the sample consists of one or more JSON documents.
     * If there is more than one, they must be newline-delimited.  The
     * documents must be non-empty, to prevent lines containing "{}" from matching.
     */
    @Override
    public boolean canCreateFromSample(String sample) {

        int completeDocCount = 0;

        try {
            String[] sampleLines = sample.split("\n");
            for (String sampleLine : sampleLines) {
                try (XContentParser parser = jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION, new ContextPrintingStringReader(sampleLine))) {

                    if (parser.map().isEmpty()) {
                        terminal.println(Verbosity.VERBOSE, "Not JSON because an empty object was parsed: [" + sampleLine + "]");
                        return false;
                    }
                    ++completeDocCount;
                    if (parser.nextToken() != null) {
                        terminal.println(Verbosity.VERBOSE,
                            "Not newline delimited JSON because a line contained more than a single object: [" + sampleLine + "]");
                        return false;
                    }
                }
            }
        } catch (IOException | IllegalStateException e) {
            terminal.println(Verbosity.VERBOSE, "Not JSON because there was a parsing exception: [" +
                e.getMessage().replaceAll("\\s?\r?\n\\s?", " ") + "]");
            return false;
        }

        if (completeDocCount == 0) {
            terminal.println(Verbosity.VERBOSE, "Not JSON because sample didn't contain a complete document");
            return false;
        }

        terminal.println(Verbosity.VERBOSE, "Deciding sample is newline delimited JSON");
        return true;
    }

    @Override
    public LogFileStructure createFromSample(String sampleFileName, String indexName, String typeName, String logstashFileTimezone,
                                             String sample, String charsetName) throws IOException {
        return new JsonLogFileStructure(terminal, sampleFileName, indexName, typeName, logstashFileTimezone, sample, charsetName);
    }

    private static class ContextPrintingStringReader extends StringReader {

        private final String str;

        ContextPrintingStringReader(String str) {
            super(str);
            this.str = str;
        }

        @Override
        public String toString() {
            if (str.length() <= 80) {
                return String.format(Locale.ROOT, "\"%s\"", str);
            } else {
                return String.format(Locale.ROOT, "\"%.77s...\"", str);
            }
        }
    }
}
