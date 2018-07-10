/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.Terminal.Verbosity;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.Location;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Objects;

public class XmlLogFileStructureFactory implements LogFileStructureFactory {

    private final Terminal terminal;
    private final XMLInputFactory xmlFactory;

    public XmlLogFileStructureFactory(Terminal terminal) {
        this.terminal = Objects.requireNonNull(terminal);
        xmlFactory = XMLInputFactory.newInstance();
        xmlFactory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, Boolean.FALSE);
        xmlFactory.setProperty(XMLInputFactory.IS_VALIDATING, Boolean.FALSE);
    }

    /**
     * This format matches if the sample consists of one or more XML documents,
     * all with the same root element name.  If there is more than one document,
     * only whitespace is allowed in between them.  The last one does not
     * necessarily have to be complete (as the sample could have truncated it).
     */
    @Override
    public boolean canCreateFromSample(String sample) {

        int completeDocCount = 0;
        String commonRootElementName = null;
        String remainder = sample.trim();
        boolean mightBeAnotherDocument = !remainder.isEmpty();

        // This processing is extremely complicated because it's necessary
        // to create a new XML stream reader per document, but each one
        // will read ahead so will potentially consume characters from the
        // following document.  We must therefore also recreate the string
        // reader for each document.
        while (mightBeAnotherDocument) {

            try (Reader reader = new StringReader(remainder)) {

                XMLStreamReader xmlReader = xmlFactory.createXMLStreamReader(reader);
                try {
                    int nestingLevel = 0;
                    while ((mightBeAnotherDocument = xmlReader.hasNext())) {
                        switch (xmlReader.next()) {
                            case XMLStreamReader.START_ELEMENT:
                                if (nestingLevel++ == 0) {
                                    String rootElementName = xmlReader.getLocalName();
                                    if (commonRootElementName == null) {
                                        commonRootElementName = rootElementName;
                                    } else if (commonRootElementName.equals(rootElementName) == false) {
                                        terminal.println(Verbosity.VERBOSE, "Not XML because different documents have different root " +
                                            "element names: [" + commonRootElementName + "] and [" + rootElementName + "]");
                                        return false;
                                    }
                                }
                                break;
                            case XMLStreamReader.END_ELEMENT:
                                if (--nestingLevel < 0) {
                                    terminal.println(Verbosity.VERBOSE, "Not XML because an end element occurs before a start element");
                                    return false;
                                }
                                break;
                        }
                        if (nestingLevel == 0) {
                            ++completeDocCount;
                            // Find the position that's one character beyond end of the end element.
                            // The next document (if there is one) must start after this (possibly
                            // preceeded by whitespace).
                            Location location = xmlReader.getLocation();
                            int endPos = 0;
                            // Line and column numbers start at 1, not 0
                            for (int wholeLines = location.getLineNumber() - 1; wholeLines > 0; --wholeLines) {
                                endPos = remainder.indexOf('\n', endPos) + 1;
                                if (endPos == 0) {
                                    terminal.println(Verbosity.VERBOSE, "Not XML because XML parser location is inconsistent: line [" +
                                        location.getLineNumber() + "], column [" + location.getColumnNumber() + "] in [" + remainder + "]");
                                    return false;
                                }
                            }
                            endPos += location.getColumnNumber() - 1;
                            remainder = remainder.substring(endPos).trim();
                            mightBeAnotherDocument = !remainder.isEmpty();
                            break;
                        }
                    }
                } finally {
                    xmlReader.close();
                }
            } catch (IOException | XMLStreamException e) {
                terminal.println(Verbosity.VERBOSE, "Not XML because there was a parsing exception: [" +
                    e.getMessage().replaceAll("\\s?\r?\n\\s?", " ") + "]");
                return false;
            }
        }

        if (completeDocCount == 0) {
            terminal.println(Verbosity.VERBOSE, "Not XML because sample didn't contain a complete document");
            return false;
        }

        terminal.println(Verbosity.VERBOSE, "Deciding sample is XML");
        return true;
    }

    @Override
    public LogFileStructure createFromSample(String sampleFileName, String indexName, String typeName, String elasticsearchHost,
                                             String logstashHost, String logstashFileTimezone, String sample, String charsetName)
        throws IOException, ParserConfigurationException, SAXException {
        return new XmlLogFileStructure(terminal, sampleFileName, indexName, typeName, elasticsearchHost, logstashHost, logstashFileTimezone,
            sample, charsetName);
    }
}
