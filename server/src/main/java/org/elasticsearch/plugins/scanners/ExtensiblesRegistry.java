/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.scanners;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

import static org.elasticsearch.core.Strings.format;

/**
 * A registry of Extensible interfaces/classes read from extensibles.json file.
 * The file is generated during Elasticsearch built time (or commited)
 * basing on the classes declared in stable plugins api (i.e. plugin-analysis-api)
 *
 * This file is present in server jar.
 * a class/interface is directly extensible when is marked with @Extensible annotation
 * a class/interface can be indirectly extensible when it extends/implements a directly extensible class
 *
 * Information about extensible interfaces/classes are stored in a map where:
 * key and value are the same cannonical name of the class that is directly marked with @Extensible
 * or
 * key: a cannonical name of the class that is indirectly extensible but extends another extensible class (directly/indirectly)
 * value: cannonical name of the class that is directly extensible
 *
 * The reason for indirectly extensible classes is to allow stable plugin apis to create hierarchies
 *
 * Example:
 * @Extensible
 * interface E{
 *     public void foo();
 * }
 * interface Eprim extends E{
 * }
 *
 * class Aclass implements E{
 *
 * }
 *
 * @Extensible
 * class E2 {
 *     public void bar(){}
 * }
 *
 * the content of extensibles.json should be
 * {
 *     "E" : "E",
 *     "Eprim" : "E",
 *     "A" : "E",
 *     "E2" : "E2"
 * }
 *
 * @see org.elasticsearch.plugin.api.Extensible
 */
public class ExtensiblesRegistry {

    private static final Logger logger = LogManager.getLogger(ExtensiblesRegistry.class);

    private static final String EXTENSIBLES_FILE = "extensibles.json";
    public static final ExtensiblesRegistry INSTANCE = new ExtensiblesRegistry(EXTENSIBLES_FILE);

    private final ExtensibleFileReader extensibleFileReader;
    // classname (potentially extending/implementing extensible) to interface/class annotated with extensible
    private final Map<String, String> loadedExtensible;

    ExtensiblesRegistry(String extensiblesFile) {
        extensibleFileReader = new ExtensibleFileReader(extensiblesFile);

        this.loadedExtensible = extensibleFileReader.readFromFile();
        if (loadedExtensible.size() > 0) {
            logger.debug(() -> format("Loaded extensible from cache file %s", loadedExtensible));
        }

    }

}
