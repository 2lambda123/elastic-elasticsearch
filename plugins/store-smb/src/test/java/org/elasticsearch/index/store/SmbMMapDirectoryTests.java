/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;

public class SmbMMapDirectoryTests extends EsBaseDirectoryTestCase {

    @Override
    protected Directory getDirectory(Path file) throws IOException {
        return new SmbDirectoryWrapper(new MMapDirectory(file));
    }

    @Override
    public void testCreateOutputForExistingFile() throws IOException {
        /**
         * This test is disabled because {@link SmbDirectoryWrapper} opens existing file
         * with an explicit StandardOpenOption.TRUNCATE_EXISTING option.
         */
    }
}
