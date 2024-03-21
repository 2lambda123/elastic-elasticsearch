/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.lookup.FieldLookup;
import org.elasticsearch.search.lookup.LeafFieldLookupProvider;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Makes pre-loaded stored fields available via a LeafSearchLookup.
 *
 * If a stored field is requested that is not in the pre-loaded list,
 * this loader will fall back to loading directly from the context
 * stored fields
 */
class PreloadedFieldLookupProvider implements LeafFieldLookupProvider {

    Map<String, List<Object>> storedFields;
    LeafFieldLookupProvider backUpLoader;
    Supplier<LeafFieldLookupProvider> loaderSupplier;

    @Override
    public void populateFieldLookup(FieldLookup fieldLookup, int doc) throws IOException {
        if (storedFields == null) {
            loadDirect(fieldLookup, doc);
        }
        final String field = fieldLookup.fieldType().name();
        if (storedFields.containsKey(field)) {
            fieldLookup.setValues(storedFields.get(field));
            return;
        }
        // stored field not preloaded, go and get it directly
        loadDirect(fieldLookup, doc);
    }

    /**
     * If stored fields are not pre-loaded load directly
     * @param fieldLookup a {@link FieldLookup} object with the field and its values
     * @param doc the doc id of the document for which we are loading fields
     * @throws IOException throws if the loading fails
     */
    private void loadDirect(final FieldLookup fieldLookup, int doc) throws IOException {
        if (backUpLoader == null) {
            backUpLoader = loaderSupplier.get();
        }
        backUpLoader.populateFieldLookup(fieldLookup, doc);
    }

    void setNextReader(LeafReaderContext ctx) {
        backUpLoader = null;
        loaderSupplier = () -> LeafFieldLookupProvider.fromStoredFields().apply(ctx);
    }
}
