/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class SearchLookup {

    private final SourceLookup sourceLookup;
    private final TrackingMappedFieldsLookup fieldTypeLookup;
    private final BiFunction<MappedFieldType, Supplier<SearchLookup>, IndexFieldData<?>> fieldDataLookup;

    /**
     * Create the top level field lookup for a search request.
     *
     * Provides a way to look up fields from doc_values, stored fields, or _source.
     */
    public SearchLookup(Function<String, MappedFieldType> fieldTypeLookup,
                        BiFunction<MappedFieldType, Supplier<SearchLookup>, IndexFieldData<?>> fieldDataLookup) {
        this.fieldTypeLookup = new TrackingMappedFieldsLookup(fieldTypeLookup);
        this.sourceLookup = new SourceLookup();
        this.fieldDataLookup = fieldDataLookup;
    }

    /**
     * Create a new {@link SearchLookup} that looks fields up the same as the wrapped one,
     * while also tracking field references starting from the provided field name. It detects cycles
     * and prevents resolving fields that depend on more than
     * {@link TrackingMappedFieldsLookup#MAX_FIELD_CHAIN_DEPTH} fields.
     * @param field        the field to exclude from further field lookups
     */
    private SearchLookup(SearchLookup searchLookup, String field) {
        this.sourceLookup = searchLookup.sourceLookup;
        this.fieldTypeLookup = searchLookup.fieldTypeLookup.trackingField(field);
        this.fieldDataLookup = searchLookup.fieldDataLookup;
    }

    public LeafSearchLookup getLeafSearchLookup(LeafReaderContext context) {
        return new LeafSearchLookup(context,
                new LeafDocLookup(fieldTypeLookup::get, this::getForField, context),
                sourceLookup,
                new LeafStoredFieldsLookup(fieldTypeLookup::get, (doc, visitor) -> context.reader().document(doc, visitor)));
    }

    public MappedFieldType fieldType(String fieldName) {
        return fieldTypeLookup.get(fieldName);
    }

    public IndexFieldData<?> getForField(MappedFieldType fieldType) {
        return fieldDataLookup.apply(fieldType, () -> new SearchLookup(this, fieldType.name()));
    }

    public SourceLookup source() {
        return sourceLookup;
    }

}
