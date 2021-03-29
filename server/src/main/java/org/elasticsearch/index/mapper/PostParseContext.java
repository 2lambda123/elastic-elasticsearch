/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.function.Function;

/**
 * Holds state useful for post-parse document processing
 */
public final class PostParseContext {

    /** A search lookup over the parsed document */
    public final SearchLookup searchLookup;

    /** A LeafReaderContext for a lucene reader based on the parsed document */
    public final LeafReaderContext leafReaderContext;

    /** The ParseContext used during document parsing */
    public final ParseContext pc;

    PostParseContext(Function<String, MappedFieldType> fieldTypeLookup, ParseContext pc, LeafReaderContext ctx) {
        this.searchLookup = new SearchLookup(
            fieldTypeLookup,
            (ft, s) -> ft.fielddataBuilder(pc.indexSettings().getIndex().getName(), s).build(
                new IndexFieldDataCache.None(),
                new NoneCircuitBreakerService())
        );
        this.pc = pc;
        this.leafReaderContext = ctx;
    }

}
