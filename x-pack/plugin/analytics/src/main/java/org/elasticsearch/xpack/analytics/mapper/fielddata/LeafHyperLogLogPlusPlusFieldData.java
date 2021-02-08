/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.mapper.fielddata;

import org.elasticsearch.index.fielddata.LeafFieldData;

import java.io.IOException;

/**
 * {@link LeafFieldData} specialization for HyperLogLogPlusPlus data.
 */
public interface LeafHyperLogLogPlusPlusFieldData extends LeafFieldData {

    /**
     * Return HyperLogLogPlusPlus values.
     */
    HyperLogLogPlusPlusValues getHllValues() throws IOException;

}
