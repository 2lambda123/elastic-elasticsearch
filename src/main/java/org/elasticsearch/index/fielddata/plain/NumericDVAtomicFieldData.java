/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.NumericDocValues;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.index.fielddata.AbstractAtomicNumericFieldData;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.LongValues;

import java.io.IOException;

/** {@link AtomicFieldData} impl on top of Lucene's numeric doc values. */
public class NumericDVAtomicFieldData extends AbstractAtomicNumericFieldData {

    private final AtomicReader reader;
    private final String field;

    public NumericDVAtomicFieldData(AtomicReader reader, String field) {
        super(false);
        this.reader = reader;
        this.field = field;
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public boolean isMultiValued() {
        return false;
    }

    @Override
    public boolean isValuesOrdered() {
        return true; // single-valued
    }

    @Override
    public int getNumDocs() {
        return reader.maxDoc();
    }

    @Override
    public long getNumberUniqueValues() {
        // good upper limit
        return reader.maxDoc();
    }

    @Override
    public long getMemorySizeInBytes() {
        // TODO: cannot be computed from Lucene
        return -1;
    }

    private NumericDocValues getDocValues() {
        final NumericDocValues values;
        try {
            values = reader.getNumericDocValues(field);
        } catch (IOException e) {
            throw new ElasticSearchIllegalStateException("Cannot load doc values", e);
        }
        return values != null ? values : NumericDocValues.EMPTY;
    }

    @Override
    public LongValues getLongValues() {
        final NumericDocValues values = getDocValues();
        return new LongValues(false) {

            @Override
            public boolean hasValue(int docId) {
                // LUCENE UPGRADE getDocsWithField
                return true;
            }

            @Override
            public long getValue(int docId) {
                return values.get(docId);
            }
        };
    }

    @Override
    public DoubleValues getDoubleValues() {
        final NumericDocValues values = getDocValues();
        return new DoubleValues(false) {

            @Override
            public boolean hasValue(int docId) {
                // LUCENE UPGRADE getDocsWithField
                return true;
            }

            @Override
            public double getValue(int docId) {
                return values.get(docId);
            }

        };
    }

}
