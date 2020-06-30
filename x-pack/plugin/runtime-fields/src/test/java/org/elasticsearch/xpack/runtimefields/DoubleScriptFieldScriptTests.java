/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.lookup.DocLookup;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.xpack.runtimefields.DoubleScriptFieldScript.Factory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class DoubleScriptFieldScriptTests extends ScriptFieldScriptTestCase<
    DoubleScriptFieldScript.Factory,
    DoubleRuntimeValues,
    SortedNumericDoubleValues,
    Double> {

    public void testConstant() throws IOException {
        assertThat(randomDoubles().collect("value(3.14)"), equalTo(List.of(3.14, 3.14)));
    }

    public void testTwoConstants() throws IOException {
        assertThat(randomDoubles().collect("value(3.14); value(2.72)"), equalTo(List.of(2.72, 3.14, 2.72, 3.14)));
    }

    public void testSource() throws IOException {
        assertThat(singleValueInSource().collect("value(source['foo'] * 10.1)"), equalTo(List.of(10.1, 101.0)));
    }

    public void testTwoSourceValues() throws IOException {
        assertThat(
            multiValueInSource().collect("value(source['foo'][0] * 10.1); value(source['foo'][1] * 10.2)"),
            equalTo(List.of(10.1, 20.4, 101.0, 204.0))
        );
    }

    public void testDocValues() throws IOException {
        assertThat(singleValueInDocValues().collect("value(doc['foo'].value * 9.9)"), equalTo(List.of(10.89, 99.99)));
    }

    public void testMultipleDocValuesValues() throws IOException {
        assertThat(
            multipleValuesInDocValues().collect("for (double d : doc['foo']) {value(d * 9.9)}"),
            equalTo(List.of(10.89, 21.78, 99.99, 198.99))
        );
    }

    private TestCase randomDoubles() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(doubleDocValue(randomDouble())));
            iw.addDocument(List.of(doubleDocValue(randomDouble())));
        });
    }

    private TestCase singleValueInSource() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 1}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 10}"))));
        });
    }

    private TestCase multiValueInSource() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1, 2]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [10, 20]}"))));
        });
    }

    private TestCase singleValueInDocValues() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(doubleDocValue(1.1)));
            iw.addDocument(List.of(doubleDocValue(10.1)));
        });
    }

    private TestCase multipleValuesInDocValues() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(doubleDocValue(1.1), doubleDocValue(2.2)));
            iw.addDocument(List.of(doubleDocValue(10.1), doubleDocValue(20.1)));
        });
    }

    private IndexableField doubleDocValue(double value) {
        return new SortedNumericDocValuesField("foo", NumericUtils.doubleToSortableLong(value));
    }

    @Override
    protected MappedFieldType[] fieldTypes() {
        return new MappedFieldType[] { new NumberFieldType("foo", NumberType.DOUBLE) };
    }

    @Override
    protected ScriptContext<DoubleScriptFieldScript.Factory> scriptContext() {
        return DoubleScriptFieldScript.CONTEXT;
    }

    @Override
    protected DoubleRuntimeValues newValues(Factory factory, Map<String, Object> params, SourceLookup source, DocLookup fieldData)
        throws IOException {
        return factory.newFactory(params, source, fieldData).runtimeValues();
    }

    @Override
    protected CheckedFunction<LeafReaderContext, SortedNumericDoubleValues, IOException> docValuesBuilder(DoubleRuntimeValues values) {
        return values.docValues();
    }

    @Override
    protected void readAllDocValues(SortedNumericDoubleValues docValues, int docId, Consumer<Double> sync) throws IOException {
        assertTrue(docValues.advanceExact(docId));
        int count = docValues.docValueCount();
        for (int i = 0; i < count; i++) {
            sync.accept(docValues.nextValue());
        }
    }
}
