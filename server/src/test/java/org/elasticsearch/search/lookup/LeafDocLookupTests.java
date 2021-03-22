/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.lookup;

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.function.Function;

import static org.elasticsearch.search.lookup.LeafDocLookup.TYPES_DEPRECATION_MESSAGE;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LeafDocLookupTests extends ESTestCase {
    private ScriptDocValues<?> docValues;
    private LeafDocLookup docLookup;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.name()).thenReturn("field");
        when(fieldType.valueForDisplay(anyObject())).then(returnsFirstArg());

        docValues = mock(ScriptDocValues.class);
        IndexFieldData<?> fieldData = createFieldData(docValues);

        docLookup = new LeafDocLookup(
            field -> field.equals("field") || field.equals("alias") || field.equals("_type") ? fieldType : null,
            ignored -> fieldData,
            null);
    }

    public void testBasicLookup() {
        ScriptDocValues<?> fetchedDocValues = docLookup.get("field");
        assertEquals(docValues, fetchedDocValues);
    }

    public void testFieldAliases() {
        ScriptDocValues<?> fetchedDocValues = docLookup.get("alias");
        assertEquals(docValues, fetchedDocValues);
    }

    public void testTypesDeprecation() {
        ScriptDocValues<?> fetchedDocValues = docLookup.get("_type");
        assertEquals(docValues, fetchedDocValues);
        assertWarnings(TYPES_DEPRECATION_MESSAGE);
    }

    public void testFlattenedField() {
        ScriptDocValues<?> docValues1 = mock(ScriptDocValues.class);
        IndexFieldData<?> fieldData1 = createFieldData(docValues1);

        ScriptDocValues<?> docValues2 = mock(ScriptDocValues.class);
        IndexFieldData<?> fieldData2 = createFieldData(docValues2);

        FlattenedFieldMapper.KeyedFlattenedFieldType fieldType1
            = new FlattenedFieldMapper.KeyedFlattenedFieldType("field", true, true, "key1", false, Collections.emptyMap());
        FlattenedFieldMapper.KeyedFlattenedFieldType fieldType2
            = new FlattenedFieldMapper.KeyedFlattenedFieldType( "field", true, true, "key2", false, Collections.emptyMap());

        Function<MappedFieldType, IndexFieldData<?>> fieldDataSupplier = fieldType -> {
            FlattenedFieldMapper.KeyedFlattenedFieldType keyedFieldType = (FlattenedFieldMapper.KeyedFlattenedFieldType) fieldType;
            return keyedFieldType.key().equals("key1") ? fieldData1 : fieldData2;
        };

        LeafDocLookup docLookup = new LeafDocLookup(field -> {
            if (field.equals("json.key1")) {
                return fieldType1;
            }
            if (field.equals("json.key2")) {
                return fieldType2;
            }
            return null;
        }, fieldDataSupplier, null);

        assertEquals(docValues1, docLookup.get("json.key1"));
        assertEquals(docValues2, docLookup.get("json.key2"));
    }

    private IndexFieldData<?> createFieldData(ScriptDocValues<?> scriptDocValues) {
        LeafFieldData leafFieldData = mock(LeafFieldData.class);
        doReturn(scriptDocValues).when(leafFieldData).getScriptValues();

        IndexFieldData<?> fieldData = mock(IndexFieldData.class);
        when(fieldData.getFieldName()).thenReturn("field");
        doReturn(leafFieldData).when(fieldData).load(anyObject());

        return fieldData;
    }
}
