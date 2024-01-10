/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fieldcaps;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class FieldCapsHasValueIT extends ESIntegTestCase {
    private final String INDEX1 = "index-1";
    private final String INDEX2 = "index-2";

    @Before
    public void setUpIndices() {
        assertAcked(
            prepareCreate(INDEX1).setWaitForActiveShards(ActiveShardCount.ALL)
                .setSettings(indexSettings())
                .setMapping("foo", "type=text", "bar", "type=keyword", "bar-alias", "type=alias,path=bar")
        );
        assertAcked(
            prepareCreate(INDEX2).setWaitForActiveShards(ActiveShardCount.ALL).setSettings(indexSettings()).setMapping("bar", "type=date")
        );
    }

    // @Override
    // protected boolean addMockHttpTransport() {
    // return false;
    // }

    // @Override
    // protected int numberOfShards() {
    // return 1; // TODO-MP find solution to remove this
    // }
    //
    // @Override
    // protected int numberOfReplicas() {
    // return 0; // TODO-MP find solution to remove this
    // }

    public void testNoFieldsInEmptyIndex() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("*").setIncludeFieldsWithNoValue(false).get();

        assertIndices(response, INDEX1, INDEX2);
        // Ensure the response has no mapped fields.
        assertFalse(response.get().containsKey("foo"));
        assertFalse(response.get().containsKey("bar"));
        assertFalse(response.get().containsKey("bar-alias"));
    }

    public void testOnlyFieldsWithValue() {
        // createIndex(INDEX1, 1, 0);
        prepareIndex(INDEX1).setSource("foo", "foo-text").get();
        refresh(INDEX1); // TODO-MP is really needed or am I missing something?

        FieldCapabilitiesResponse response = client().prepareFieldCaps(INDEX1).setFields("*").setIncludeFieldsWithNoValue(false).get();

        assertIndices(response, INDEX1);
        assertTrue(response.get().containsKey("foo"));
        // Check the capabilities for the 'foo' field.
        Map<String, FieldCapabilities> fooField = response.getField("foo");
        assertEquals(1, fooField.size());
        assertTrue(fooField.containsKey("text"));
        assertEquals(
            new FieldCapabilities("foo", "text", false, true, false, null, null, null, Collections.emptyMap()),
            fooField.get("text")
        );
    }

    public void testOnlyFieldsWithValueWithIndexFilter() {
        prepareIndex(INDEX1).setSource("foo", "foo-text").get();
        refresh(INDEX1); // TODO-MP is really needed or am I missing something?

        FieldCapabilitiesResponse response = client().prepareFieldCaps(INDEX1).setFields("*").setIncludeFieldsWithNoValue(false).get();

        assertIndices(response, INDEX1);
        assertTrue(response.get().containsKey("foo"));
        // Check the capabilities for the 'foo' field.
        Map<String, FieldCapabilities> fooField = response.getField("foo");
        assertEquals(1, fooField.size());
        assertTrue(fooField.containsKey("text"));
        assertEquals(
            new FieldCapabilities("foo", "text", false, true, false, null, null, null, Collections.emptyMap()),
            fooField.get("text")
        );
    }

    public void testOnlyFieldsWithValueAfterNodesRestart() throws Exception {
        prepareIndex(INDEX1).setSource("foo", "foo-text").get();
        internalCluster().fullRestart();
        ensureGreen(INDEX1);

        FieldCapabilitiesResponse response = client().prepareFieldCaps(INDEX1).setFields("*").setIncludeFieldsWithNoValue(false).get();

        assertIndices(response, INDEX1);
        assertTrue(response.get().containsKey("foo"));
        // Check the capabilities for the 'foo' field.
        Map<String, FieldCapabilities> fooField = response.getField("foo");
        assertEquals(1, fooField.size());
        assertTrue(fooField.containsKey("text"));
        assertEquals(
            new FieldCapabilities("foo", "text", false, true, false, null, null, null, Collections.emptyMap()),
            fooField.get("text")
        );

    }

    public void testFieldsAndAliasWithValue() {
        prepareIndex(INDEX1).setSource("foo", "foo-text").get();
        prepareIndex(INDEX1).setSource("bar", "bar-keyword").get();
        refresh(INDEX1);

        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("*").setIncludeFieldsWithNoValue(false).get();

        assertIndices(response, INDEX1, INDEX2);
        assertTrue(response.get().containsKey("foo"));
        assertTrue(response.get().containsKey("bar"));
        assertTrue(response.get().containsKey("bar-alias"));
        // Check the capabilities for the 'foo' field.
        Map<String, FieldCapabilities> fooField = response.getField("foo");
        assertEquals(1, fooField.size());
        assertTrue(fooField.containsKey("text"));
        assertEquals(
            new FieldCapabilities("foo", "text", false, true, false, null, null, null, Collections.emptyMap()),
            fooField.get("text")
        );
        // Check the capabilities for the 'bar' field.
        Map<String, FieldCapabilities> barField = response.getField("bar");
        assertEquals(1, barField.size());
        assertTrue(barField.containsKey("keyword"));
        assertEquals(
            new FieldCapabilities("bar", "keyword", false, true, true, null, null, null, Collections.emptyMap()),
            barField.get("keyword")
        );
        // Check the capabilities for the 'bar-alias' field.
        Map<String, FieldCapabilities> barAlias = response.getField("bar-alias");
        assertEquals(1, barAlias.size());
        assertTrue(barAlias.containsKey("keyword"));
        assertEquals(
            new FieldCapabilities("bar-alias", "keyword", false, true, true, null, null, null, Collections.emptyMap()),
            barAlias.get("keyword")
        );
    }

    public void testUnmappedFieldsWithValue() {
        prepareIndex(INDEX1).setSource("unmapped-field", "unmapped-text").get();
        refresh(INDEX1);

        FieldCapabilitiesResponse response = client().prepareFieldCaps(INDEX1)
            .setFields("*")
            .setIncludeUnmapped(true)
            .setIncludeFieldsWithNoValue(false)
            .get();

        assertIndices(response, INDEX1);
        assertTrue(response.get().containsKey("unmapped-field"));
        // Check the capabilities for the 'unmapped' field.
        Map<String, FieldCapabilities> unmappedField = response.getField("unmapped-field");
        assertEquals(1, unmappedField.size());
        assertTrue(unmappedField.containsKey("text"));
        assertEquals(
            new FieldCapabilities("unmapped-field", "text", false, true, false, null, null, null, Collections.emptyMap()),
            unmappedField.get("text")
        );
    }

    public void testUnmappedFieldsWithValueAfterRestart() throws Exception {
        prepareIndex(INDEX1).setSource("unmapped", "unmapped-text").get();
        internalCluster().fullRestart();
        ensureGreen(INDEX1);

        FieldCapabilitiesResponse response = client().prepareFieldCaps()
            .setFields("*")
            .setIncludeUnmapped(true)
            .setIncludeFieldsWithNoValue(false)
            .get();

        assertIndices(response, INDEX1, INDEX2);
        assertTrue(response.get().containsKey("unmapped"));
        // Check the capabilities for the 'unmapped' field.
        Map<String, FieldCapabilities> unmappedField = response.getField("unmapped");
        assertEquals(2, unmappedField.size());
        assertTrue(unmappedField.containsKey("text"));
        assertEquals(
            new FieldCapabilities("unmapped", "text", false, true, false, new String[] { INDEX1 }, null, null, Collections.emptyMap()),
            unmappedField.get("text")
        );
    }

    public void testConstantFieldsNeverExcluded() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps()
            .setFields("*")
            .setIncludeUnmapped(true)
            .setIncludeFieldsWithNoValue(false)
            .get();

        assertIndices(response, INDEX1, INDEX2);
        assertTrue(response.get().containsKey("_index"));
        // Check the capabilities for the '_index' constant field.
        Map<String, FieldCapabilities> unmappedField = response.getField("_index");
        assertEquals(1, unmappedField.size());
        assertTrue(unmappedField.containsKey("_index"));
        assertEquals(
            new FieldCapabilities("_index", "_index", true, true, true, null, null, null, Collections.emptyMap()),
            unmappedField.get("_index")
        );
    }

    public void testTwoFieldsNameTwoIndices() {
        prepareIndex(INDEX1).setSource("foo", "foo-text").get();
        prepareIndex(INDEX2).setSource("bar", 1704293160000L).get();
        refresh(INDEX1, INDEX2);

        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("*").setIncludeFieldsWithNoValue(false).get();

        assertIndices(response, INDEX1, INDEX2);
        assertTrue(response.get().containsKey("foo"));
        assertTrue(response.get().containsKey("bar"));
        // Check the capabilities for the 'foo' field.
        Map<String, FieldCapabilities> fooField = response.getField("foo");
        assertEquals(1, fooField.size());
        assertTrue(fooField.containsKey("text"));
        assertEquals(
            new FieldCapabilities("foo", "text", false, true, false, null, null, null, Collections.emptyMap()),
            fooField.get("text")
        );
        // Check the capabilities for the 'bar' field.
        Map<String, FieldCapabilities> barField = response.getField("bar");
        assertEquals(1, barField.size());
        assertTrue(barField.containsKey("date"));
        assertEquals(
            new FieldCapabilities("bar", "date", false, true, true, null, null, null, Collections.emptyMap()),
            barField.get("date")
        );
    }

    public void testSameFieldNameTwoIndices() {
        prepareIndex(INDEX1).setSource("bar", "bar-text").get();
        prepareIndex(INDEX2).setSource("bar", 1704293160000L).get();
        refresh(INDEX1, INDEX2);

        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("*").setIncludeFieldsWithNoValue(false).get();

        assertIndices(response, INDEX1, INDEX2);
        assertTrue(response.get().containsKey("bar"));
        // Check the capabilities for the 'bar' field.
        Map<String, FieldCapabilities> barField = response.getField("bar");
        assertEquals(2, barField.size());
        assertTrue(barField.containsKey("keyword"));
        assertEquals(
            new FieldCapabilities("bar", "keyword", false, true, true, new String[] { INDEX1 }, null, null, Collections.emptyMap()),
            barField.get("keyword")
        );
        assertTrue(barField.containsKey("date"));
        assertEquals(
            new FieldCapabilities("bar", "date", false, true, true, new String[] { INDEX2 }, null, null, Collections.emptyMap()),
            barField.get("date")
        );
    }

    public void testDeletedDocsReturned() {
        // In this current implementation we do not handle deleted documents (without a restart).
        // This test should fail if in a future implementation we handle deletes.
        DocWriteResponse foo = prepareIndex(INDEX1).setSource("foo", "foo-text").get();
        client().prepareDelete().setId(foo.getId());
        refresh(INDEX1);

        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("*").setIncludeFieldsWithNoValue(false).get();

        assertIndices(response, INDEX1, INDEX2);
        assertTrue(response.get().containsKey("foo"));
        // Check the capabilities for the 'foo' field.
        Map<String, FieldCapabilities> fooField = response.getField("foo");
        assertEquals(1, fooField.size());
        assertTrue(fooField.containsKey("text"));
        assertEquals(
            new FieldCapabilities("foo", "text", false, true, false, null, null, null, Collections.emptyMap()),
            fooField.get("text")
        );
    }

    public void testDeletedDocsNotReturnedAfterRestart() throws Exception {
        DocWriteResponse foo = prepareIndex(INDEX1).setSource("foo", "foo-text").get();
        client().prepareDelete().setId(foo.getId());
        internalCluster().fullRestart();
        ensureGreen(INDEX1);

        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("*").setIncludeFieldsWithNoValue(false).get();

        assertIndices(response, INDEX1, INDEX2);
        assertFalse(response.get().containsKey("foo"));
    }

    private void assertIndices(FieldCapabilitiesResponse response, String... indices) {
        assertNotNull(response.getIndices());
        Arrays.sort(indices);
        Arrays.sort(response.getIndices());
        assertArrayEquals(indices, response.getIndices());
    }

}
