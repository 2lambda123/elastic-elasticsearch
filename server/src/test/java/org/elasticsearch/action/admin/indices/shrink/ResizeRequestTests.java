/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.shrink;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestTests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.RandomCreateIndexGenerator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;

public class ResizeRequestTests extends ESTestCase {

    public void testCopySettingsValidation() {
        runTestCopySettingsValidation(false, r -> {
            final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, r::get);
            assertThat(e, hasToString(containsString("[copySettings] can not be explicitly set to [false]")));
        });

        runTestCopySettingsValidation(null, r -> assertNull(r.get().getCopySettings()));
        runTestCopySettingsValidation(true, r -> assertTrue(r.get().getCopySettings()));
    }

    private void runTestCopySettingsValidation(final Boolean copySettings, final Consumer<Supplier<ResizeRequest>> consumer) {
        consumer.accept(() -> {
            final ResizeRequest request = new ResizeRequest();
            request.setCopySettings(copySettings);
            return request;
        });
    }

    public void testToXContent() throws IOException {
        {
            ResizeRequest request = new ResizeRequest("target", "source");
            String actualRequestBody = Strings.toString(request);
            assertEquals("{\"settings\":{},\"aliases\":{}}", actualRequestBody);
        }
        {
            ResizeRequest request = new ResizeRequest("target", "source");
            request.setMaxPrimaryShardSize(new ByteSizeValue(100, ByteSizeUnit.MB));
            String actualRequestBody = Strings.toString(request);
            assertEquals("{\"settings\":{},\"aliases\":{},\"max_primary_shard_size\":\"100mb\"}", actualRequestBody);
        }
        {
            ResizeRequest request = new ResizeRequest();
            CreateIndexRequest target = new CreateIndexRequest("target");
            Alias alias = new Alias("test_alias");
            alias.routing("1");
            alias.filter("{\"term\":{\"year\":2016}}");
            alias.writeIndex(true);
            target.alias(alias);
            Settings.Builder settings = Settings.builder();
            settings.put(SETTING_NUMBER_OF_SHARDS, 10);
            target.settings(settings);
            request.setTargetIndex(target);
            String actualRequestBody = Strings.toString(request);
            String expectedRequestBody = "{\"settings\":{\"index\":{\"number_of_shards\":\"10\"}}," +
                    "\"aliases\":{\"test_alias\":{\"filter\":{\"term\":{\"year\":2016}},\"routing\":\"1\",\"is_write_index\":true}}}";
            assertEquals(expectedRequestBody, actualRequestBody);
        }
    }

    public void testToAndFromXContent() throws IOException {
        final ResizeRequest resizeRequest = createTestItem();

        boolean humanReadable = randomBoolean();
        final XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(resizeRequest, xContentType, EMPTY_PARAMS, humanReadable);

        ResizeRequest parsedResizeRequest = new ResizeRequest(resizeRequest.getTargetIndexRequest().index(),
                resizeRequest.getSourceIndex());
        try (XContentParser xParser = createParser(xContentType.xContent(), originalBytes)) {
            parsedResizeRequest.fromXContent(xParser);
        }

        assertEquals(resizeRequest.getSourceIndex(), parsedResizeRequest.getSourceIndex());
        assertEquals(resizeRequest.getTargetIndexRequest().index(), parsedResizeRequest.getTargetIndexRequest().index());
        CreateIndexRequestTests.assertAliasesEqual(resizeRequest.getTargetIndexRequest().aliases(),
                parsedResizeRequest.getTargetIndexRequest().aliases());
        assertEquals(resizeRequest.getTargetIndexRequest().settings(), parsedResizeRequest.getTargetIndexRequest().settings());
        assertEquals(resizeRequest.getMaxPrimaryShardSize(), parsedResizeRequest.getMaxPrimaryShardSize());

        BytesReference finalBytes = toShuffledXContent(parsedResizeRequest, xContentType, EMPTY_PARAMS, humanReadable);
        ElasticsearchAssertions.assertToXContentEquivalent(originalBytes, finalBytes, xContentType);
    }

    public void testEqualsAndHashCode() {
        ResizeRequest request = createTestItem();
        assertEquals(request, request);
        assertEquals(request.hashCode(), request.hashCode());

        ResizeRequest copy = new ResizeRequest();
        copy.setTargetIndex(request.getTargetIndexRequest());
        copy.setSourceIndex(request.getSourceIndex());
        copy.setResizeType(request.getResizeType());
        copy.setCopySettings(request.getCopySettings());
        copy.setMaxPrimaryShardSize(request.getMaxPrimaryShardSize());
        assertEquals(request, copy);
        assertEquals(copy, request);
        assertEquals(request.hashCode(), copy.hashCode());

        // Changing targetIndexRequest makes requests not equal
        copy.setTargetIndex(new CreateIndexRequest(request.getTargetIndexRequest().index() + randomAlphaOfLength(1)));
        assertNotEquals(request, copy);
        assertNotEquals(request.hashCode(), copy.hashCode());

        // Changing sourceIndex makes requests not equal
        copy.setSourceIndex(request.getSourceIndex() + randomAlphaOfLength(1));
        assertNotEquals(request, copy);
        assertNotEquals(request.hashCode(), copy.hashCode());

        // Changing resizeType makes requests not equal
        copy.setResizeType(ResizeType.SPLIT);
        assertNotEquals(request, copy);
        assertNotEquals(request.hashCode(), copy.hashCode());

        // Changing resizeType makes requests not equal
        if (request.getCopySettings()) {
            copy.setCopySettings(null);
        } else {
            copy.setCopySettings(true);
        }
        assertNotEquals(request, copy);
        assertNotEquals(request.hashCode(), copy.hashCode());

        // Changing maxPrimaryShardSize makes requests not equal
        if (request.getMaxPrimaryShardSize() != null) {
            copy.setMaxPrimaryShardSize(null);
        } else {
            copy.setMaxPrimaryShardSize(new ByteSizeValue(randomIntBetween(1, 100)));
        }
        assertNotEquals(request, copy);
        assertNotEquals(request.hashCode(), copy.hashCode());
    }

    public void testSerializeRequest() throws IOException {
        ResizeRequest request = createTestItem();
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        BytesReference bytes = out.bytes();
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(NetworkModule.getNamedWriteables());
        StreamInput wrap = new NamedWriteableAwareStreamInput(bytes.streamInput(),
            namedWriteableRegistry);
        ResizeRequest deserializedReq = new ResizeRequest(wrap);

        assertEquals(request.getSourceIndex(), deserializedReq.getSourceIndex());
        assertEquals(request.getTargetIndexRequest().settings(), deserializedReq.getTargetIndexRequest().settings());
        assertEquals(request.getTargetIndexRequest().aliases(), deserializedReq.getTargetIndexRequest().aliases());
        assertEquals(request.getCopySettings(), deserializedReq.getCopySettings());
        assertEquals(request.getResizeType(), deserializedReq.getResizeType());
        assertEquals(request.getMaxPrimaryShardSize(), deserializedReq.getMaxPrimaryShardSize());
    }

    private static ResizeRequest createTestItem() {
        ResizeRequest resizeRequest = new ResizeRequest(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) {
            resizeRequest.setTargetIndex(RandomCreateIndexGenerator.randomCreateIndexRequest());
        }
        if (randomBoolean()) {
            resizeRequest.setMaxPrimaryShardSize(new ByteSizeValue(randomIntBetween(1, 100)));
        }
        return resizeRequest;
    }
}
