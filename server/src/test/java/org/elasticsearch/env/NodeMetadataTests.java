/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.env;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class NodeMetadataTests extends ESTestCase {
    // (Index)VersionUtils.randomVersion() only returns known versions, which are necessarily no later than (Index)Version.CURRENT;
    // however we want to also consider our behaviour with all versions, so occasionally pick up a truly random version.
    private Version randomVersion() {
        return rarely() ? Version.fromId(randomInt()) : VersionUtils.randomVersion(random());
    }

    private IndexVersion randomIndexVersion() {
        return rarely() ? IndexVersion.fromId(randomInt()) : IndexVersionUtils.randomVersion(random());
    }

    public void testEqualsHashcodeSerialization() {
        final Path tempDir = createTempDir();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new NodeMetadata(randomAlphaOfLength(10), randomIndexVersion(), randomIndexVersion()),
            nodeMetadata -> {
                final long generation = NodeMetadata.FORMAT.writeAndCleanup(nodeMetadata, tempDir);
                final Tuple<NodeMetadata, Long> nodeMetadataLongTuple = NodeMetadata.FORMAT.loadLatestStateWithGeneration(
                    logger,
                    xContentRegistry(),
                    tempDir
                );
                assertThat(nodeMetadataLongTuple.v2(), equalTo(generation));
                return nodeMetadataLongTuple.v1();
            },
            nodeMetadata -> switch (randomInt(3)) {
                case 0 -> new NodeMetadata(
                    randomAlphaOfLength(21 - nodeMetadata.nodeId().length()),
                    nodeMetadata.nodeIndexVersion(),
                    nodeMetadata.oldestIndexVersion()
                );
                case 1 -> new NodeMetadata(
                    nodeMetadata.nodeId(),
                    randomValueOtherThan(nodeMetadata.nodeIndexVersion(), this::randomIndexVersion),
                    nodeMetadata.oldestIndexVersion()
                );
                default -> new NodeMetadata(
                    nodeMetadata.nodeId(),
                    nodeMetadata.nodeIndexVersion(),
                    randomValueOtherThan(nodeMetadata.oldestIndexVersion(), this::randomIndexVersion)
                );
            }
        );
    }

    public void testReadsFormatWithoutVersion() throws IOException {
        // the behaviour tested here is only appropriate if the current version is compatible with versions 7 and earlier
        assertTrue(IndexVersions.MINIMUM_COMPATIBLE.onOrBefore(IndexVersions.V_7_0_0));
        // when the current version is incompatible with version 7, the behaviour should change to reject files like the given resource
        // which do not have the version field

        final Path tempDir = createTempDir();
        final Path stateDir = Files.createDirectory(tempDir.resolve(MetadataStateFormat.STATE_DIR_NAME));
        final InputStream resource = this.getClass().getResourceAsStream("testReadsFormatWithoutVersion.binary");
        assertThat(resource, notNullValue());
        Files.copy(resource, stateDir.resolve(NodeMetadata.FORMAT.getStateFileName(between(0, Integer.MAX_VALUE))));
        final NodeMetadata nodeMetadata = NodeMetadata.FORMAT.loadLatestState(logger, xContentRegistry(), tempDir);
        assertThat(nodeMetadata.nodeId(), equalTo("y6VUVMSaStO4Tz-B5BxcOw"));
        assertThat(nodeMetadata.nodeIndexVersion(), equalTo(IndexVersions.ZERO));
    }

    public void testUpgradesLegitimateVersions() {
        final String nodeId = randomAlphaOfLength(10);
        final NodeMetadata nodeMetadata = new NodeMetadata(
            nodeId,
            randomValueOtherThanMany(
                v -> v.after(IndexVersion.current()) || v.before(IndexVersions.MINIMUM_IN_PLACE_UPGRADE_COMPATIBLE),
                this::randomIndexVersion
            ),
            IndexVersion.current()
        ).upgradeToCurrentVersion();
        assertThat(nodeMetadata.nodeIndexVersion(), equalTo(IndexVersion.current()));
        assertThat(nodeMetadata.nodeId(), equalTo(nodeId));
    }

    public void testUpgradesMissingVersion() {
        final String nodeId = randomAlphaOfLength(10);

        final IllegalStateException illegalStateException = expectThrows(
            IllegalStateException.class,
            () -> new NodeMetadata(nodeId, IndexVersions.ZERO, IndexVersion.current()).upgradeToCurrentVersion()
        );
        assertThat(
            illegalStateException.getMessage(),
            startsWith(
                "cannot upgrade a node using index version [" + IndexVersions.ZERO + "] directly to version [" + Build.current().version() + "]"
            )
        );
    }

    public void testDoesNotUpgradeFutureVersion() {
        final IllegalStateException illegalStateException = expectThrows(
            IllegalStateException.class,
            () -> new NodeMetadata(randomAlphaOfLength(10), tooNewVersion(), IndexVersion.current()).upgradeToCurrentVersion()
        );
        assertThat(
            illegalStateException.getMessage(),
            allOf(startsWith("cannot downgrade a node using index version ["), endsWith("] to version [" + Build.current().version() + "]"))
        );
    }

    public void testDoesNotUpgradeAncientVersion() {
        final IllegalStateException illegalStateException = expectThrows(
            IllegalStateException.class,
            () -> new NodeMetadata(randomAlphaOfLength(10), tooOldVersion(), IndexVersion.current()).upgradeToCurrentVersion()
        );
        assertThat(
            illegalStateException.getMessage(),
            allOf(
                startsWith("cannot upgrade a node using index version ["),
                endsWith(
                    "] directly to version ["
                        + Build.current().version()
                        + "], upgrade to version ["
                        + Build.current().minWireCompatVersion()
                        + "] first."
                )
            )
        );
    }

    public void testUpgradeMarksPreviousVersion() {
        final String nodeId = randomAlphaOfLength(10);
        final IndexVersion version = IndexVersionUtils.randomVersionBetween(
            random(),
            IndexVersions.MINIMUM_IN_PLACE_UPGRADE_COMPATIBLE,
            IndexVersions.V_8_0_0
        );

        final NodeMetadata nodeMetadata = new NodeMetadata(nodeId, version, IndexVersion.current()).upgradeToCurrentVersion();
        assertThat(nodeMetadata.nodeIndexVersion(), equalTo(IndexVersion.current()));
        assertThat(nodeMetadata.previousNodeIndexVersion(), equalTo(version));
    }

    public static IndexVersion tooNewVersion() {
        return IndexVersion.fromId(between(IndexVersion.current().id() + 1, 99999999));
    }

    public static IndexVersion tooNewIndexVersion() {
        return IndexVersion.fromId(between(IndexVersion.current().id() + 1, 99999999));
    }

    public static IndexVersion tooOldVersion() {
        return IndexVersion.fromId(between(1, IndexVersions.MINIMUM_IN_PLACE_UPGRADE_COMPATIBLE.id() - 1));
    }
}
