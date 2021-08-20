/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import org.elasticsearch.gradle.internal.test.GradleUnitTestCase;
import org.gradle.api.GradleException;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class GenerateReleaseNotesTaskTest extends GradleUnitTestCase {
    private GitWrapper gitWrapper;

    @Before
    public void setup() {
        this.gitWrapper = mock(GitWrapper.class);
    }

    /**
     * Check that partitioning changelog files when the current version is a snapshot returns a map with a single entry.
     */
    @Test
    public void partitionFiles_withSnapshot_returnsSingleMapping() {
        // when:
        Map<QualifiedVersion, Set<File>> partitionedFiles = GenerateReleaseNotesTask.partitionFiles(
            gitWrapper,
            "8.0.0-SNAPSHOT",
            Set.of(new File("docs/changelog/1234.yaml"))
        );

        // then:
        assertThat(partitionedFiles, aMapWithSize(1));
        assertThat(
            partitionedFiles,
            hasEntry(equalTo(QualifiedVersion.of("8.0.0-SNAPSHOT")), hasItem(new File("docs/changelog/1234.yaml")))
        );
        verifyZeroInteractions(gitWrapper);
    }

    /**
     * Check that partitioning changelog files when the current version is the first release
     * in a minor series returns a map with a single entry.
     */
    @Test
    public void partitionFiles_withFirstRevision_returnsSingleMapping() {
        // when:
        Map<QualifiedVersion, Set<File>> partitionedFiles = GenerateReleaseNotesTask.partitionFiles(
            gitWrapper,
            "8.5.0",
            Set.of(new File("docs/changelog/1234.yaml"))
        );

        // then:
        assertThat(partitionedFiles, aMapWithSize(1));
        assertThat(partitionedFiles, hasEntry(equalTo(QualifiedVersion.of("8.5.0")), hasItem(new File("docs/changelog/1234.yaml"))));
        verifyZeroInteractions(gitWrapper);
    }

    /**
     * Check that partitioning changelog files when the current version is the first alpha prerelease returns a map with a single entry.
     */
    @Test
    public void partitionFiles_withFirstAlpha_returnsSingleMapping() {
        // when:
        Map<QualifiedVersion, Set<File>> partitionedFiles = GenerateReleaseNotesTask.partitionFiles(
            gitWrapper,
            "8.0.0-alpha1",
            Set.of(new File("docs/changelog/1234.yaml"))
        );

        // then:
        assertThat(partitionedFiles, aMapWithSize(1));
        assertThat(partitionedFiles, hasEntry(equalTo(QualifiedVersion.of("8.0.0-alpha1")), hasItem(new File("docs/changelog/1234.yaml"))));
        verifyZeroInteractions(gitWrapper);
    }

    /**
     * Check that the git wrapper throws an error if it can't find the right git remote.
     */
    @Test
    public void partitionFiles_withoutGitRemote_throwsError() {
        // when:
        when(gitWrapper.listRemotes()).thenReturn(Map.of("fred", "fred/elasticsearch"));
        GradleException exception = expectThrows(
            GradleException.class,
            () -> GenerateReleaseNotesTask.partitionFiles(gitWrapper, "8.0.1", Set.of())
        );

        // then:
        assertThat(exception.getMessage(), containsString("I need to ensure the git tags are up-to-date"));
    }

    /**
     * Check that the task partitions the list of files correctly by version for a prerelease.
     */
    @Test
    public void partitionFiles_withPrerelease_correctlyGroupsByPrereleaseVersion() {
        // given:
        when(gitWrapper.listRemotes()).thenReturn(Map.of("upstream", "elastic/elasticsearch"));
        when(gitWrapper.listVersions(anyString())).thenReturn(
            Stream.of("8.0.0-alpha1", "8.0.0-alpha2", "8.0.0-beta1", "8.0.0-beta2", "8.0.0-beta3", "8.0.0-rc1", "8.0.0")
                .map(QualifiedVersion::of)
        );
        when(gitWrapper.listFiles(eq("v8.0.0-alpha1"), anyString())).thenReturn(
            Stream.of("docs/changelog/1_1234.yaml", "docs/changelog/1_5678.yaml")
        );
        when(gitWrapper.listFiles(eq("v8.0.0-alpha2"), anyString())).thenReturn(
            Stream.of("docs/changelog/2_1234.yaml", "docs/changelog/2_5678.yaml")
        );

        Set<File> allFiles = Set.of(
            new File("docs/changelog/1_1234.yaml"),
            new File("docs/changelog/1_5678.yaml"),
            new File("docs/changelog/2_1234.yaml"),
            new File("docs/changelog/2_5678.yaml"),
            new File("docs/changelog/3_1234.yaml"),
            new File("docs/changelog/3_5678.yaml")
        );

        // when:
        Map<QualifiedVersion, Set<File>> partitionedFiles = GenerateReleaseNotesTask.partitionFiles(gitWrapper, "8.0.0-beta1", allFiles);

        // then:
        verify(gitWrapper).updateRemote("upstream");
        verify(gitWrapper).updateTags("upstream");
        verify(gitWrapper).updateTags("upstream");
        verify(gitWrapper).listVersions("v8.0*");
        verify(gitWrapper).listFiles("v8.0.0-alpha1", "docs/changelog");
        verify(gitWrapper).listFiles("v8.0.0-alpha2", "docs/changelog");

        assertThat(
            partitionedFiles,
            allOf(
                aMapWithSize(3),
                hasKey(QualifiedVersion.of("8.0.0-alpha1")),
                hasKey(QualifiedVersion.of("8.0.0-alpha2")),
                hasKey(QualifiedVersion.of("8.0.0-beta1"))
            )
        );

        assertThat(
            partitionedFiles,
            allOf(
                hasEntry(
                    equalTo(QualifiedVersion.of("8.0.0-alpha1")),
                    containsInAnyOrder(new File("docs/changelog/1_1234.yaml"), new File("docs/changelog/1_5678.yaml"))
                ),
                hasEntry(
                    equalTo(QualifiedVersion.of("8.0.0-alpha2")),
                    containsInAnyOrder(new File("docs/changelog/2_1234.yaml"), new File("docs/changelog/2_5678.yaml"))
                ),
                hasEntry(
                    equalTo(QualifiedVersion.of("8.0.0-beta1")),
                    containsInAnyOrder(new File("docs/changelog/3_1234.yaml"), new File("docs/changelog/3_5678.yaml"))
                )
            )
        );
    }

    /**
     * Check that the task partitions the list of files correctly by version for a patch release.
     */
    @Test
    public void partitionFiles_withPatchRelease_correctlyGroupsByPatchVersion() {
        // given:
        when(gitWrapper.listRemotes()).thenReturn(Map.of("upstream", "elastic/elasticsearch"));
        when(gitWrapper.listVersions(anyString())).thenReturn(
            Stream.of("8.0.0-alpha1", "8.0.0-alpha2", "8.0.0-beta1", "8.0.0-rc1", "8.0.0", "8.0.1", "8.0.2", "8.1.0")
                .map(QualifiedVersion::of)
        );
        when(gitWrapper.listFiles(eq("v8.0.0"), anyString())).thenReturn(
            Stream.of("docs/changelog/1_1234.yaml", "docs/changelog/1_5678.yaml")
        );
        when(gitWrapper.listFiles(eq("v8.0.1"), anyString())).thenReturn(
            Stream.of("docs/changelog/2_1234.yaml", "docs/changelog/2_5678.yaml")
        );

        Set<File> allFiles = Set.of(
            new File("docs/changelog/1_1234.yaml"),
            new File("docs/changelog/1_5678.yaml"),
            new File("docs/changelog/2_1234.yaml"),
            new File("docs/changelog/2_5678.yaml"),
            new File("docs/changelog/3_1234.yaml"),
            new File("docs/changelog/3_5678.yaml")
        );

        // when:
        Map<QualifiedVersion, Set<File>> partitionedFiles = GenerateReleaseNotesTask.partitionFiles(gitWrapper, "8.0.2", allFiles);

        // then:
        verify(gitWrapper).updateRemote("upstream");
        verify(gitWrapper).updateTags("upstream");
        verify(gitWrapper).updateTags("upstream");
        verify(gitWrapper).listVersions("v8.0*");
        verify(gitWrapper).listFiles("v8.0.0", "docs/changelog");
        verify(gitWrapper).listFiles("v8.0.1", "docs/changelog");

        assertThat(
            partitionedFiles,
            allOf(
                aMapWithSize(3),
                hasKey(QualifiedVersion.of("8.0.0")),
                hasKey(QualifiedVersion.of("8.0.1")),
                hasKey(QualifiedVersion.of("8.0.2"))
            )
        );

        assertThat(
            partitionedFiles,
            allOf(
                hasEntry(
                    equalTo(QualifiedVersion.of("8.0.0")),
                    containsInAnyOrder(new File("docs/changelog/1_1234.yaml"), new File("docs/changelog/1_5678.yaml"))
                ),
                hasEntry(
                    equalTo(QualifiedVersion.of("8.0.1")),
                    containsInAnyOrder(new File("docs/changelog/2_1234.yaml"), new File("docs/changelog/2_5678.yaml"))
                ),
                hasEntry(
                    equalTo(QualifiedVersion.of("8.0.2")),
                    containsInAnyOrder(new File("docs/changelog/3_1234.yaml"), new File("docs/changelog/3_5678.yaml"))
                )
            )
        );
    }
}
