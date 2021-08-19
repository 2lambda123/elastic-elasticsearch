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

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.mockito.Matchers.anyString;
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
     * Check that no files are ignored when the current version is a snapshot.
     */
    @Test
    public void getFilesToIgnore_withSnapshot_returnsNothing() {
        // when:
        Set<String> filesToIgnore = GenerateReleaseNotesTask.getFilesToIgnore(gitWrapper, "8.0.0-SNAPSHOT");

        // then:
        assertThat(filesToIgnore, empty());
        verifyZeroInteractions(gitWrapper);
    }

    /**
     * Check that no files are ignored for the first version in a minor series.
     */
    @Test
    public void getFilesToIgnore_withNoPrerelease_returnsNothing() {
        Stream.of("7.0.0", "8.1.0", "9.2.0").forEach(version -> {
            // when:
            Set<String> filesToIgnore = GenerateReleaseNotesTask.getFilesToIgnore(gitWrapper, version);

            // then:
            assertThat(filesToIgnore, empty());
            verifyZeroInteractions(gitWrapper);
        });
    }

    /**
     * Check that no files are ignored when a version is the first alpha prerelease.
     */
    @Test
    public void getFilesToIgnore_withFirstAlpha_returnsNothing() {
        // when:
        Set<String> filesToIgnore = GenerateReleaseNotesTask.getFilesToIgnore(gitWrapper, "8.0.0-alpha1");

        // then:
        assertThat(filesToIgnore, empty());
        verifyZeroInteractions(gitWrapper);
    }

    /**
     * Check that the git wrapper throws an error if it can't find the right git remote.
     */
    @Test
    public void getFilesToIgnore_withoutGitRemote_throwsError() {
        // when:
        when(gitWrapper.listRemotes()).thenReturn(Map.of("fred", "fred/elasticsearch"));
        GradleException exception = expectThrows(
            GradleException.class,
            () -> GenerateReleaseNotesTask.getFilesToIgnore(gitWrapper, "8.0.0-alpha2")
        );

        // then:
        assertThat(exception.getMessage(), containsString("I need to ensure the git tags are up-to-date"));
    }

    /**
     * Check that the task identifies the expected list of files for a prerelease. It should select the immediately
     * preceding prerelease version, and use the file tree from that tag.
     */
    @Test
    public void getFilesToIgnore_withPrerelease_returnsListOfFiles() {
        // given:
        when(gitWrapper.listRemotes()).thenReturn(Map.of("fred", "fred/elasticsearch", "upstream", "elastic/elasticsearch"));
        when(gitWrapper.listVersions(anyString())).thenReturn(
            Stream.of("8.0.0-alpha1", "8.0.0-alpha2", "8.0.0-beta1", "8.0.0-beta2", "8.0.0-beta3", "8.0.0-rc1", "8.0.0")
                .map(QualifiedVersion::of)
        );
        when(gitWrapper.listFiles(anyString(), anyString())).thenReturn(Stream.of("docs/changelog/1234.yml", "docs/changelog/5678.yaml"));

        // when:
        Set<String> filesToIgnore = GenerateReleaseNotesTask.getFilesToIgnore(gitWrapper, "8.0.0-beta2");

        // then:
        verify(gitWrapper).updateRemote("upstream");
        verify(gitWrapper).updateTags("upstream");
        verify(gitWrapper).updateTags("upstream");
        verify(gitWrapper).listVersions("v8.0*");
        // The expected version here is just before the version that we pass to `getFilesToIgnore()`
        verify(gitWrapper).listFiles("v8.0.0-beta1", "docs/changelog");
        assertThat(filesToIgnore, containsInAnyOrder("1234.yml", "5678.yaml"));
    }

    /**
     * Check that the task identifies the expected list of files for a patch release. It should select the immediately
     * preceding version, and use the file tree from that tag.
     */
    @Test
    public void getFilesToIgnore_withPatchRelease_returnsListOfFiles() {
        // given:
        when(gitWrapper.listRemotes()).thenReturn(Map.of("upstream", "elastic/elasticsearch"));
        when(gitWrapper.listVersions(anyString())).thenReturn(
            Stream.of("8.0.0-alpha1", "8.0.0-alpha2", "8.0.0-beta1", "8.0.0-rc1", "8.0.0", "8.0.1", "8.0.2", "8.1.0")
                .map(QualifiedVersion::of)
        );
        // Version here is just before the version that we pass to `getFilesToIgnore`
        when(gitWrapper.listFiles(anyString(), anyString())).thenReturn(Stream.of("docs/changelog/1234.yml", "docs/changelog/5678.yaml"));

        // when:
        Set<String> filesToIgnore = GenerateReleaseNotesTask.getFilesToIgnore(gitWrapper, "8.0.2");

        // then:
        verify(gitWrapper).listVersions("v8.0*");
        // The expected version here is just before the version that we pass to `getFilesToIgnore()`
        verify(gitWrapper).listFiles("v8.0.1", "docs/changelog");
        assertThat(filesToIgnore, containsInAnyOrder("1234.yml", "5678.yaml"));
    }
}
