/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.release;

import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.util.PatternSet;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class GenerateReleaseNotesTask extends DefaultTask {
    private static final Logger LOGGER = Logging.getLogger(GenerateReleaseNotesTask.class);

    private final ConfigurableFileCollection changelogs = getProject().getObjects().fileCollection();
    private final RegularFileProperty releaseNotesFile = getProject().getObjects().fileProperty();
    private final RegularFileProperty releaseHighlightsFile = getProject().getObjects().fileProperty();
    private final RegularFileProperty breakingChangesFile = getProject().getObjects().fileProperty();

    public GenerateReleaseNotesTask() {
        final Version version = VersionProperties.getElasticsearchVersion();

        this.changelogs.setFrom(
            getProject().getLayout()
                .getProjectDirectory()
                .dir("docs/changelog")
                .getAsFileTree()
                .matching(new PatternSet().include("**/*.yml", "**/*.yaml"))
                .getFiles()
        );

        this.releaseNotesFile.set(
            getProject().getLayout()
                .getProjectDirectory()
                .file(String.format("docs/reference/release-notes/%d.%d.asciidoc", version.getMajor(), version.getMinor()))
        );

        this.releaseHighlightsFile.set(
            getProject().getLayout().getProjectDirectory().file("docs/reference/release-notes/highlights.asciidoc")
        );

        this.breakingChangesFile.set(
            getProject().getLayout()
                .getProjectDirectory()
                .file(String.format("docs/reference/migration/migrate_%d_%d.asciidoc", version.getMajor(), version.getMinor()))
        );
    }

    @InputFiles
    public FileCollection getChangelogs() {
        return changelogs;
    }

    @OutputFile
    public RegularFileProperty getReleaseNotesFile() {
        return releaseNotesFile;
    }

    @OutputFile
    public RegularFileProperty getReleaseHighlightsFile() {
        return releaseHighlightsFile;
    }

    @OutputFile
    public RegularFileProperty getBreakingChangesFile() {
        return breakingChangesFile;
    }

    @TaskAction
    public void executeTask() throws IOException {
        LOGGER.info("Finding changelog files...");

        final Version elasticsearchVersion = VersionProperties.getElasticsearchVersion();

        final List<ChangelogEntry> entries = this.changelogs.getFiles()
            .stream()
            .map(ChangelogEntry::parseChangelog)
            .filter(
                // Only process changelogs that are included in this minor version series of ES.
                // If this change was released in an earlier major or minor version of Elasticsearch, do not
                // include it in the notes. An absence of versions indicates that this change is only
                // for the current release. An earlier patch version is OK, the release notes include changes
                // for every patch release in a minor series.
                log -> {
                    final List<Version> changelogVersions = log.getVersions()
                        .stream()
                        .map(v -> Version.fromString(v, Version.Mode.RELAXED))
                        .collect(Collectors.toList());

                    final Predicate<Version> includedInSameMinor = v -> v.getMajor() == elasticsearchVersion.getMajor()
                        && v.getMinor() == elasticsearchVersion.getMinor();

                    final Predicate<Version> includedInEarlierMajorOrMinor = v -> v.getMajor() < elasticsearchVersion.getMajor()
                        || v.getMinor() < elasticsearchVersion.getMinor();

                    boolean includedInThisMinor = changelogVersions.stream().anyMatch(includedInSameMinor);

                    if (includedInThisMinor) {
                        return false == changelogVersions.stream().anyMatch(includedInEarlierMajorOrMinor);
                    } else {
                        return false;
                    }
                }
            )
            .collect(Collectors.toList());

        try (ReleaseNotesGenerator generator = new ReleaseNotesGenerator(this.releaseNotesFile.get().getAsFile())) {
            generator.generate(entries);
        }

        try (ReleaseHighlightsGenerator generator = new ReleaseHighlightsGenerator(this.releaseHighlightsFile.get().getAsFile())) {
            generator.generate(entries);
        }

        try (BreakingChangesGenerator generator = new BreakingChangesGenerator(this.breakingChangesFile.get().getAsFile())) {
            generator.generate(entries);
        }
    }
}
