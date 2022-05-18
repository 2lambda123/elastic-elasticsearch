/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.TaskProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * Distribution level checks for Elasticsearch Java modules, i.e. modular jar files.
 * Currently, ES modular jar files are in the lib directory.
 */
public class InternalDistributionModuleCheckTaskProvider {

    private static final Logger LOGGER = Logging.getLogger(InternalDistributionModuleCheckTaskProvider.class);

    private static final String MODULE_INFO = "module-info.class";

    private static final String ES_JAR_PREFIX = "elasticsearch-";

    /** ES jars in the lib directory that are not modularized. For now, es-log4j is the only one. */
    private static final List<String> ES_JAR_EXCLUDES = List.of("elasticsearch-log4j");

    /** List of the current Elasticsearch Java Modules, by name. */
    private static final List<String> EXPECTED_ES_MODUlES = List.of(
        "org.elasticsearch.base",
        "org.elasticsearch.cli",
        "org.elasticsearch.geo",
        "org.elasticsearch.lz4",
        "org.elasticsearch.plugin.classloader",
        "org.elasticsearch.secure_sm",
        "org.elasticsearch.server",
        "org.elasticsearch.xcontent"
    );

    private static final Class<?> THIS_CLASS = InternalDistributionModuleCheckTaskProvider.class;

    private static final Predicate<ModuleReference> isESModule = mref -> mref.descriptor().name().startsWith("org.elasticsearch");

    private static Predicate<Path> isESJar = path -> path.getFileName().toString().startsWith(ES_JAR_PREFIX);

    private static Predicate<Path> isNotExcluded = path -> ES_JAR_EXCLUDES.stream()
        .filter(path.getFileName().toString()::startsWith)
        .findAny()
        .isEmpty();

    private static final Function<ModuleReference, String> toName = mref -> mref.descriptor().name();

    private InternalDistributionModuleCheckTaskProvider() {};

    /** Registers the checkModules tasks, which contains all checks relevant to ES Java Modules. */
    static TaskProvider<Task> registerCheckModulesTask(Project project, TaskProvider<Copy> checkExtraction) {
        return project.getTasks().register("checkModules", task -> {
            task.dependsOn(checkExtraction);
            task.doLast(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    final Path libPath = checkExtraction.get()
                        .getDestinationDir()
                        .toPath()
                        .resolve("elasticsearch-" + VersionProperties.getElasticsearch())
                        .resolve("lib");
                    try {
                        assertAllESJarsAreModular(libPath);
                        assertAllModulesPresent(libPath);
                        assertModuleVersions(libPath, VersionProperties.getElasticsearch());
                        assertModuleServices(libPath);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            });
        });
    }

    /** Checks that all expected ES jar files are modular, i.e. contain a module-info.class in their root. */
    private static void assertAllESJarsAreModular(Path libPath) throws IOException {
        try (var s = Files.walk(libPath, 1)) {
            s.filter(Files::isRegularFile).filter(isESJar).filter(isNotExcluded).sorted().forEach(path -> {
                try (JarFile jf = new JarFile(path.toFile())) {
                    JarEntry entry = jf.getJarEntry(MODULE_INFO);
                    if (entry == null) {
                        throw new GradleException(MODULE_INFO + " no found in " + path);
                    }
                } catch (IOException e) {
                    throw new GradleException("Failed when reading jar file " + path, e);
                }
            });
        }
    }

    /** Checks that all expected Elasticsearch modules are present. */
    private static void assertAllModulesPresent(Path libPath) {
        List<String> actualESModules = ModuleFinder.of(libPath).findAll().stream().filter(isESModule).map(toName).sorted().toList();
        if (actualESModules.equals(EXPECTED_ES_MODUlES) == false) {
            throw new GradleException(
                "expected modules " + listToString(EXPECTED_ES_MODUlES) + ", \nactual modules " + listToString(actualESModules)
            );
        }
    }

    /** Checks that all expected Elasticsearch modules have the expected versions. */
    private static void assertModuleVersions(Path libPath, String expectedVersion) {
        List<ModuleReference> esModules = ModuleFinder.of(libPath)
            .findAll()
            .stream()
            .filter(isESModule)
            .sorted(Comparator.comparing(ModuleReference::descriptor))
            .toList();
        for (ModuleReference mref : esModules) {
            String mVersion = mref.descriptor()
                .rawVersion()
                .orElseThrow(() -> new GradleException("no version found in module " + mref.descriptor().name()));
            if (mVersion != expectedVersion) {
                new GradleException("Expected version [" + expectedVersion + "], in " + mref.descriptor());
            }
        }
    }

    /** Checks that all modules have, at least, the META-INF services declared. */
    private static void assertModuleServices(Path libPath) throws IOException {
        List<ModuleReference> esModules = ModuleFinder.of(libPath)
            .findAll()
            .stream()
            .filter(isESModule) // TODO: how to open this to all, including plugins and modules
            .sorted(Comparator.comparing(ModuleReference::descriptor))
            .toList();

        for (ModuleReference mref : esModules) {
            URI uri = URI.create("jar:" + mref.location().get());
            Set<String> modServices = mref.descriptor().provides().stream().map(ModuleDescriptor.Provides::service).collect(toSet());
            try (var fileSystem = FileSystems.newFileSystem(uri, Map.of(), THIS_CLASS.getClassLoader())) {
                Path servicesRoot = fileSystem.getPath("/META-INF/services");
                if (Files.exists(servicesRoot)) {
                    Files.walk(servicesRoot)
                        .filter(Files::isRegularFile)
                        .map(p -> servicesRoot.relativize(p))
                        .map(Path::toString)
                        .forEach(service -> {
                            if (modServices.contains(service) == false) {
                                throw new GradleException(
                                    "Expected provides %s in module %s with provides %s.".formatted(
                                        service,
                                        mref.descriptor().name(),
                                        mref.descriptor().provides()
                                    )
                                );
                            }
                        });
                }
            }
        }
    }

    // ####: eventually assert hashes, etc

    static String listToString(List<String> list) {
        return list.stream().sorted().collect(joining("\n  ", "[\n  ", "]"));
    }
}
