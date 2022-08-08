/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.scanners;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.plugins.PluginBundle;
import org.elasticsearch.plugins.PluginsService;
import org.objectweb.asm.ClassReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class ClassReaders {

    public static Stream<ClassReader> ofModuleAndClassPaths() throws IOException {
        return Stream.concat(ofClassPath(), ofModulePath());
    }

    public static Stream<ClassReader> ofClassPath() throws IOException {
        String classpath = System.getProperty("java.class.path");
        String[] pathelements = classpath.split(":");
        return ofPaths(Arrays.stream(pathelements).map(Path::of));
    }

    public static Stream<ClassReader> ofModulePath() throws IOException {
        String modulePath = System.getProperty("jdk.module.path");
        return ofDirWithJars(modulePath);
    }

    public static Stream<ClassReader> ofDirWithJars(String path) throws IOException {
        Stream<Path> list = Files.list(Path.of(path));
        return ofPaths(list);
    }

    public static Stream<ClassReader> ofPaths(Stream<Path> list) {
        return list.filter(Files::exists)
            .flatMap(p -> {
                if (p.toString().endsWith(".jar")) {
                    return classesInJar(p);
                } else {
                    return classesInPath(p);
                }
            });
    }

    private static Stream<ClassReader> classesInJar(Path jar) {
        try {
            FileSystem jarFs = FileSystems.newFileSystem(jar);
            Path root = jarFs.getPath("/");
            return classesInPath(root);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Stream.empty();

    }

    private static Stream<ClassReader> classesInPath(Path root) {
        try { // todo what happens here??
            Stream<Path> stream = Files.walk(root);
            return stream.filter(p -> p.toString().endsWith(".class"))
                .filter(p -> p.toString().endsWith("module-info.class") == false)
                .map(p -> {
                    try (InputStream is = Files.newInputStream(p)) {
                        byte[] classBytes = is.readAllBytes();
                        return new ClassReader(classBytes);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Stream.empty();
    }


    public static Stream<ClassReader> ofBundle(PluginBundle bundle) {
        return ofPaths(bundle.allUrls.stream().map(PluginsService::uncheckedToURI).map(PathUtils::get));
    }
}
