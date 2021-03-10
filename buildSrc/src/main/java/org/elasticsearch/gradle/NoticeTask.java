/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle;

import org.apache.commons.io.FileUtils;
import org.codehaus.groovy.runtime.ResourceGroovyMethods;
import org.codehaus.groovy.runtime.StringGroovyMethods;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileTree;
import org.gradle.api.file.SourceDirectorySet;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.commons.io.FileUtils.readFileToString;

/**
 * A task to create a notice file which includes dependencies' notices.
 */
public class NoticeTask extends DefaultTask {

    @InputFile
    private File inputFile = getProject().getRootProject().file("NOTICE.txt");
    @OutputFile
    private File outputFile = new File(getProject().getBuildDir(), "notices/" + getName() + "/NOTICE.txt");
    private FileTree sources;
    /**
     * Directories to include notices from
     */
    private List<File> licensesDirs = new ArrayList<File>();

    public NoticeTask() {
        setDescription("Create a notice file from dependencies");
        // Default licenses directory is ${projectDir}/licenses (if it exists)
        File licensesDir = new File(getProject().getProjectDir(), "licenses");
        if (licensesDir.exists()) {
            licensesDirs.add(licensesDir);
        }
    }

    /**
     * Add notices from the specified directory.
     */
    public void licensesDir(File licensesDir) {
        licensesDirs.add(licensesDir);
    }

    public void source(Object source) {
        if (sources == null) {
            sources = getProject().fileTree(source);
        } else {
            sources = sources.plus(getProject().fileTree(source));
        }

    }

    public void source(SourceDirectorySet source) {
        if (sources == null) {
            sources = source;
        } else {
            sources = sources.plus(source);
        }

    }

    @TaskAction
    public void generateNotice() throws IOException {
        StringBuilder output = new StringBuilder();
        output.append(readFileToString(inputFile, "UTF-8"));
        output.append("\n\n");
        // This is a map rather than a set so that the sort order is the 3rd
        // party component names, unaffected by the full path to the various files
        final Map<String, File> seen = new TreeMap<String, File>();
        FileCollection noticeFiles = getNoticeFiles();
        if(noticeFiles != null) {
            for (File file : getNoticeFiles()) {
                String name = file.getName().replaceFirst(" -NOTICE\\.txt$ ", "");
                if (seen.containsKey(name)) {
                    File prevFile = seen.get(name);
                    String previousFileText = readFileToString(prevFile, "UTF-8");
                    if (previousFileText.equals(readFileToString(file, "UTF-8")) == false) {
                        throw new RuntimeException("Two different notices exist for dependency '" + name + "': " + prevFile + " and " + file);
                    }
                } else {
                    seen.put(name, file);
                }
            }
        }

        // Add all LICENSE and NOTICE files in licenses directory
        for (Map.Entry<String, File> entry : seen.entrySet()) {
            final String name = entry.getKey();
            File file = entry.getValue();
            appendFile(file, name, "NOTICE", output);
            appendFile(new File(file.getParentFile(), name + "-LICENSE.txt"), name, "LICENSE", output);
        }

        // Find any source files with "@notice" annotated license header
        for (File sourceFile : sources.getFiles()) {
            boolean isPackageInfo = sourceFile.getName().equals("package-info.java");
            boolean foundNotice = false;
            boolean inNotice = false;
            StringBuilder header = new StringBuilder();
            String packageDeclaration = null;

            for (String line : FileUtils.readLines(sourceFile)) {
                if (isPackageInfo && packageDeclaration == null && line.startsWith("package")) {
                    packageDeclaration = line;
                }

                if (foundNotice == false) {
                    foundNotice = line.contains("@notice");
                    inNotice = true;
                } else {
                    if (line.contains("*/")) {
                        inNotice = false;

                        if (isPackageInfo == false) {
                            break;
                        }

                    } else if (inNotice) {
                        header.append(StringGroovyMethods.stripMargin(line, "*"));
                        header.append("\n");
                    }
                }
            }

            if (foundNotice) {
                appendText(header.toString(), isPackageInfo ? packageDeclaration : sourceFile.getName(), "", output);
            }
        }

        FileUtils.write(outputFile, output.toString(), "UTF-8");
    }

    @InputFiles
    @Optional
    public FileCollection getNoticeFiles() {
        FileTree tree = null;
        for (File dir : licensesDirs) {
            if (tree == null) {
                tree = getProject().fileTree(dir);
            } else {
                tree = tree.plus(getProject().fileTree(dir));
            }
        }
        return tree == null ? null : tree.matching(patternFilterable -> patternFilterable.include("**/*-NOTICE.txt"));
    }

    @InputFiles
    @Optional
    public FileCollection getSources() {
        return sources;
    }

    public static void appendFile(File file, String name, String type, StringBuilder output) throws IOException {
        String text = ResourceGroovyMethods.getText(file, "UTF-8");
        if (text.trim().isEmpty()) {
            return;
        }
        appendText(text, name, type, output);
    }

    public static void appendText(String text, final String name, final String type, StringBuilder output) {
        output.append("================================================================================\n");
        output.append(name + " " + type + "\n");
        output.append("================================================================================\n");
        output.append(text);
        output.append("\n\n");
    }

    public File getInputFile() {
        return inputFile;
    }

    public void setInputFile(File inputFile) {
        this.inputFile = inputFile;
    }

    public File getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(File outputFile) {
        this.outputFile = outputFile;
    }

}
