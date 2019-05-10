/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

import org.elasticsearch.gradle.precommit.DependencyLicensesTask;
import org.gradle.api.DefaultTask;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.DependencySet;
import org.gradle.api.artifacts.ResolvedArtifact;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

/**
 * A task to gather information about the dependencies and export them into a csv file.
 *
 * The following information is gathered:
 * <ul>
 *     <li>name: name that identifies the library (groupId:artifactId)</li>
 *     <li>version</li>
 *     <li>URL: link to have more information about the dependency.</li>
 *     <li>license: <a href="https://spdx.org/licenses/">SPDX license</a> identifier, custom license or UNKNOWN.</li>
 * </ul>
 *
 */
public class DependenciesInfoTask extends DefaultTask {
    
    public DependenciesInfoTask() {
        setDescription("Create a CSV file with dependencies information.");
    }
    
    private final Logger logger = Logging.getLogger(getClass());
    
    /** Dependencies to gather information from. */
    private Configuration runtimeConfiguration;

    /** We subtract compile-only dependencies. */
    private Configuration compileOnlyConfiguration;
    
    private LinkedHashMap<String, String> mappings;

    /** Directory to read license files */
    private File licensesDir = new File(getProject().getProjectDir(), "licenses");

    private File outputFile = new File(getProject().getBuildDir(), "reports/dependencies/dependencies.csv");

    @Input
    public Configuration getRuntimeConfiguration() {
        return runtimeConfiguration;
    }

    public void setRuntimeConfiguration(Configuration runtimeConfiguration) {
        this.runtimeConfiguration = runtimeConfiguration;
    }

    @Input
    public Configuration getCompileOnlyConfiguration() {
        return compileOnlyConfiguration;
    }

    public void setCompileOnlyConfiguration(Configuration compileOnlyConfiguration) {
        this.compileOnlyConfiguration = compileOnlyConfiguration;
    }

    @Input
    public LinkedHashMap<String, String> getMappings() {
        return mappings;
    }

    public void setMappings(LinkedHashMap<String, String> mappings) {
        this.mappings = mappings;
    }

    @InputDirectory
    public File getLicensesDir() {
        return licensesDir;
    }

    public void setLicensesDir(File licensesDir) {
        this.licensesDir = licensesDir;
    }

    @OutputFile
    public File getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(File outputFile) {
        this.outputFile = outputFile;
    }
    @TaskAction
    public void generateDependenciesInfo() throws IOException {

        final DependencySet runtimeDependencies = runtimeConfiguration.getAllDependencies();
        // we have to resolve the transitive dependencies and create a group:artifactId:version map
        final Set<ResolvedArtifact> compileOnlyArtifactsSet =
                compileOnlyConfiguration
                .getResolvedConfiguration()
                .getResolvedArtifacts();
        final Set<String> compileOnlyArtifacts = new HashSet<>();
        compileOnlyArtifactsSet.forEach(ra -> compileOnlyArtifacts.add(
                ra.getModuleVersion().getId().getGroup() + ":" +
                ra.getModuleVersion().getId().getName() + ":" + 
                ra.getModuleVersion().getId().getVersion()));

        final StringBuilder output = new StringBuilder();

        for (final Dependency dependency : runtimeDependencies) {
            // we do not need compile-only dependencies here
            if (compileOnlyArtifacts.contains(dependency.getGroup() + ":" +
                                              dependency.getName() + ":" +
                                              dependency.getVersion())) {
                continue;
            }
            // only external dependencies are checked
            if (dependency.getGroup() != null && dependency.getGroup().contains("org.elasticsearch")) {
                continue;
            }

            final String url = createURL(dependency.getGroup(), dependency.getName(), dependency.getVersion());
            final String dependencyName = DependencyLicensesTask.getDependencyName(mappings, dependency.getName());
            logger.info("mapped dependency " + dependency.getGroup() + ":" + dependency.getName() + " to " + dependencyName + " for license info");

            final String licenseType = getLicenseType(dependency.getGroup(), dependencyName);
            output.append(dependency.getGroup() + ":" + dependency.getName() + "," + dependency.getVersion() + "," + url + "," + licenseType + "\n");

        }
        Files.write(outputFile.toPath(), output.toString().getBytes(StandardCharsets.UTF_8));
    }
    
    /**
     * Create an URL on <a href="https://repo1.maven.org/maven2/">Maven Central</a>
     * based on dependency coordinates.
     */
    protected String createURL(final String group, final String name, final String version){
        final String baseURL = "https://repo1.maven.org/maven2";
        return baseURL + "/" + group.replaceAll("\\.", "/") + "/" + name + "/" + version;
    }
    
    /**
     * Read the LICENSE file associated with the dependency and determine a license type.
     *
     * The license type is one of the following values:
     * <u>
     *     <li><em>UNKNOWN</em> if LICENSE file is not present for this dependency.</li>
     *     <li><em>one SPDX identifier</em> if the LICENSE content matches with an SPDX license.</li>
     *     <li><em>Custom;URL</em> if it's not an SPDX license,
     *          URL is the Github URL to the LICENSE file in elasticsearch repository.</li>
     * </ul>
     *
     * @param group dependency group
     * @param name dependency name
     * @return SPDX identifier, UNKNOWN or a Custom license
     */
    protected String getLicenseType(final String group, final String name) throws IOException {
        File license = null;

        if (licensesDir.exists()) {
            File[] matchedfiles = licensesDir.listFiles((dir, fname) -> fname.contains("-LICENSE"));
            for(File file : matchedfiles) {
                String prefix = file.getName().split("-LICENSE.*")[0];
                if (group.contains(prefix) || name.contains(prefix)) {
                    license = file.getAbsoluteFile();
                }
            }
        }

        if (license != null) {
            // replace * because they are sometimes used at the beginning lines as if the license was a multi-line comment
            final String content = new String(Files.readAllBytes(outputFile.toPath()), StandardCharsets.UTF_8);
            content.replaceAll("\\s+", " ").replaceAll("\\*", " ");
            final String spdx = checkSPDXLicense(content);
            if (spdx ==  null) {
                // License has not be identified as SPDX.
                // As we have the license file, we create a Custom entry with the URL to this license file.
                final String gitBranch = System.getProperty("build.branch", "master");
                final String githubBaseURL = "https://raw.githubusercontent.com/elastic/elasticsearch/" + gitBranch + "/";
                return "Custom;" + license.getCanonicalPath().replaceFirst(".*/elasticsearch/", githubBaseURL);
            }
            return spdx;
        } else {
            return "UNKNOWN";
        }
    }
    
    /**
     * Check the license content to identify an SPDX license type.
     *
     * @param licenseText LICENSE file content.
     * @return SPDX identifier or null.
     */
    protected String checkSPDXLicense(final String licenseText) {
        String spdx = null;

        final String APACHE_2_0 = "Apache.*License.*(v|V)ersion.*2\\.0";
        final String BSD_2 = 
                "Redistribution and use in source and binary forms, with or without\\s*" + 
                "modification, are permitted provided that the following conditions\\s*" + 
                "are met:\\s*" + 
                "1\\. Redistributions of source code must retain the above copyright\\s*" +
                "notice, this list of conditions and the following disclaimer.\\s*" +
                "2\\. Redistributions in binary form must reproduce the above copyright\\s*" + 
                "notice, this list of conditions and the following disclaimer in the\\s*" + 
                "documentation and/or other materials provided with the distribution.\\s*" + 
                "THIS SOFTWARE IS PROVIDED BY .+ (``|''|\")AS IS(''|\") AND ANY EXPRESS OR\\s*" + 
                "IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES\\s*" + 
                "OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED\\.\\s*" + 
                "IN NO EVENT SHALL .+ BE LIABLE FOR ANY DIRECT, INDIRECT,\\s*" + 
                "INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES \\(INCLUDING, BUT\\s*" + 
                "NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,\\s*" + 
                "DATA, OR PROFITS; OR BUSINESS INTERRUPTION\\) HOWEVER CAUSED AND ON ANY\\s*" + 
                "THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\\s*" + 
                "\\(INCLUDING NEGLIGENCE OR OTHERWISE\\) ARISING IN ANY WAY OUT OF THE USE OF\\s*" + 
                "THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE\\.";
                BSD_2.replaceAll("\\s+", "\\\\s*");
        final String BSD_3 = 
                "Redistribution and use in source and binary forms, with or without\\s*" + 
                "modification, are permitted provided that the following conditions\\s*" + 
                "are met:\\s*" +  
                "(1\\.)? Redistributions of source code must retain the above copyright\\s*" + 
                "notice, this list of conditions and the following disclaimer\\.\\s*" + 
                "(2\\.)? Redistributions in binary form must reproduce the above copyright\\s*" + 
                "notice, this list of conditions and the following disclaimer in the\\s*" + 
                "documentation and/or other materials provided with the distribution\\.\\s*" + 
                "((3\\.)? The name of .+ may not be used to endorse or promote products\\s*" + 
                "derived from this software without specific prior written permission\\.|" + 
                "(3\\.)? Neither the name of .+ nor the names of its\\s*" + 
                "contributors may be used to endorse or promote products derived from\\s*" + 
                "this software without specific prior written permission\\.)\\s*" + 
                "THIS SOFTWARE IS PROVIDED BY .+ (``|''|\")AS IS(''|\") AND ANY EXPRESS OR\\s*" + 
                "IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES\\s*" + 
                "OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED\\.\\s*" + 
                "IN NO EVENT SHALL .+ BE LIABLE FOR ANY DIRECT, INDIRECT,\\s*" + 
                "INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES \\(INCLUDING, BUT\\s*" + 
                "NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,\\s*" + 
                "DATA, OR PROFITS; OR BUSINESS INTERRUPTION\\) HOWEVER CAUSED AND ON ANY\\s*" + 
                "THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\\s*" + 
                "\\(INCLUDING NEGLIGENCE OR OTHERWISE\\) ARISING IN ANY WAY OUT OF THE USE OF\\s*" + 
                "THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE\\.";
                BSD_3.replaceAll("\\s+", "\\\\s*");
        final String CDDL_1_0 = "COMMON DEVELOPMENT AND DISTRIBUTION LICENSE.*Version 1.0";
        final String CDDL_1_1 = "COMMON DEVELOPMENT AND DISTRIBUTION LICENSE.*Version 1.1";
        final String ICU = "ICU License - ICU 1.8.1 and later";
        final String LGPL_3 = "GNU LESSER GENERAL PUBLIC LICENSE.*Version 3";
        final String MIT = 
                "Permission is hereby granted, free of charge, to any person obtaining a copy of\\s*" + 
                "this software and associated documentation files \\(the \"Software\"\\), to deal in\\s*" + 
                "the Software without restriction, including without limitation the rights to\\s*" + 
                "use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies\\s*" + 
                "of the Software, and to permit persons to whom the Software is furnished to do\\s*" + 
                "so, subject to the following conditions:\\s*" +  
                "The above copyright notice and this permission notice shall be included in all\\s*" + 
                "copies or substantial portions of the Software\\.\\s*" + 
                "THE SOFTWARE IS PROVIDED \"AS IS\", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR\\s*" + 
                "IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,\\s*" + 
                "FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT\\. IN NO EVENT SHALL THE\\s*" + 
                "AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER\\s*" + 
                "LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,\\s*" + 
                "OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE\\s*" + 
                "SOFTWARE\\.";
                MIT.replaceAll("\\s+", "\\\\s*");
        final String MOZILLA_1_1 = "Mozilla Public License.*Version 1.1";

        final String MOZILLA_2_0 = "Mozilla\\s*Public\\s*License\\s*Version\\s*2\\.0";
        
        if(licenseText.matches(".*" + APACHE_2_0 + ".*")) {
            spdx = "Apache-2.0";
        }else if(licenseText.matches(".*" + MIT + ".*")) {
            spdx = "MIT";
        }else if(licenseText.matches(".*" + BSD_2 + ".*")) {
            spdx = "BSD-2-Clause";
        }else if(licenseText.matches(".*" + BSD_3 + ".*")) {
            spdx = "BSD-3-Clause";
        }else if(licenseText.matches(".*" + LGPL_3 + ".*")) {
            spdx = "LGPL-3.0";
        }else if(licenseText.matches(".*" + CDDL_1_0 + ".*")) {
            spdx = "CDDL-1.0";
        }else if(licenseText.matches(".*" + CDDL_1_1 + ".*")) {
            spdx = "CDDL-1.1";
        }else if(licenseText.matches(".*" + ICU + ".*")) {
            spdx = "ICU";
        }else if(licenseText.matches(".*" + MOZILLA_1_1 + ".*")) {
            spdx = "MPL-1.1";
        }else if(licenseText.matches(".*" + MOZILLA_2_0 + ".*")) {
            spdx = "MPL-2.0";
        }
        return spdx;
    }
}

