/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CustomAnalyzerWrapper;

/**
 * Named analyzer is an analyzer wrapper around an actual analyzer ({@link #analyzer} that is associated
 * with a name ({@link #name()}.
 */
public class NamedAnalyzer extends CustomAnalyzerWrapper {

    private final String name;

    private final AnalyzerScope scope;

    private final Analyzer analyzer;

    public NamedAnalyzer(String name, Analyzer analyzer) {
        this(name, AnalyzerScope.INDEX, analyzer);
    }

    public NamedAnalyzer(String name, AnalyzerScope scope, Analyzer analyzer) {
        this.name = name;
        this.scope = scope;
        this.analyzer = analyzer;
    }

    /**
     * The name of the analyzer.
     */
    public String name() {
        return this.name;
    }

    /**
     * The scope of the analyzer.
     */
    public AnalyzerScope scope() {
        return this.scope;
    }

    /**
     * The actual analyzer.
     */
    public Analyzer analyzer() {
        return this.analyzer;
    }

    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
        return this.analyzer;
    }

    @Override
    protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
        return components;
    }

    @Override
    public String toString() {
        return "analyzer name[" + name + "], analyzer [" + analyzer + "]";
    }
}
