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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

import java.io.Reader;
import java.util.Arrays;

/**
 *
 */
public final class CustomAnalyzer extends Analyzer {

    private final TokenizerFactory tokenizerFactory;

    private final CharFilterFactory[] charFilters;

    private final TokenFilterFactory[] tokenFilters;

    private final int positionIncrementGap;
    private final int offsetGap;

    public CustomAnalyzer(TokenizerFactory tokenizerFactory, CharFilterFactory[] charFilters, TokenFilterFactory[] tokenFilters) {
        this(tokenizerFactory, charFilters, tokenFilters, 0, -1);
    }

    public CustomAnalyzer(TokenizerFactory tokenizerFactory, CharFilterFactory[] charFilters, TokenFilterFactory[] tokenFilters,
                          int positionOffsetGap, int offsetGap) {
        this.tokenizerFactory = tokenizerFactory;
        this.charFilters = charFilters;
        this.tokenFilters = tokenFilters;
        this.positionIncrementGap = positionOffsetGap;
        this.offsetGap = offsetGap;
    }


    public TokenizerFactory tokenizerFactory() {
        return tokenizerFactory;
    }

    public TokenFilterFactory[] tokenFilters() {
        return tokenFilters;
    }

    public CharFilterFactory[] charFilters() {
        return charFilters;
    }

    @Override
    public int getPositionIncrementGap(String fieldName) {
        return this.positionIncrementGap;
    }

    @Override
    public int getOffsetGap(String field) {
        if (offsetGap < 0) {
            return super.getOffsetGap(field);
        }
        return this.offsetGap;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = tokenizerFactory.create(reader);
        TokenStream tokenStream = tokenizer;
        for (TokenFilterFactory tokenFilter : tokenFilters) {
            tokenStream = tokenFilter.create(tokenStream);
        }
        return new TokenStreamComponents(tokenizer, tokenStream);
    }

    @Override
    protected Reader initReader(String fieldName, Reader reader) {
        if (charFilters != null && charFilters.length > 0) {
            for (CharFilterFactory charFilter : charFilters) {
                reader = charFilter.create(reader);
            }
        }
        return reader;
    }
    
    public CustomAnalyzer copyWithoutShingleFilter() {
        int[] tokenFiltersToRemove = new int[tokenFilters.length];
        Arrays.fill(tokenFiltersToRemove, -1);
        int removeIndex = 0;
        int i;
        for (i = 0; i < tokenFilters.length; i++) {
            TokenFilterFactory tokenFilterFactory = tokenFilters[i];
            if (tokenFilterFactory instanceof ShingleTokenFilterFactory) {
                tokenFiltersToRemove[removeIndex++] = i;
            } else if (tokenFilterFactory instanceof ShingleTokenFilterFactory.Factory) {
                tokenFiltersToRemove[removeIndex++] = i;
            }
        }
        if (removeIndex == 0) {
            // Nothing to remove, just return this analyzer
            return this;
        }
        TokenFilterFactory[] newTokenFilters = new TokenFilterFactory[tokenFilters.length - removeIndex + 1];
        removeIndex = 0;
        int newIndex = 0;
        for (i = 0; i < tokenFilters.length; i++) {
            if (i == tokenFiltersToRemove[removeIndex]) {
                removeIndex++;
            } else {
                newTokenFilters[newIndex++] = tokenFilters[i];
            }
        }
        return new CustomAnalyzer(tokenizerFactory, charFilters, newTokenFilters, positionIncrementGap, offsetGap);
    }
}
