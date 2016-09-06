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

package org.elasticsearch.indices.analysis;

import org.apache.lucene.analysis.minhash.MinHashFilterFactory;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.NamedRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.*;
import org.elasticsearch.index.analysis.compound.DictionaryCompoundWordTokenFilterFactory;
import org.elasticsearch.index.analysis.compound.HyphenationCompoundWordTokenFilterFactory;
import org.elasticsearch.plugins.AnalysisPlugin;

import java.io.IOException;
import java.util.List;

/**
 * Sets up {@link AnalysisRegistry}.
 */
public final class AnalysisModule {
    static {
        Settings build = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .build();
        IndexMetaData metaData = IndexMetaData.builder("_na_").settings(build).build();
        NA_INDEX_SETTINGS = new IndexSettings(metaData, Settings.EMPTY);
    }
    private static final IndexSettings NA_INDEX_SETTINGS;

    private final HunspellService hunspellService;
    private final AnalysisRegistry analysisRegistry;

    public AnalysisModule(Environment environment, List<AnalysisPlugin> plugins) throws IOException {
        NamedRegistry<AnalysisProvider<CharFilterFactory>> charFilters = setupCharFilters(plugins);
        NamedRegistry<org.apache.lucene.analysis.hunspell.Dictionary> hunspellDictionaries = setupHunspellDictionaries(plugins);
        hunspellService = new HunspellService(environment.settings(), environment, hunspellDictionaries.getRegistry());
        NamedRegistry<AnalysisProvider<TokenFilterFactory>> tokenFilters = setupTokenFilters(plugins, hunspellService);
        NamedRegistry<AnalysisProvider<TokenizerFactory>> tokenizers = setupTokenizers(plugins);
        NamedRegistry<AnalysisProvider<AnalyzerProvider<?>>> analyzers = setupAnalyzers(plugins);
        analysisRegistry = new AnalysisRegistry(environment, charFilters.getRegistry(), tokenFilters.getRegistry(),
                tokenizers.getRegistry(), analyzers.getRegistry());
    }

    HunspellService getHunspellService() {
        return hunspellService;
    }

    public AnalysisRegistry getAnalysisRegistry() {
        return analysisRegistry;
    }

    private NamedRegistry<AnalysisProvider<CharFilterFactory>> setupCharFilters(List<AnalysisPlugin> plugins) {
        NamedRegistry<AnalysisProvider<CharFilterFactory>> charFilters = new NamedRegistry<>("char_filter");
        charFilters.register("html_strip", HtmlStripCharFilterFactory::new);
        charFilters.register("pattern_replace", requriesAnalysisSettings(PatternReplaceCharFilterFactory::new));
        charFilters.register("mapping", requriesAnalysisSettings(MappingCharFilterFactory::new));
        charFilters.extractAndRegister(plugins, AnalysisPlugin::getCharFilters);
        return charFilters;
    }

    public NamedRegistry<org.apache.lucene.analysis.hunspell.Dictionary> setupHunspellDictionaries(List<AnalysisPlugin> plugins) {
        NamedRegistry<org.apache.lucene.analysis.hunspell.Dictionary> hunspellDictionaries = new NamedRegistry<>("dictionary");
        hunspellDictionaries.extractAndRegister(plugins, AnalysisPlugin::getHunspellDictionaries);
        return hunspellDictionaries;
    }

    private NamedRegistry<AnalysisProvider<TokenFilterFactory>> setupTokenFilters(List<AnalysisPlugin> plugins,
            HunspellService hunspellService) {
        NamedRegistry<AnalysisProvider<TokenFilterFactory>> tokenFilters = new NamedRegistry<>("token_filter");
        tokenFilters.register("stop", StopTokenFilterFactory::new);
        tokenFilters.register("reverse", ReverseTokenFilterFactory::new);
        tokenFilters.register("asciifolding", ASCIIFoldingTokenFilterFactory::new);
        tokenFilters.register("length", LengthTokenFilterFactory::new);
        tokenFilters.register("lowercase", LowerCaseTokenFilterFactory::new);
        tokenFilters.register("uppercase", UpperCaseTokenFilterFactory::new);
        tokenFilters.register("porter_stem", PorterStemTokenFilterFactory::new);
        tokenFilters.register("kstem", KStemTokenFilterFactory::new);
        tokenFilters.register("standard", StandardTokenFilterFactory::new);
        tokenFilters.register("nGram", NGramTokenFilterFactory::new);
        tokenFilters.register("ngram", NGramTokenFilterFactory::new);
        tokenFilters.register("edgeNGram", EdgeNGramTokenFilterFactory::new);
        tokenFilters.register("edge_ngram", EdgeNGramTokenFilterFactory::new);
        tokenFilters.register("shingle", ShingleTokenFilterFactory::new);
        tokenFilters.register("unique", UniqueTokenFilterFactory::new);
        tokenFilters.register("truncate", requriesAnalysisSettings(TruncateTokenFilterFactory::new));
        tokenFilters.register("trim", TrimTokenFilterFactory::new);
        tokenFilters.register("limit", LimitTokenCountFilterFactory::new);
        tokenFilters.register("common_grams", requriesAnalysisSettings(CommonGramsTokenFilterFactory::new));
        tokenFilters.register("snowball", SnowballTokenFilterFactory::new);
        tokenFilters.register("stemmer", StemmerTokenFilterFactory::new);
        tokenFilters.register("word_delimiter", WordDelimiterTokenFilterFactory::new);
        tokenFilters.register("delimited_payload_filter", DelimitedPayloadTokenFilterFactory::new);
        tokenFilters.register("elision", ElisionTokenFilterFactory::new);
        tokenFilters.register("keep", requriesAnalysisSettings(KeepWordFilterFactory::new));
        tokenFilters.register("keep_types", requriesAnalysisSettings(KeepTypesFilterFactory::new));
        tokenFilters.register("pattern_capture", requriesAnalysisSettings(PatternCaptureGroupTokenFilterFactory::new));
        tokenFilters.register("pattern_replace", requriesAnalysisSettings(PatternReplaceTokenFilterFactory::new));
        tokenFilters.register("dictionary_decompounder", requriesAnalysisSettings(DictionaryCompoundWordTokenFilterFactory::new));
        tokenFilters.register("hyphenation_decompounder", requriesAnalysisSettings(HyphenationCompoundWordTokenFilterFactory::new));
        tokenFilters.register("arabic_stem", ArabicStemTokenFilterFactory::new);
        tokenFilters.register("brazilian_stem", BrazilianStemTokenFilterFactory::new);
        tokenFilters.register("czech_stem", CzechStemTokenFilterFactory::new);
        tokenFilters.register("dutch_stem", DutchStemTokenFilterFactory::new);
        tokenFilters.register("french_stem", FrenchStemTokenFilterFactory::new);
        tokenFilters.register("german_stem", GermanStemTokenFilterFactory::new);
        tokenFilters.register("russian_stem", RussianStemTokenFilterFactory::new);
        tokenFilters.register("keyword_marker", requriesAnalysisSettings(KeywordMarkerTokenFilterFactory::new));
        tokenFilters.register("stemmer_override", requriesAnalysisSettings(StemmerOverrideTokenFilterFactory::new));
        tokenFilters.register("arabic_normalization", ArabicNormalizationFilterFactory::new);
        tokenFilters.register("german_normalization", GermanNormalizationFilterFactory::new);
        tokenFilters.register("hindi_normalization", HindiNormalizationFilterFactory::new);
        tokenFilters.register("indic_normalization", IndicNormalizationFilterFactory::new);
        tokenFilters.register("sorani_normalization", SoraniNormalizationFilterFactory::new);
        tokenFilters.register("persian_normalization", PersianNormalizationFilterFactory::new);
        tokenFilters.register("scandinavian_normalization", ScandinavianNormalizationFilterFactory::new);
        tokenFilters.register("scandinavian_folding", ScandinavianFoldingFilterFactory::new);
        tokenFilters.register("serbian_normalization", SerbianNormalizationFilterFactory::new);

        tokenFilters.register("hunspell", requriesAnalysisSettings(
                (indexSettings, env, name, settings) -> new HunspellTokenFilterFactory(indexSettings, name, settings, hunspellService)));
        tokenFilters.register("cjk_bigram", CJKBigramFilterFactory::new);
        tokenFilters.register("cjk_width", CJKWidthFilterFactory::new);

        tokenFilters.register("apostrophe", ApostropheFilterFactory::new);
        tokenFilters.register("classic", ClassicFilterFactory::new);
        tokenFilters.register("decimal_digit", DecimalDigitFilterFactory::new);
        tokenFilters.register("fingerprint", FingerprintTokenFilterFactory::new);
        tokenFilters.register("minhash", MinHashTokenFilterFactory::new);
        tokenFilters.extractAndRegister(plugins, AnalysisPlugin::getTokenFilters);
        return tokenFilters;
    }

    private NamedRegistry<AnalysisProvider<TokenizerFactory>> setupTokenizers(List<AnalysisPlugin> plugins) {
        NamedRegistry<AnalysisProvider<TokenizerFactory>> tokenizers = new NamedRegistry<>("tokenizer");
        tokenizers.register("standard", StandardTokenizerFactory::new);
        tokenizers.register("uax_url_email", UAX29URLEmailTokenizerFactory::new);
        tokenizers.register("path_hierarchy", PathHierarchyTokenizerFactory::new);
        tokenizers.register("PathHierarchy", PathHierarchyTokenizerFactory::new);
        tokenizers.register("keyword", KeywordTokenizerFactory::new);
        tokenizers.register("letter", LetterTokenizerFactory::new);
        tokenizers.register("lowercase", LowerCaseTokenizerFactory::new);
        tokenizers.register("whitespace", WhitespaceTokenizerFactory::new);
        tokenizers.register("nGram", NGramTokenizerFactory::new);
        tokenizers.register("ngram", NGramTokenizerFactory::new);
        tokenizers.register("edgeNGram", EdgeNGramTokenizerFactory::new);
        tokenizers.register("edge_ngram", EdgeNGramTokenizerFactory::new);
        tokenizers.register("pattern", PatternTokenizerFactory::new);
        tokenizers.register("classic", ClassicTokenizerFactory::new);
        tokenizers.register("thai", ThaiTokenizerFactory::new);
        tokenizers.extractAndRegister(plugins, AnalysisPlugin::getTokenizers);
        return tokenizers;
    }

    private NamedRegistry<AnalysisProvider<AnalyzerProvider<?>>> setupAnalyzers(List<AnalysisPlugin> plugins) {
        NamedRegistry<AnalysisProvider<AnalyzerProvider<?>>> analyzers = new NamedRegistry<>("analyzer");
        analyzers.register("default", StandardAnalyzerProvider::new);
        analyzers.register("standard", StandardAnalyzerProvider::new);
        analyzers.register("standard_html_strip", StandardHtmlStripAnalyzerProvider::new);
        analyzers.register("simple", SimpleAnalyzerProvider::new);
        analyzers.register("stop", StopAnalyzerProvider::new);
        analyzers.register("whitespace", WhitespaceAnalyzerProvider::new);
        analyzers.register("keyword", KeywordAnalyzerProvider::new);
        analyzers.register("pattern", PatternAnalyzerProvider::new);
        analyzers.register("snowball", SnowballAnalyzerProvider::new);
        analyzers.register("arabic", ArabicAnalyzerProvider::new);
        analyzers.register("armenian", ArmenianAnalyzerProvider::new);
        analyzers.register("basque", BasqueAnalyzerProvider::new);
        analyzers.register("brazilian", BrazilianAnalyzerProvider::new);
        analyzers.register("bulgarian", BulgarianAnalyzerProvider::new);
        analyzers.register("catalan", CatalanAnalyzerProvider::new);
        analyzers.register("chinese", ChineseAnalyzerProvider::new);
        analyzers.register("cjk", CjkAnalyzerProvider::new);
        analyzers.register("czech", CzechAnalyzerProvider::new);
        analyzers.register("danish", DanishAnalyzerProvider::new);
        analyzers.register("dutch", DutchAnalyzerProvider::new);
        analyzers.register("english", EnglishAnalyzerProvider::new);
        analyzers.register("finnish", FinnishAnalyzerProvider::new);
        analyzers.register("french", FrenchAnalyzerProvider::new);
        analyzers.register("galician", GalicianAnalyzerProvider::new);
        analyzers.register("german", GermanAnalyzerProvider::new);
        analyzers.register("greek", GreekAnalyzerProvider::new);
        analyzers.register("hindi", HindiAnalyzerProvider::new);
        analyzers.register("hungarian", HungarianAnalyzerProvider::new);
        analyzers.register("indonesian", IndonesianAnalyzerProvider::new);
        analyzers.register("irish", IrishAnalyzerProvider::new);
        analyzers.register("italian", ItalianAnalyzerProvider::new);
        analyzers.register("latvian", LatvianAnalyzerProvider::new);
        analyzers.register("lithuanian", LithuanianAnalyzerProvider::new);
        analyzers.register("norwegian", NorwegianAnalyzerProvider::new);
        analyzers.register("persian", PersianAnalyzerProvider::new);
        analyzers.register("portuguese", PortugueseAnalyzerProvider::new);
        analyzers.register("romanian", RomanianAnalyzerProvider::new);
        analyzers.register("russian", RussianAnalyzerProvider::new);
        analyzers.register("sorani", SoraniAnalyzerProvider::new);
        analyzers.register("spanish", SpanishAnalyzerProvider::new);
        analyzers.register("swedish", SwedishAnalyzerProvider::new);
        analyzers.register("turkish", TurkishAnalyzerProvider::new);
        analyzers.register("thai", ThaiAnalyzerProvider::new);
        analyzers.register("fingerprint", FingerprintAnalyzerProvider::new);
        analyzers.extractAndRegister(plugins, AnalysisPlugin::getAnalyzers);
        return analyzers;
    }

    private static <T> AnalysisModule.AnalysisProvider<T> requriesAnalysisSettings(AnalysisModule.AnalysisProvider<T> provider) {
        return new AnalysisModule.AnalysisProvider<T>() {
            @Override
            public T get(IndexSettings indexSettings, Environment environment, String name, Settings settings) throws IOException {
                return provider.get(indexSettings, environment, name, settings);
            }
            @Override
            public boolean requiresAnalysisSettings() {
                return true;
            }
        };
    }

    /**
     * The basic factory interface for analysis components.
     */
    public interface AnalysisProvider<T> {

        /**
         * Creates a new analysis provider.
         * @param indexSettings the index settings for the index this provider is created for
         * @param environment the nodes environment to load resources from persistent storage
         * @param name the name of the analysis component
         * @param settings the component specific settings without context prefixes
         * @return a new provider instance
         * @throws IOException if an {@link IOException} occurs
         */
        T get(IndexSettings indexSettings, Environment environment, String name, Settings settings) throws IOException;

        /**
         * Creates a new global scope analysis provider without index specific settings not settings for the provider itself.
         * This can be used to get a default instance of an analysis factory without binding to an index.
         *
         * @param environment the nodes environment to load resources from persistent storage
         * @param name the name of the analysis component
         * @return a new provider instance
         * @throws IOException if an {@link IOException} occurs
         * @throws IllegalArgumentException if the provider requires analysis settings ie. if {@link #requiresAnalysisSettings()} returns
         *  <code>true</code>
         */
        default T get(Environment environment, String name) throws IOException {
            if (requiresAnalysisSettings()) {
                throw new IllegalArgumentException("Analysis settings required - can't instantiate analysis factory");
            }
            return get(NA_INDEX_SETTINGS, environment, name, NA_INDEX_SETTINGS.getSettings());
        }

        /**
         * If <code>true</code> the analysis component created by this provider requires certain settings to be instantiated.
         * it can't be created with defaults. The default is <code>false</code>.
         */
        default boolean requiresAnalysisSettings() {
            return false;
        }
    }
}
