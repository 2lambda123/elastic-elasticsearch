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

package org.elasticsearch.index.similarity;

import org.apache.lucene.search.similarities.AfterEffectL;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BasicModelG;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.DFISimilarity;
import org.apache.lucene.search.similarities.DFRSimilarity;
import org.apache.lucene.search.similarities.DistributionSPL;
import org.apache.lucene.search.similarities.IBSimilarity;
import org.apache.lucene.search.similarities.IndependenceChiSquared;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.LambdaTTF;
import org.apache.lucene.search.similarities.NormalizationH2;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class SimilarityTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testResolveDefaultSimilarities() {
        SimilarityService similarityService = createIndex("foo").similarityService();
        assertThat(similarityService.getSimilarity("BM25").get(), instanceOf(BM25Similarity.class));
        assertThat(similarityService.getSimilarity("boolean").get(), instanceOf(BooleanSimilarity.class));
        assertThat(similarityService.getSimilarity("default"), equalTo(null));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> similarityService.getSimilarity("classic"));
        assertEquals("The [classic] similarity may not be used anymore. Please use the [BM25] similarity or build a custom [scripted] "
                + "similarity instead.", e.getMessage());
    }

    public void testResolveDefaultSimilaritiesOn6xIndex() {
        Settings indexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_6_3_0) // otherwise classic is forbidden
                .build();
        SimilarityService similarityService = createIndex("foo", indexSettings).similarityService();
        assertThat(similarityService.getSimilarity("classic").get(), instanceOf(ClassicSimilarity.class));
        assertWarnings("The [classic] similarity is now deprecated in favour of BM25, which is generally "
                + "accepted as a better alternative. Use the [BM25] similarity or build a custom [scripted] similarity "
                + "instead.");
        assertThat(similarityService.getSimilarity("BM25").get(), instanceOf(BM25Similarity.class));
        assertThat(similarityService.getSimilarity("boolean").get(), instanceOf(BooleanSimilarity.class));
        assertThat(similarityService.getSimilarity("default"), equalTo(null));
    }

    public void testResolveSimilaritiesFromMapping_classic() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "my_similarity").endObject()
            .endObject()
            .endObject().endObject());

        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "classic")
            .put("index.similarity.my_similarity.discount_overlaps", false)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_6_3_0) // otherwise classic is forbidden
            .build();
        IndexService indexService = createIndex("foo", indexSettings);
        DocumentMapper documentMapper = indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        assertThat(documentMapper.mappers().getFieldMapper("field1").fieldType().similarity().get(), instanceOf(ClassicSimilarity.class));

        ClassicSimilarity similarity = (ClassicSimilarity) documentMapper.mappers().getFieldMapper("field1").fieldType().similarity().get();
        assertThat(similarity.getDiscountOverlaps(), equalTo(false));
    }

    public void testResolveSimilaritiesFromMapping_classicIsForbidden() throws IOException {
        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "classic")
            .put("index.similarity.my_similarity.discount_overlaps", false)
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> createIndex("foo", indexSettings));
        assertEquals("The [classic] similarity may not be used anymore. Please use the [BM25] similarity or build a custom [scripted] "
                + "similarity instead.", e.getMessage());
    }

    public void testResolveSimilaritiesFromMapping_bm25() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "my_similarity").endObject()
            .endObject()
            .endObject().endObject());

        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "BM25")
            .put("index.similarity.my_similarity.k1", 2.0f)
            .put("index.similarity.my_similarity.b", 0.5f)
            .put("index.similarity.my_similarity.discount_overlaps", false)
            .build();
        IndexService indexService = createIndex("foo", indexSettings);
        DocumentMapper documentMapper = indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        assertThat(documentMapper.mappers().getFieldMapper("field1").fieldType().similarity().get(), instanceOf(BM25Similarity.class));

        BM25Similarity similarity = (BM25Similarity) documentMapper.mappers().getFieldMapper("field1").fieldType().similarity().get();
        assertThat(similarity.getK1(), equalTo(2.0f));
        assertThat(similarity.getB(), equalTo(0.5f));
        assertThat(similarity.getDiscountOverlaps(), equalTo(false));
    }

    public void testResolveSimilaritiesFromMapping_boolean() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "boolean").endObject()
            .endObject()
            .endObject().endObject());

        IndexService indexService = createIndex("foo", Settings.EMPTY);
        DocumentMapper documentMapper = indexService.mapperService()
            .documentMapperParser()
            .parse("type", new CompressedXContent(mapping));
        assertThat(documentMapper.mappers().getFieldMapper("field1").fieldType().similarity().get(),
            instanceOf(BooleanSimilarity.class));
    }

    public void testResolveSimilaritiesFromMapping_DFR() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "my_similarity").endObject()
            .endObject()
            .endObject().endObject());

        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "DFR")
            .put("index.similarity.my_similarity.basic_model", "g")
            .put("index.similarity.my_similarity.after_effect", "l")
            .put("index.similarity.my_similarity.normalization", "h2")
            .put("index.similarity.my_similarity.normalization.h2.c", 3f)
            .build();
        IndexService indexService = createIndex("foo", indexSettings);
        DocumentMapper documentMapper = indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        assertThat(documentMapper.mappers().getFieldMapper("field1").fieldType().similarity().get(), instanceOf(DFRSimilarity.class));

        DFRSimilarity similarity = (DFRSimilarity) documentMapper.mappers().getFieldMapper("field1").fieldType().similarity().get();
        assertThat(similarity.getBasicModel(), instanceOf(BasicModelG.class));
        assertThat(similarity.getAfterEffect(), instanceOf(AfterEffectL.class));
        assertThat(similarity.getNormalization(), instanceOf(NormalizationH2.class));
        assertThat(((NormalizationH2) similarity.getNormalization()).getC(), equalTo(3f));
    }

    public void testResolveSimilaritiesFromMapping_IB() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "my_similarity").endObject()
            .endObject()
            .endObject().endObject());

        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "IB")
            .put("index.similarity.my_similarity.distribution", "spl")
            .put("index.similarity.my_similarity.lambda", "ttf")
            .put("index.similarity.my_similarity.normalization", "h2")
            .put("index.similarity.my_similarity.normalization.h2.c", 3f)
            .build();
        IndexService indexService = createIndex("foo", indexSettings);
        DocumentMapper documentMapper = indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        assertThat(documentMapper.mappers().getFieldMapper("field1").fieldType().similarity().get(), instanceOf(IBSimilarity.class));

        IBSimilarity similarity = (IBSimilarity) documentMapper.mappers().getFieldMapper("field1").fieldType().similarity().get();
        assertThat(similarity.getDistribution(), instanceOf(DistributionSPL.class));
        assertThat(similarity.getLambda(), instanceOf(LambdaTTF.class));
        assertThat(similarity.getNormalization(), instanceOf(NormalizationH2.class));
        assertThat(((NormalizationH2) similarity.getNormalization()).getC(), equalTo(3f));
    }

    public void testResolveSimilaritiesFromMapping_DFI() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "my_similarity").endObject()
            .endObject()
            .endObject().endObject());

        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "DFI")
            .put("index.similarity.my_similarity.independence_measure", "chisquared")
            .build();
        IndexService indexService = createIndex("foo", indexSettings);
        DocumentMapper documentMapper = indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        MappedFieldType fieldType = documentMapper.mappers().getFieldMapper("field1").fieldType();
        assertThat(fieldType.similarity().get(), instanceOf(DFISimilarity.class));
        DFISimilarity similarity = (DFISimilarity) fieldType.similarity().get();
        assertThat(similarity.getIndependence(), instanceOf(IndependenceChiSquared.class));
    }

    public void testResolveSimilaritiesFromMapping_LMDirichlet() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "my_similarity").endObject()
            .endObject()
            .endObject().endObject());

        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "LMDirichlet")
            .put("index.similarity.my_similarity.mu", 3000f)
            .build();
        IndexService indexService = createIndex("foo", indexSettings);
        DocumentMapper documentMapper = indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        assertThat(documentMapper.mappers().getFieldMapper("field1").fieldType().similarity().get(), instanceOf(LMDirichletSimilarity.class));

        LMDirichletSimilarity similarity = (LMDirichletSimilarity) documentMapper.mappers().getFieldMapper("field1").fieldType().similarity().get();
        assertThat(similarity.getMu(), equalTo(3000f));
    }

    public void testResolveSimilaritiesFromMapping_LMJelinekMercer() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "my_similarity").endObject()
            .endObject()
            .endObject().endObject());

        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "LMJelinekMercer")
            .put("index.similarity.my_similarity.lambda", 0.7f)
            .build();
        IndexService indexService = createIndex("foo", indexSettings);
        DocumentMapper documentMapper = indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        assertThat(documentMapper.mappers().getFieldMapper("field1").fieldType().similarity().get(), instanceOf(LMJelinekMercerSimilarity.class));

        LMJelinekMercerSimilarity similarity = (LMJelinekMercerSimilarity) documentMapper.mappers().getFieldMapper("field1").fieldType().similarity().get();
        assertThat(similarity.getLambda(), equalTo(0.7f));
    }

    public void testResolveSimilaritiesFromMapping_Unknown() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("similarity", "unknown_similarity").endObject()
            .endObject()
            .endObject().endObject());

        IndexService indexService = createIndex("foo");
        try {
            indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
            fail("Expected MappingParsingException");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), equalTo("Unknown Similarity type [unknown_similarity] for field [field1]"));
        }
    }

    public void testUnknownParameters() throws IOException {
        Settings indexSettings = Settings.builder()
            .put("index.similarity.my_similarity.type", "BM25")
            .put("index.similarity.my_similarity.z", 2.0f)
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> createIndex("foo", indexSettings));
        assertEquals("Unknown settings for similarity of type [BM25]: [z]", e.getMessage());
    }
}
