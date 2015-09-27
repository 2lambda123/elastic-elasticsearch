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
package org.elasticsearch.percolator;

import com.carrotsearch.hppc.IntObjectHashMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.index.memory.ExtendedMemoryIndex;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.percolate.PercolateShardRequest;
import org.elasticsearch.action.percolate.PercolateShardResponse;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.HasContextAndHeaders;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.DocumentMapperForType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.percolator.QueryMetadataService;
import org.elasticsearch.index.percolator.PercolatorQueriesRegistry;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregator;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.highlight.HighlightPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortParseElement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.lucene.search.BooleanClause.Occur.*;
import static org.elasticsearch.index.mapper.SourceToParse.source;

public class PercolatorService extends AbstractComponent {

    public final static float NO_SCORE = Float.NEGATIVE_INFINITY;
    public final static String TYPE_NAME = ".percolator";

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final IndicesService indicesService;
    private final IntObjectHashMap<PercolatorType> percolatorTypes;
    private final PageCacheRecycler pageCacheRecycler;
    private final BigArrays bigArrays;
    private final ClusterService clusterService;

    private final PercolatorIndex single;
    private final PercolatorIndex multi;

    private final HighlightPhase highlightPhase;
    private final AggregationPhase aggregationPhase;
    private final SortParseElement sortParseElement;
    private final ScriptService scriptService;
    private final MappingUpdatedAction mappingUpdatedAction;

    private final CloseableThreadLocal<MemoryIndex> cache;

    private final ParseFieldMatcher parseFieldMatcher;

    @Inject
    public PercolatorService(Settings settings, IndexNameExpressionResolver indexNameExpressionResolver, IndicesService indicesService,
                             PageCacheRecycler pageCacheRecycler, BigArrays bigArrays,
                             HighlightPhase highlightPhase, ClusterService clusterService,
                             AggregationPhase aggregationPhase, ScriptService scriptService,
                             MappingUpdatedAction mappingUpdatedAction) {
        super(settings);
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.parseFieldMatcher = new ParseFieldMatcher(settings);
        this.indicesService = indicesService;
        this.pageCacheRecycler = pageCacheRecycler;
        this.bigArrays = bigArrays;
        this.clusterService = clusterService;
        this.highlightPhase = highlightPhase;
        this.aggregationPhase = aggregationPhase;
        this.scriptService = scriptService;
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.sortParseElement = new SortParseElement();

        final long maxReuseBytes = settings.getAsBytesSize("indices.memory.memory_index.size_per_thread", new ByteSizeValue(1, ByteSizeUnit.MB)).bytes();
        cache = new CloseableThreadLocal<MemoryIndex>() {
            @Override
            protected MemoryIndex initialValue() {
                // TODO: should we expose payloads as an option? should offsets be turned on always?
                return new ExtendedMemoryIndex(true, false, maxReuseBytes);
            }
        };
        single = new SingleDocumentPercolatorIndex(cache);
        multi = new MultiDocumentPercolatorIndex(cache);

        percolatorTypes = new IntObjectHashMap<>(4);
        percolatorTypes.put(queryCountPercolator.id(), queryCountPercolator);
        percolatorTypes.put(topMatchingPercolator.id(), topMatchingPercolator);
    }


    public ReduceResult reduce(byte percolatorTypeId, List<PercolateShardResponse> shardResults, HasContextAndHeaders headersContext) {
        PercolatorType percolatorType = percolatorTypes.get(percolatorTypeId);
        return percolatorType.reduce(shardResults, headersContext);
    }

    public PercolateShardResponse percolate(PercolateShardRequest request) throws IOException {
        IndexService percolateIndexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        IndexShard indexShard = percolateIndexService.getShard(request.shardId().id());
        indexShard.readAllowed(); // check if we can read the shard...
        PercolatorQueriesRegistry percolateQueryRegistry = indexShard.percolateRegistry();
        percolateQueryRegistry.prePercolate();
        long startTime = System.nanoTime();

        // TODO: The filteringAliases should be looked up at the coordinating node and serialized with all shard request,
        // just like is done in other apis.
        String[] filteringAliases = indexNameExpressionResolver.filteringAliases(
                clusterService.state(),
                indexShard.shardId().index().name(),
                request.indices()
        );
        Query aliasFilter = percolateIndexService.aliasesService().aliasFilter(filteringAliases);

        SearchShardTarget searchShardTarget = new SearchShardTarget(clusterService.localNode().id(), request.shardId().getIndex(), request.shardId().id());
        final PercolateContext context = new PercolateContext(
                request, searchShardTarget, indexShard, percolateIndexService, pageCacheRecycler, bigArrays, scriptService, aliasFilter, parseFieldMatcher
        );
        SearchContext.setCurrent(context);
        try {
            ParsedDocument parsedDocument = parseRequest(percolateIndexService, request, context, request.shardId().getIndex());
            if (context.percolateQueries().isEmpty()) {
                return new PercolateShardResponse(context);
            }

            if (request.docSource() != null && request.docSource().length() != 0) {
                parsedDocument = parseFetchedDoc(context, request.docSource(), percolateIndexService, request.shardId().getIndex(), request.documentType());
            } else if (parsedDocument == null) {
                throw new IllegalArgumentException("Nothing to percolate");
            }

            if (context.size() < 0) {
                context.size(0);
            }

            // parse the source either into one MemoryIndex, if it is a single document or index multiple docs if nested
            PercolatorIndex percolatorIndex;
            boolean isNested = indexShard.mapperService().documentMapper(request.documentType()).hasNestedObjects();
            if (parsedDocument.docs().size() > 1) {
                assert isNested;
                percolatorIndex = multi;
            } else {
                percolatorIndex = single;
            }

            final PercolatorType action;
            if (request.onlyCount()) {
                action = queryCountPercolator;
            } else {
                action = topMatchingPercolator;
            }
            context.percolatorTypeId = action.id();

            percolatorIndex.prepare(context, parsedDocument);
            Query queryMetaDataQuery = indexShard.percolateRegistry().getQueryMetadataService().createQueryMetadataQuery(context.docSearcher().getIndexReader());
            context.setQueryMetaDataQuery(queryMetaDataQuery);
            return doPercolate(action, context, isNested);
        } finally {
            SearchContext.removeCurrent();
            context.close();
            percolateQueryRegistry.postPercolate(System.nanoTime() - startTime);
        }
    }

    private ParsedDocument parseRequest(IndexService documentIndexService, PercolateShardRequest request, PercolateContext context, String index) {
        BytesReference source = request.source();
        if (source == null || source.length() == 0) {
            return null;
        }

        // TODO: combine all feature parse elements into one map
        Map<String, ? extends SearchParseElement> hlElements = highlightPhase.parseElements();
        Map<String, ? extends SearchParseElement> aggregationElements = aggregationPhase.parseElements();

        ParsedDocument doc = null;
        XContentParser parser = null;

        // Some queries (function_score query when for decay functions) rely on a SearchContext being set:
        // We switch types because this context needs to be in the context of the percolate queries in the shard and
        // not the in memory percolate doc
        String[] previousTypes = context.types();
        context.types(new String[]{TYPE_NAME});
        try {
            parser = XContentFactory.xContent(source).createParser(source);
            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                    // we need to check the "doc" here, so the next token will be START_OBJECT which is
                    // the actual document starting
                    if ("doc".equals(currentFieldName)) {
                        if (doc != null) {
                            throw new ElasticsearchParseException("Either specify doc or get, not both");
                        }

                        MapperService mapperService = documentIndexService.mapperService();
                        DocumentMapperForType docMapper = mapperService.documentMapperWithAutoCreate(request.documentType());
                        doc = docMapper.getDocumentMapper().parse(source(parser).index(index).type(request.documentType()).flyweight(true));
                        if (docMapper.getMapping() != null) {
                            doc.addDynamicMappingsUpdate(docMapper.getMapping());
                        }
                        if (doc.dynamicMappingsUpdate() != null) {
                            mappingUpdatedAction.updateMappingOnMasterSynchronously(request.shardId().getIndex(), request.documentType(), doc.dynamicMappingsUpdate());
                        }
                        // the document parsing exists the "doc" object, so we need to set the new current field.
                        currentFieldName = parser.currentName();
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    SearchParseElement element = hlElements.get(currentFieldName);
                    if (element == null) {
                        element = aggregationElements.get(currentFieldName);
                    }

                    if ("query".equals(currentFieldName)) {
                        if (context.percolateQuery() != null) {
                            throw new ElasticsearchParseException("Either specify query or filter, not both");
                        }
                        context.percolateQuery(documentIndexService.queryParserService().parse(parser).query());
                    } else if ("filter".equals(currentFieldName)) {
                        if (context.percolateQuery() != null) {
                            throw new ElasticsearchParseException("Either specify query or filter, not both");
                        }
                        Query filter = documentIndexService.queryParserService().parseInnerFilter(parser).query();
                        context.percolateQuery(new ConstantScoreQuery(filter));
                    } else if ("sort".equals(currentFieldName)) {
                        parseSort(parser, context);
                    } else if (element != null) {
                        element.parse(parser, context);
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("sort".equals(currentFieldName)) {
                        parseSort(parser, context);
                    }
                } else if (token == null) {
                    break;
                } else if (token.isValue()) {
                    if ("size".equals(currentFieldName)) {
                        context.size(parser.intValue());
                        if (context.size() < 0) {
                            throw new ElasticsearchParseException("size is set to [{}] and is expected to be higher or equal to 0", context.size());
                        }
                    } else if ("sort".equals(currentFieldName)) {
                        parseSort(parser, context);
                    } else if ("track_scores".equals(currentFieldName) || "trackScores".equals(currentFieldName)) {
                        context.trackScores(parser.booleanValue());
                    }
                }
            }

            // We need to get the actual source from the request body for highlighting, so parse the request body again
            // and only get the doc source.
            if (context.highlight() != null) {
                parser.close();
                currentFieldName = null;
                parser = XContentFactory.xContent(source).createParser(source);
                token = parser.nextToken();
                assert token == XContentParser.Token.START_OBJECT;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if ("doc".equals(currentFieldName)) {
                            BytesStreamOutput bStream = new BytesStreamOutput();
                            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.SMILE, bStream);
                            builder.copyCurrentStructure(parser);
                            builder.close();
                            doc.setSource(bStream.bytes());
                            break;
                        } else {
                            parser.skipChildren();
                        }
                    } else if (token == null) {
                        break;
                    }
                }
            }

        } catch (Throwable e) {
            throw new ElasticsearchParseException("failed to parse request", e);
        } finally {
            context.types(previousTypes);
            if (parser != null) {
                parser.close();
            }
        }

        return doc;
    }

    private void parseSort(XContentParser parser, PercolateContext context) throws Exception {
        sortParseElement.parse(parser, context);
        // null, means default sorting by relevancy
        if (context.sort() != null) {
            throw new ElasticsearchParseException("Only _score desc is supported");
        }
    }

    private ParsedDocument parseFetchedDoc(PercolateContext context, BytesReference fetchedDoc, IndexService documentIndexService, String index, String type) {
        ParsedDocument doc = null;
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(fetchedDoc).createParser(fetchedDoc);
            MapperService mapperService = documentIndexService.mapperService();
            DocumentMapperForType docMapper = mapperService.documentMapperWithAutoCreate(type);
            doc = docMapper.getDocumentMapper().parse(source(parser).index(index).type(type).flyweight(true));

            if (context.highlight() != null) {
                doc.setSource(fetchedDoc);
            }
        } catch (Throwable e) {
            throw new ElasticsearchParseException("failed to parse request", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }

        if (doc == null) {
            throw new ElasticsearchParseException("No doc to percolate in the request");
        }

        return doc;
    }

    public void close() {
        cache.close();
    }

    interface PercolatorType<C extends Collector> {

        // 0x00 is reserved for empty type.
        byte id();

        ReduceResult reduce(List<PercolateShardResponse> shardResults, HasContextAndHeaders headersContext);

        C getCollector(PercolateContext context);

        PercolateShardResponse processResults(PercolateContext context, C collector);

    }

    private final PercolatorType queryCountPercolator = new PercolatorType<TotalHitCountCollector>() {

        @Override
        public byte id() {
            return 0x02;
        }

        @Override
        public ReduceResult reduce(List<PercolateShardResponse> shardResults, HasContextAndHeaders headersContext) {
            long finalCount = 0;
            for (PercolateShardResponse shardResponse : shardResults) {
                finalCount += shardResponse.count();
            }

            assert !shardResults.isEmpty();
            InternalAggregations reducedAggregations = reduceAggregations(shardResults, headersContext);
            return new ReduceResult(finalCount, reducedAggregations);
        }

        @Override
        public TotalHitCountCollector getCollector(PercolateContext context) {
            return new TotalHitCountCollector();
        }

        @Override
        public PercolateShardResponse processResults(PercolateContext context, TotalHitCountCollector collector) {
            return new PercolateShardResponse(collector.getTotalHits(), context);
        }

    };

    private final PercolatorType topMatchingPercolator = new PercolatorType<TopScoreDocCollector>() {

        @Override
        public byte id() {
            return 0x06;
        }

        @Override
        public ReduceResult reduce(List<PercolateShardResponse> shardResults, HasContextAndHeaders headersContext) {
            long foundMatches = 0;
            int nonEmptyResponses = 0;
            int firstNonEmptyIndex = 0;
            for (int i = 0; i < shardResults.size(); i++) {
                PercolateShardResponse response = shardResults.get(i);
                foundMatches += response.count();
                if (response.matches().length != 0) {
                    if (firstNonEmptyIndex == 0) {
                        firstNonEmptyIndex = i;
                    }
                    nonEmptyResponses++;
                }
            }

            int requestedSize = shardResults.get(0).requestedSize();

            // Use a custom impl of AbstractBigArray for Object[]?
            List<PercolateResponse.Match> finalMatches = new ArrayList<>(requestedSize);
            if (nonEmptyResponses == 1) {
                PercolateShardResponse response = shardResults.get(firstNonEmptyIndex);
                Text index = new StringText(response.getIndex());
                for (int i = 0; i < response.matches().length; i++) {
                    float score = response.scores().length == 0 ? Float.NaN : response.scores()[i];
                    Text match = new BytesText(new BytesArray(response.matches()[i]));
                    if (!response.hls().isEmpty()) {
                        Map<String, HighlightField> hl = response.hls().get(i);
                        finalMatches.add(new PercolateResponse.Match(index, match, score, hl));
                    } else {
                        finalMatches.add(new PercolateResponse.Match(index, match, score));
                    }
                }
            } else {
                int[] slots = new int[shardResults.size()];
                while (true) {
                    float lowestScore = Float.NEGATIVE_INFINITY;
                    int requestIndex = -1;
                    int itemIndex = -1;
                    for (int i = 0; i < shardResults.size(); i++) {
                        int scoreIndex = slots[i];
                        float[] scores = shardResults.get(i).scores();
                        if (scoreIndex >= scores.length) {
                            continue;
                        }

                        float score = scores[scoreIndex];
                        int cmp = Float.compare(lowestScore, score);
                        // TODO: Maybe add a tie?
                        if (cmp < 0) {
                            requestIndex = i;
                            itemIndex = scoreIndex;
                            lowestScore = score;
                        }
                    }

                    // This means the shard matches have been exhausted and we should bail
                    if (requestIndex == -1) {
                        break;
                    }

                    slots[requestIndex]++;

                    PercolateShardResponse shardResponse = shardResults.get(requestIndex);
                    Text index = new StringText(shardResponse.getIndex());
                    Text match = new BytesText(new BytesArray(shardResponse.matches()[itemIndex]));
                    float score = shardResponse.scores()[itemIndex];
                    if (!shardResponse.hls().isEmpty()) {
                        Map<String, HighlightField> hl = shardResponse.hls().get(itemIndex);
                        finalMatches.add(new PercolateResponse.Match(index, match, score, hl));
                    } else {
                        finalMatches.add(new PercolateResponse.Match(index, match, score));
                    }
                    if (finalMatches.size() == requestedSize) {
                        break;
                    }
                }
            }

            assert !shardResults.isEmpty();
            InternalAggregations reducedAggregations = reduceAggregations(shardResults, headersContext);
            return new ReduceResult(foundMatches, finalMatches.toArray(new PercolateResponse.Match[finalMatches.size()]), reducedAggregations);
        }

        @Override
        public TopScoreDocCollector getCollector(PercolateContext context) {
            return TopScoreDocCollector.create(context.size());
        }

        @Override
        public PercolateShardResponse processResults(PercolateContext context, TopScoreDocCollector collector) {
            TopDocs topDocs = collector.topDocs();
            long count = topDocs.totalHits;
            List<BytesRef> matches = new ArrayList<>(topDocs.scoreDocs.length);
            float[] scores = new float[topDocs.scoreDocs.length];
            List<Map<String, HighlightField>> hls = null;
            if (context.highlight() != null) {
                hls = new ArrayList<>(topDocs.scoreDocs.length);
            }

            final MappedFieldType uidMapper = context.mapperService().smartNameFieldType(UidFieldMapper.NAME);
            final IndexFieldData<?> uidFieldData = context.fieldData().getForField(uidMapper);
            int i = 0;
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                int segmentIdx = ReaderUtil.subIndex(scoreDoc.doc, context.searcher().getIndexReader().leaves());
                LeafReaderContext atomicReaderContext = context.searcher().getIndexReader().leaves().get(segmentIdx);
                SortedBinaryDocValues values = uidFieldData.load(atomicReaderContext).getBytesValues();
                final int segmentDocId = scoreDoc.doc - atomicReaderContext.docBase;
                values.setDocument(segmentDocId);
                final int numValues = values.count();
                assert numValues == 1;
                BytesRef bytes = Uid.splitUidIntoTypeAndId(values.valueAt(0))[1];
                matches.add(BytesRef.deepCopyOf(bytes));
                if (hls != null) {
                    Query query = context.percolateQueries().get(bytes);
                    context.parsedQuery(new ParsedQuery(query));
                    context.hitContext().cache().clear();
                    highlightPhase.hitExecute(context, context.hitContext());
                    hls.add(i, context.hitContext().hit().getHighlightFields());
                }
                scores[i++] = scoreDoc.score;
            }
            if (hls != null) {
                return new PercolateShardResponse(matches.toArray(new BytesRef[matches.size()]), hls, count, scores, context);
            } else {
                return new PercolateShardResponse(matches.toArray(new BytesRef[matches.size()]), count, scores, context);
            }
        }

    };

    PercolateShardResponse doPercolate(PercolatorType percolatorType, PercolateContext context, boolean percolatorIndexSearcherHoldsNestedDocs) throws IOException {
        final MappedFieldType uidMapper = context.mapperService().smartNameFieldType(UidFieldMapper.NAME);
        IndexFieldData<?> uidFieldData = context.fieldData().getForField(uidMapper);

        Collector typeCollector = percolatorType.getCollector(context);
        BucketCollector aggregatorCollector = null;
        if (context.aggregations() != null) {
            AggregationContext aggregationContext = new AggregationContext(context);
            context.aggregations().aggregationContext(aggregationContext);

            Aggregator[] aggregators = context.aggregations().factories().createTopLevelAggregators(aggregationContext);
            List<Aggregator> aggregatorCollectors = new ArrayList<>(aggregators.length);
            for (int i = 0; i < aggregators.length; i++) {
                if (!(aggregators[i] instanceof GlobalAggregator)) {
                    Aggregator aggregator = aggregators[i];
                    aggregatorCollectors.add(aggregator);
                }
            }
            context.aggregations().aggregators(aggregators);
            aggregatorCollector = BucketCollector.wrap(aggregatorCollectors);
            aggregatorCollector.preCollection();
        }

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        if (context.aliasFilter() != null) {
            builder.add(context.aliasFilter(), FILTER);
        }
        if (context.indexService().indexSettings().getAsVersion(IndexMetaData.SETTING_VERSION_CREATED, null).onOrAfter(Version.V_2_1_0)) {
            builder.add(context.getQueryMetaDataQuery(), FILTER);
        }
        if (context.percolateQuery() != null){
            builder.add(context.percolateQuery(), MUST);
        }
        Query percolatorTypeFilter = context.indexService().mapperService().documentMapper(TYPE_NAME).typeFilter();
        builder.add(percolatorTypeFilter, FILTER);
        Query query = builder.build();
        PercolatorQuery percolatorQuery = new PercolatorQuery(query, uidFieldData, context.docSearcher(), percolatorIndexSearcherHoldsNestedDocs, context.percolateQueries());
        context.searcher().search(percolatorQuery, MultiCollector.wrap(typeCollector, aggregatorCollector));
        if (aggregatorCollector != null) {
            aggregatorCollector.postCollection();
            aggregationPhase.execute(context);
        }
        return percolatorType.processResults(context, typeCollector);
    }

    public final static class ReduceResult {

        private final long count;
        private final PercolateResponse.Match[] matches;
        private final InternalAggregations reducedAggregations;

        ReduceResult(long count, PercolateResponse.Match[] matches, InternalAggregations reducedAggregations) {
            this.count = count;
            this.matches = matches;
            this.reducedAggregations = reducedAggregations;
        }

        public ReduceResult(long count, InternalAggregations reducedAggregations) {
            this.count = count;
            this.matches = null;
            this.reducedAggregations = reducedAggregations;
        }

        public long count() {
            return count;
        }

        public PercolateResponse.Match[] matches() {
            return matches;
        }

        public InternalAggregations reducedAggregations() {
            return reducedAggregations;
        }
    }

    private InternalAggregations reduceAggregations(List<PercolateShardResponse> shardResults, HasContextAndHeaders headersContext) {
        if (shardResults.get(0).aggregations() == null) {
            return null;
        }

        List<InternalAggregations> aggregationsList = new ArrayList<>(shardResults.size());
        for (PercolateShardResponse shardResult : shardResults) {
            aggregationsList.add(shardResult.aggregations());
        }
        InternalAggregations aggregations = InternalAggregations.reduce(aggregationsList, new ReduceContext(bigArrays, scriptService,
                headersContext));
        if (aggregations != null) {
            List<SiblingPipelineAggregator> pipelineAggregators = shardResults.get(0).pipelineAggregators();
            if (pipelineAggregators != null) {
                List<InternalAggregation> newAggs = StreamSupport.stream(aggregations.spliterator(), false).map((p) -> {
                    return (InternalAggregation) p;
                }).collect(Collectors.toList());
                for (SiblingPipelineAggregator pipelineAggregator : pipelineAggregators) {
                    InternalAggregation newAgg = pipelineAggregator.doReduce(new InternalAggregations(newAggs), new ReduceContext(
                            bigArrays, scriptService, headersContext));
                    newAggs.add(newAgg);
                }
                aggregations = new InternalAggregations(newAggs);
            }
        }
        return aggregations;
    }

}
