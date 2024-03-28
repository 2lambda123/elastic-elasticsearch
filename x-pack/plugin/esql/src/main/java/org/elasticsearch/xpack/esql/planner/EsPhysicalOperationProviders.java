/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.LuceneCountOperator;
import org.elasticsearch.compute.lucene.LuceneOperator;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.LuceneTopNSourceOperator;
import org.elasticsearch.compute.lucene.TimeSeriesSortedSourceOperatorFactory;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OrdinalsGroupingOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.NestedHelper;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec.FieldSort;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.DriverParallelism;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.LocalExecutionPlannerContext;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.PhysicalOperation;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.esql.type.MultiTypeEsField;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.type.DataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;

import static org.elasticsearch.common.lucene.search.Queries.newNonNestedFilter;
import static org.elasticsearch.compute.lucene.LuceneSourceOperator.NO_LIMIT;
import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.NONE;

public class EsPhysicalOperationProviders extends AbstractPhysicalOperationProviders {
    /**
     * Context of each shard we're operating against.
     */
    public interface ShardContext extends org.elasticsearch.compute.lucene.ShardContext {
        /**
         * Build something to load source {@code _source}.
         */
        SourceLoader newSourceLoader();

        /**
         * Convert a {@link QueryBuilder} into a real {@link Query lucene query}.
         */
        Query toQuery(QueryBuilder queryBuilder);

        /**
         * Returns something to load values from this field into a {@link Block}.
         */
        BlockLoader blockLoader(String name, boolean asUnsupportedSource, MappedFieldType.FieldExtractPreference fieldExtractPreference);
    }

    private final List<ShardContext> shardContexts;

    public EsPhysicalOperationProviders(List<ShardContext> shardContexts) {
        this.shardContexts = shardContexts;
    }

    @Override
    public final PhysicalOperation fieldExtractPhysicalOperation(FieldExtractExec fieldExtractExec, PhysicalOperation source) {
        Layout.Builder layout = source.layout.builder();
        var sourceAttr = fieldExtractExec.sourceAttribute();
        List<ValuesSourceReaderOperator.ShardContext> readers = shardContexts.stream()
            .map(s -> new ValuesSourceReaderOperator.ShardContext(s.searcher().getIndexReader(), s::newSourceLoader))
            .toList();
        List<ValuesSourceReaderOperator.FieldInfo> fields = new ArrayList<>();
        int docChannel = source.layout.get(sourceAttr.id()).channel();
        var docValuesAttrs = fieldExtractExec.docValuesAttributes();
        for (Attribute attr : fieldExtractExec.attributesToExtract()) {
            layout.append(attr);
            var unionTypes = findUnionTypes(attr);
            DataType dataType = attr.dataType();
            MappedFieldType.FieldExtractPreference fieldExtractPreference = PlannerUtils.extractPreference(docValuesAttrs.contains(attr));
            ElementType elementType = PlannerUtils.toElementType(dataType, fieldExtractPreference);
            String fieldName = attr.name();
            boolean isSupported = EsqlDataTypes.isUnsupported(dataType);
            IntFunction<BlockLoader> loader = s -> getBlockLoaderFor(s, fieldName, isSupported, fieldExtractPreference, unionTypes);
            fields.add(new ValuesSourceReaderOperator.FieldInfo(fieldName, elementType, loader));
        }
        return source.with(new ValuesSourceReaderOperator.Factory(fields, readers, docChannel), layout.build());
    }

    private BlockLoader getBlockLoaderFor(
        int shardId,
        String fieldName,
        boolean isSupported,
        MappedFieldType.FieldExtractPreference fieldExtractPreference,
        MultiTypeEsField unionTypes
    ) {
        DefaultShardContext shardContext = (DefaultShardContext) shardContexts.get(shardId);
        if (unionTypes != null && unionTypes.getName().equals(fieldName)) {
            String indexName = shardContext.ctx.index().getName();
            Expression conversion = unionTypes.getConversionExpressionForIndex(indexName);
            var typeConvertingShardContext = new TypeConvertingShardContext(shardContext, (AbstractConvertFunction) conversion);
            return typeConvertingShardContext.blockLoader(fieldName, isSupported, fieldExtractPreference);
        }
        return shardContext.blockLoader(fieldName, isSupported, fieldExtractPreference);
    }

    private MultiTypeEsField findUnionTypes(Attribute attr) {
        if (attr instanceof FieldAttribute fa && fa.field() instanceof MultiTypeEsField multiTypeEsField) {
            return multiTypeEsField;
        }
        return null;
    }

    public Function<org.elasticsearch.compute.lucene.ShardContext, Query> querySupplier(QueryBuilder builder) {
        QueryBuilder qb = builder == null ? QueryBuilders.matchAllQuery() : builder;
        return ctx -> shardContexts.get(ctx.index()).toQuery(qb);
    }

    @Override
    public final PhysicalOperation sourcePhysicalOperation(EsQueryExec esQueryExec, LocalExecutionPlannerContext context) {
        final LuceneOperator.Factory luceneFactory;

        List<FieldSort> sorts = esQueryExec.sorts();
        List<SortBuilder<?>> fieldSorts = null;
        assert esQueryExec.estimatedRowSize() != null : "estimated row size not initialized";
        int rowEstimatedSize = esQueryExec.estimatedRowSize();
        int limit = esQueryExec.limit() != null ? (Integer) esQueryExec.limit().fold() : NO_LIMIT;
        if (sorts != null && sorts.isEmpty() == false) {
            fieldSorts = new ArrayList<>(sorts.size());
            for (FieldSort sort : sorts) {
                fieldSorts.add(sort.fieldSortBuilder());
            }
            luceneFactory = new LuceneTopNSourceOperator.Factory(
                shardContexts,
                querySupplier(esQueryExec.query()),
                context.queryPragmas().dataPartitioning(),
                context.queryPragmas().taskConcurrency(),
                context.pageSize(rowEstimatedSize),
                limit,
                fieldSorts
            );
        } else {
            if (context.queryPragmas().timeSeriesMode()) {
                luceneFactory = TimeSeriesSortedSourceOperatorFactory.create(
                    limit,
                    context.pageSize(rowEstimatedSize),
                    context.queryPragmas().taskConcurrency(),
                    TimeValue.ZERO,
                    shardContexts,
                    querySupplier(esQueryExec.query())
                );
            } else {
                luceneFactory = new LuceneSourceOperator.Factory(
                    shardContexts,
                    querySupplier(esQueryExec.query()),
                    context.queryPragmas().dataPartitioning(),
                    context.queryPragmas().taskConcurrency(),
                    context.pageSize(rowEstimatedSize),
                    limit
                );
            }
        }
        Layout.Builder layout = new Layout.Builder();
        layout.append(esQueryExec.output());
        int instanceCount = Math.max(1, luceneFactory.taskConcurrency());
        context.driverParallelism(new DriverParallelism(DriverParallelism.Type.DATA_PARALLELISM, instanceCount));
        return PhysicalOperation.fromSource(luceneFactory, layout.build());
    }

    /**
     * Build a {@link SourceOperator.SourceOperatorFactory} that counts documents in the search index.
     */
    public LuceneCountOperator.Factory countSource(LocalExecutionPlannerContext context, QueryBuilder queryBuilder, Expression limit) {
        return new LuceneCountOperator.Factory(
            shardContexts,
            querySupplier(queryBuilder),
            context.queryPragmas().dataPartitioning(),
            context.queryPragmas().taskConcurrency(),
            limit == null ? NO_LIMIT : (Integer) limit.fold()
        );
    }

    @Override
    public final Operator.OperatorFactory ordinalGroupingOperatorFactory(
        LocalExecutionPlanner.PhysicalOperation source,
        AggregateExec aggregateExec,
        List<GroupingAggregator.Factory> aggregatorFactories,
        Attribute attrSource,
        ElementType groupElementType,
        LocalExecutionPlannerContext context
    ) {
        var sourceAttribute = FieldExtractExec.extractSourceAttributesFrom(aggregateExec.child());
        int docChannel = source.layout.get(sourceAttribute.id()).channel();
        List<ValuesSourceReaderOperator.ShardContext> vsShardContexts = shardContexts.stream()
            .map(s -> new ValuesSourceReaderOperator.ShardContext(s.searcher().getIndexReader(), s::newSourceLoader))
            .toList();
        // The grouping-by values are ready, let's group on them directly.
        // Costin: why are they ready and not already exposed in the layout?
        boolean isUnsupported = EsqlDataTypes.isUnsupported(attrSource.dataType());
        return new OrdinalsGroupingOperator.OrdinalsGroupingOperatorFactory(
            shardIdx -> shardContexts.get(shardIdx).blockLoader(attrSource.name(), isUnsupported, NONE),
            vsShardContexts,
            groupElementType,
            docChannel,
            attrSource.name(),
            aggregatorFactories,
            context.pageSize(aggregateExec.estimatedRowSize())
        );
    }

    public static class DefaultShardContext implements ShardContext {
        private final int index;
        private final SearchExecutionContext ctx;
        private final AliasFilter aliasFilter;

        public DefaultShardContext(int index, SearchExecutionContext ctx, AliasFilter aliasFilter) {
            this.index = index;
            this.ctx = ctx;
            this.aliasFilter = aliasFilter;
        }

        @Override
        public int index() {
            return index;
        }

        @Override
        public IndexSearcher searcher() {
            return ctx.searcher();
        }

        @Override
        public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sorts) throws IOException {
            return SortBuilder.buildSort(sorts, ctx);
        }

        @Override
        public String shardIdentifier() {
            return ctx.getFullyQualifiedIndex().getName() + ":" + ctx.getShardId();
        }

        @Override
        public SourceLoader newSourceLoader() {
            return ctx.newSourceLoader(false);
        }

        @Override
        public Query toQuery(QueryBuilder queryBuilder) {
            Query query = ctx.toQuery(queryBuilder).query();
            NestedLookup nestedLookup = ctx.nestedLookup();
            if (nestedLookup != NestedLookup.EMPTY) {
                NestedHelper nestedHelper = new NestedHelper(nestedLookup, ctx::isFieldMapped);
                if (nestedHelper.mightMatchNestedDocs(query)) {
                    // filter out nested documents
                    query = new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST)
                        .add(newNonNestedFilter(ctx.indexVersionCreated()), BooleanClause.Occur.FILTER)
                        .build();
                }
            }
            if (aliasFilter != AliasFilter.EMPTY) {
                Query filterQuery = ctx.toQuery(aliasFilter.getQueryBuilder()).query();
                query = new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST)
                    .add(filterQuery, BooleanClause.Occur.FILTER)
                    .build();
            }
            return query;
        }

        @Override
        public BlockLoader blockLoader(
            String name,
            boolean asUnsupportedSource,
            MappedFieldType.FieldExtractPreference fieldExtractPreference
        ) {
            if (asUnsupportedSource) {
                return BlockLoader.CONSTANT_NULLS;
            }
            MappedFieldType fieldType = ctx.getFieldType(name);
            if (fieldType == null) {
                // the field does not exist in this context
                return BlockLoader.CONSTANT_NULLS;
            }
            BlockLoader loader = fieldType.blockLoader(new MappedFieldType.BlockLoaderContext() {
                @Override
                public String indexName() {
                    return ctx.getFullyQualifiedIndex().getName();
                }

                @Override
                public MappedFieldType.FieldExtractPreference fieldExtractPreference() {
                    return fieldExtractPreference;
                }

                @Override
                public SearchLookup lookup() {
                    return ctx.lookup();
                }

                @Override
                public Set<String> sourcePaths(String name) {
                    return ctx.sourcePath(name);
                }

                @Override
                public String parentField(String field) {
                    return ctx.parentPath(field);
                }

                @Override
                public FieldNamesFieldMapper.FieldNamesFieldType fieldNames() {
                    return (FieldNamesFieldMapper.FieldNamesFieldType) ctx.lookup().fieldType(FieldNamesFieldMapper.NAME);
                }
            });
            if (loader == null) {
                HeaderWarning.addWarning("Field [{}] cannot be retrieved, it is unsupported or not indexed; returning null", name);
                return BlockLoader.CONSTANT_NULLS;
            }

            return loader;
        }
    }

    public static class TypeConvertingShardContext extends DefaultShardContext {
        private final AbstractConvertFunction convertFunction;

        public TypeConvertingShardContext(DefaultShardContext delegate, AbstractConvertFunction convertFunction) {
            super(delegate.index, delegate.ctx, delegate.aliasFilter);
            this.convertFunction = convertFunction;
        }

        @Override
        public BlockLoader blockLoader(
            String name,
            boolean asUnsupportedSource,
            MappedFieldType.FieldExtractPreference fieldExtractPreference
        ) {
            return new TypeConvertingBlockLoader(super.blockLoader(name, asUnsupportedSource, fieldExtractPreference), convertFunction);
        }
    }

    static class TypeConvertingBlockLoader implements BlockLoader {
        protected final BlockLoader delegate;
        DriverContext driverContext;
        private EvalOperator.ExpressionEvaluator convertEvaluator;

        protected TypeConvertingBlockLoader(BlockLoader delegate, AbstractConvertFunction convertFunction) {
            this.delegate = delegate;
            this.driverContext = new DriverContext(
                BigArrays.NON_RECYCLING_INSTANCE,
                new org.elasticsearch.compute.data.BlockFactory(
                    new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                    BigArrays.NON_RECYCLING_INSTANCE
                )
            );
            this.convertEvaluator = convertFunction.toEvaluator(e -> driverContext -> new EvalOperator.ExpressionEvaluator() {
                @Override
                public org.elasticsearch.compute.data.Block eval(Page page) {
                    return page.getBlock(0);
                }

                @Override
                public void close() {}
            }).get(driverContext);
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            Builder indexTypeBuilder = delegate.builder(factory, expectedCount);
            return new OutputTypeBuilder(indexTypeBuilder, convertEvaluator);
        }

        @Override
        public ColumnAtATimeReader columnAtATimeReader(LeafReaderContext context) throws IOException {
            ColumnAtATimeReader reader = delegate.columnAtATimeReader(context);
            if (reader == null) {
                return null;
            }
            return new ColumnAtATimeReader() {
                @Override
                public Block read(BlockFactory factory, Docs docs) throws IOException {
                    Block block = reader.read(factory, docs);
                    Page page = new Page((org.elasticsearch.compute.data.Block) block);
                    org.elasticsearch.compute.data.Block converted = convertEvaluator.eval(page);
                    return converted;
                }

                @Override
                public boolean canReuse(int startingDocID) {
                    return reader.canReuse(startingDocID);
                }

                @Override
                public String toString() {
                    return "Delegating[to=" + delegatingTo() + ", impl=" + reader + "]";
                }
            };
        }

        @Override
        public RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException {
            RowStrideReader reader = delegate.rowStrideReader(context);
            if (reader == null) {
                return null;
            }
            return new RowStrideReader() {
                @Override
                public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
                    // TODO: Support union-types
                    reader.read(docId, storedFields, builder);
                }

                @Override
                public boolean canReuse(int startingDocID) {
                    return reader.canReuse(startingDocID);
                }

                @Override
                public String toString() {
                    return "Delegating[to=" + delegatingTo() + ", impl=" + reader + "]";
                }
            };
        }

        private static class OutputTypeBuilder implements Builder, DelegatingBuilder {
            private final Builder delegate;
            private final EvalOperator.ExpressionEvaluator convertEvaluator;

            private OutputTypeBuilder(Builder delegate, EvalOperator.ExpressionEvaluator convertEvaluator) {
                this.delegate = delegate;
                this.convertEvaluator = convertEvaluator;
            }

            @Override
            public void close() {
                delegate.close();
            }

            @Override
            public Block build() {
                Block fromIndex = delegate.build();
                Page page = new Page((org.elasticsearch.compute.data.Block) fromIndex);
                org.elasticsearch.compute.data.Block converted = convertEvaluator.eval(page);
                return converted;
            }

            @Override
            public Builder appendNull() {
                return delegate.appendNull();
            }

            @Override
            public Builder beginPositionEntry() {
                return delegate.beginPositionEntry();
            }

            @Override
            public Builder endPositionEntry() {
                return delegate.endPositionEntry();
            }

            @Override
            public Builder delegate() {
                return delegate;
            }
        }

        @Override
        public StoredFieldsSpec rowStrideStoredFieldSpec() {
            return delegate.rowStrideStoredFieldSpec();
        }

        @Override
        public boolean supportsOrdinals() {
            return delegate.supportsOrdinals();
        }

        @Override
        public SortedSetDocValues ordinals(LeafReaderContext context) throws IOException {
            return delegate.ordinals(context);
        }

        protected String delegatingTo() {
            return delegate.toString();
        }

        @Override
        public final String toString() {
            return "Delegating[to=" + delegatingTo() + ", impl=" + delegate + "]";
        }
    }
}
