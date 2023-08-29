/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A query that performs kNN search using Lucene's {@link org.apache.lucene.search.KnnFloatVectorQuery} or
 * {@link org.apache.lucene.search.KnnByteVectorQuery}.
 *
 */
public class KnnVectorQueryBuilder extends AbstractQueryBuilder<KnnVectorQueryBuilder> {
    public static final String NAME = "knn";
    private static final int NUM_CANDS_LIMIT = 10000;
    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField NUM_CANDS_FIELD = new ParseField("num_candidates");
    public static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");
    public static final ParseField QUERY_VECTOR_BUILDER_FIELD = new ParseField("query_vector_builder");
    public static final ParseField VECTOR_SIMILARITY_FIELD = new ParseField("similarity");
    public static final ParseField FILTER_FIELD = new ParseField("filter");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<KnnVectorQueryBuilder, Void> PARSER = new ConstructingObjectParser<>("knn", args -> {
        List<Float> vector = (List<Float>) args[1];
        final float[] vectorArray;
        if (vector != null) {
            vectorArray = new float[vector.size()];
            for (int i = 0; i < vector.size(); i++) {
                vectorArray[i] = vector.get(i);
            }
        } else {
            vectorArray = null;
        }
        return new KnnVectorQueryBuilder((String) args[0], vectorArray, (QueryVectorBuilder) args[2], (int) args[3], (Float) args[4]);
    });

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareFloatArray(optionalConstructorArg(), QUERY_VECTOR_FIELD);
        PARSER.declareNamedObject(
            optionalConstructorArg(),
            (p, c, n) -> p.namedObject(QueryVectorBuilder.class, n, c),
            QUERY_VECTOR_BUILDER_FIELD
        );
        // TODO: make num_candidates optional
        PARSER.declareInt(constructorArg(), NUM_CANDS_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), VECTOR_SIMILARITY_FIELD);
        PARSER.declareFieldArray(
            KnnVectorQueryBuilder::addFilterQueries,
            (p, c) -> AbstractQueryBuilder.parseTopLevelQuery(p),
            FILTER_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY
        );
        declareStandardFields(PARSER);
    }

    public static KnnVectorQueryBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String fieldName;
    private final float[] queryVector;
    private final QueryVectorBuilder queryVectorBuilder;
    private final Supplier<float[]> querySupplier;
    private final int numCands;
    private final List<QueryBuilder> filterQueries = new ArrayList<>();
    private final Float vectorSimilarity;

    private KnnVectorQueryBuilder(
        String fieldName,
        float[] queryVector,
        QueryVectorBuilder queryVectorBuilder,
        int numCands,
        Float vectorSimilarity
    ) {
        if (numCands > NUM_CANDS_LIMIT) {
            throw new IllegalArgumentException("[" + NUM_CANDS_FIELD.getPreferredName() + "] cannot exceed [" + NUM_CANDS_LIMIT + "]");
        }
        if (queryVector == null && queryVectorBuilder == null) {
            throw new IllegalArgumentException(
                format(
                    "either [%s] or [%s] must be provided",
                    QUERY_VECTOR_BUILDER_FIELD.getPreferredName(),
                    QUERY_VECTOR_FIELD.getPreferredName()
                )
            );
        }
        if (queryVector != null && queryVectorBuilder != null) {
            throw new IllegalArgumentException(
                format(
                    "cannot provide both [%s] and [%s]",
                    QUERY_VECTOR_BUILDER_FIELD.getPreferredName(),
                    QUERY_VECTOR_FIELD.getPreferredName()
                )
            );
        }
        this.fieldName = fieldName;
        this.queryVector = queryVector == null ? new float[0] : queryVector;
        this.queryVectorBuilder = queryVectorBuilder;
        this.querySupplier = null;
        this.numCands = numCands;
        this.vectorSimilarity = vectorSimilarity;
    }

    public KnnVectorQueryBuilder(String fieldName, float[] queryVector, int numCands, Float vectorSimilarity) {
        this(fieldName, queryVector, null, numCands, vectorSimilarity);
    }

    public KnnVectorQueryBuilder(String fieldName, Supplier<float[]> querySupplier, int numCands, Float vectorSimilarity) {
        this.fieldName = fieldName;
        this.queryVector = new float[0];
        this.queryVectorBuilder = null;
        this.querySupplier = querySupplier;
        this.numCands = numCands;
        this.vectorSimilarity = vectorSimilarity;
    }

    public KnnVectorQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.numCands = in.readVInt();
        if (in.getTransportVersion().before(TransportVersion.V_8_7_0)) {
            this.queryVector = in.readFloatArray();
        } else {
            this.queryVector = in.readBoolean() ? in.readFloatArray() : null;
            if (in.getTransportVersion().before(TransportVersion.V_8_500_066)) {
                in.readBoolean(); // used for byteQueryVector, which was always null
            }
        }
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_2_0)) {
            this.filterQueries.addAll(readQueries(in));
        }
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            this.vectorSimilarity = in.readOptionalFloat();
        } else {
            this.vectorSimilarity = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_500_066)) {
            this.queryVectorBuilder = in.readOptionalNamedWriteable(QueryVectorBuilder.class);
        } else {
            this.queryVectorBuilder = null;
        }
        this.querySupplier = null;
    }

    public String getFieldName() {
        return fieldName;
    }

    @Nullable
    public float[] queryVector() {
        return queryVector;
    }

    @Nullable
    public Float getVectorSimilarity() {
        return vectorSimilarity;
    }

    public int numCands() {
        return numCands;
    }

    public List<QueryBuilder> filterQueries() {
        return filterQueries;
    }

    public KnnVectorQueryBuilder addFilterQuery(QueryBuilder filterQuery) {
        Objects.requireNonNull(filterQuery);
        this.filterQueries.add(filterQuery);
        return this;
    }

    public KnnVectorQueryBuilder addFilterQueries(List<QueryBuilder> filterQueries) {
        Objects.requireNonNull(filterQueries);
        this.filterQueries.addAll(filterQueries);
        return this;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (querySupplier != null) {
            throw new IllegalStateException("missing a rewriteAndFetch?");
        }
        out.writeString(fieldName);
        out.writeVInt(numCands);
        if (out.getTransportVersion().before(TransportVersion.V_8_7_0)) {
            out.writeFloatArray(queryVector);
        } else {
            if (queryVector == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeFloatArray(queryVector);
            }
            if (out.getTransportVersion().before(TransportVersion.V_8_500_066)) {
                out.writeBoolean(false); // used for byteQueryVector, which was always null
            }
        }
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_2_0)) {
            writeQueries(out, filterQueries);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            out.writeOptionalFloat(vectorSimilarity);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_500_066)) {
            out.writeOptionalNamedWriteable(queryVectorBuilder);
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(FIELD_FIELD.getPreferredName(), fieldName);
        if (queryVectorBuilder != null) {
            builder.startObject(QUERY_VECTOR_BUILDER_FIELD.getPreferredName());
            builder.field(queryVectorBuilder.getWriteableName(), queryVectorBuilder);
            builder.endObject();
        } else {
            builder.field(QUERY_VECTOR_FIELD.getPreferredName(), queryVector);
        }
        builder.field(NUM_CANDS_FIELD.getPreferredName(), numCands);
        if (vectorSimilarity != null) {
            builder.field(VECTOR_SIMILARITY_FIELD.getPreferredName(), vectorSimilarity);
        }
        if (filterQueries.isEmpty() == false) {
            builder.startArray(FILTER_FIELD.getPreferredName());
            for (QueryBuilder filterQuery : filterQueries) {
                filterQuery.toXContent(builder, params);
            }
            builder.endArray();
        }
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext ctx) throws IOException {
        if (querySupplier != null) {
            if (querySupplier.get() == null) {
                return this;
            }
            return new KnnVectorQueryBuilder(fieldName, querySupplier.get(), numCands, vectorSimilarity).boost(boost)
                .queryName(queryName)
                .addFilterQueries(filterQueries);
        }
        if (queryVectorBuilder != null) {
            SetOnce<float[]> toSet = new SetOnce<>();
            ctx.registerAsyncAction((c, l) -> queryVectorBuilder.buildVector(c, l.delegateFailureAndWrap((ll, v) -> {
                toSet.set(v);
                if (v == null) {
                    ll.onFailure(
                        new IllegalArgumentException(
                            format(
                                "[%s] with name [%s] returned null query_vector",
                                QUERY_VECTOR_BUILDER_FIELD.getPreferredName(),
                                queryVectorBuilder.getWriteableName()
                            )
                        )
                    );
                    return;
                }
                ll.onResponse(null);
            })));
            return new KnnVectorQueryBuilder(fieldName, toSet::get, numCands, vectorSimilarity).boost(boost)
                .queryName(queryName)
                .addFilterQueries(filterQueries);
        }

        boolean changed = false;
        if (ctx instanceof SearchExecutionContext searchExecutionContext && searchExecutionContext.getAliasFilter() != null) {
            filterQueries.add(searchExecutionContext.getAliasFilter());
        }
        List<QueryBuilder> rewrittenQueries = new ArrayList<>(filterQueries.size());
        for (QueryBuilder query : filterQueries) {
            QueryBuilder rewrittenQuery = query.rewrite(ctx);
            if (rewrittenQuery instanceof MatchNoneQueryBuilder) {
                return rewrittenQuery;
            }
            if (rewrittenQuery != query) {
                changed = true;
            }
            rewrittenQueries.add(rewrittenQuery);
        }
        if (changed) {
            return new KnnVectorQueryBuilder(fieldName, queryVector, numCands, vectorSimilarity).boost(boost)
                .queryName(queryName)
                .addFilterQueries(rewrittenQueries);
        }
        return this;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        MappedFieldType fieldType = context.getFieldType(fieldName);
        if (fieldType == null) {
            throw new IllegalArgumentException("field [" + fieldName + "] does not exist in the mapping");
        }

        if (fieldType instanceof DenseVectorFieldType == false) {
            throw new IllegalArgumentException(
                "[" + NAME + "] queries are only supported on [" + DenseVectorFieldMapper.CONTENT_TYPE + "] fields"
            );
        }

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (QueryBuilder query : this.filterQueries) {
            builder.add(query.toQuery(context), BooleanClause.Occur.FILTER);
        }
        BooleanQuery booleanQuery = builder.build();
        Query filterQuery = booleanQuery.clauses().isEmpty() ? null : booleanQuery;

        DenseVectorFieldType vectorFieldType = (DenseVectorFieldType) fieldType;
        return vectorFieldType.createKnnQuery(queryVector, numCands, filterQuery, vectorSimilarity);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(
            fieldName,
            Arrays.hashCode(queryVector),
            querySupplier,
            queryVectorBuilder,
            numCands,
            filterQueries,
            vectorSimilarity
        );
    }

    @Override
    protected boolean doEquals(KnnVectorQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Arrays.equals(queryVector, other.queryVector)
            && Objects.equals(queryVectorBuilder, other.queryVectorBuilder)
            && Objects.equals(querySupplier, other.querySupplier)
            && numCands == other.numCands
            && Objects.equals(filterQueries, other.filterQueries)
            && Objects.equals(vectorSimilarity, other.vectorSimilarity);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_0_0;
    }
}
