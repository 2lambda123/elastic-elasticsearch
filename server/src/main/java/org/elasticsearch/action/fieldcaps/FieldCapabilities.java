/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Describes the capabilities of a field optionally merged across multiple indices.
 */
public class FieldCapabilities implements Writeable, ToXContentObject {

    private static final ParseField TYPE_FIELD = new ParseField("type");
    private static final ParseField IS_METADATA_FIELD = new ParseField("metadata_field");
    private static final ParseField SEARCHABLE_FIELD = new ParseField("searchable");
    private static final ParseField AGGREGATABLE_FIELD = new ParseField("aggregatable");
    private static final ParseField INDICES_FIELD = new ParseField("indices");
    private static final ParseField NON_SEARCHABLE_INDICES_FIELD = new ParseField("non_searchable_indices");
    private static final ParseField NON_AGGREGATABLE_INDICES_FIELD = new ParseField("non_aggregatable_indices");
    private static final ParseField META_FIELD = new ParseField("meta");

    private final String name;
    private final String type;
    private final boolean isMetadataField;
    private final boolean isSearchable;
    private final boolean isAggregatable;

    private final String[] indices;
    private final String[] nonSearchableIndices;
    private final String[] nonAggregatableIndices;

    private final Map<String, Set<String>> meta;

    /**
     * Constructor for a set of indices.
     * @param name The name of the field
     * @param type The type associated with the field.
     * @param isMetadataField Whether this field is a metadata field.
     * @param isSearchable Whether this field is indexed for search.
     * @param isAggregatable Whether this field can be aggregated on.
     * @param indices The list of indices where this field name is defined as {@code type},
     *                or null if all indices have the same {@code type} for the field.
     * @param nonSearchableIndices The list of indices where this field is not searchable,
     *                             or null if the field is searchable in all indices.
     * @param nonAggregatableIndices The list of indices where this field is not aggregatable,
     *                               or null if the field is aggregatable in all indices.
     * @param meta Merged metadata across indices.
     */
    public FieldCapabilities(
        String name,
        String type,
        boolean isMetadataField,
        boolean isSearchable,
        boolean isAggregatable,
        String[] indices,
        String[] nonSearchableIndices,
        String[] nonAggregatableIndices,
        Map<String, Set<String>> meta
    ) {
        this.name = name;
        this.type = type;
        this.isMetadataField = isMetadataField;
        this.isSearchable = isSearchable;
        this.isAggregatable = isAggregatable;
        this.indices = indices;
        this.nonSearchableIndices = nonSearchableIndices;
        this.nonAggregatableIndices = nonAggregatableIndices;
        this.meta = Objects.requireNonNull(meta);
    }

    FieldCapabilities(StreamInput in) throws IOException {
        this.name = in.readString();
        this.type = in.readString();
        this.isMetadataField = in.getVersion().onOrAfter(Version.V_7_13_0) ? in.readBoolean() : false;
        this.isSearchable = in.readBoolean();
        this.isAggregatable = in.readBoolean();
        this.indices = in.readOptionalStringArray();
        this.nonSearchableIndices = in.readOptionalStringArray();
        this.nonAggregatableIndices = in.readOptionalStringArray();
        if (in.getVersion().onOrAfter(Version.V_7_6_0)) {
            meta = in.readMap(StreamInput::readString, i -> i.readSet(StreamInput::readString));
        } else {
            meta = Collections.emptyMap();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        if (out.getVersion().onOrAfter(Version.V_7_13_0)) {
            out.writeBoolean(isMetadataField);
        }
        out.writeBoolean(isSearchable);
        out.writeBoolean(isAggregatable);
        out.writeOptionalStringArray(indices);
        out.writeOptionalStringArray(nonSearchableIndices);
        out.writeOptionalStringArray(nonAggregatableIndices);
        if (out.getVersion().onOrAfter(Version.V_7_6_0)) {
            out.writeMap(meta, StreamOutput::writeString, (o, set) -> o.writeCollection(set, StreamOutput::writeString));
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE_FIELD.getPreferredName(), type);
        builder.field(IS_METADATA_FIELD.getPreferredName(), isMetadataField);
        builder.field(SEARCHABLE_FIELD.getPreferredName(), isSearchable);
        builder.field(AGGREGATABLE_FIELD.getPreferredName(), isAggregatable);
        if (indices != null) {
            builder.array(INDICES_FIELD.getPreferredName(), indices);
        }
        if (nonSearchableIndices != null) {
            builder.array(NON_SEARCHABLE_INDICES_FIELD.getPreferredName(), nonSearchableIndices);
        }
        if (nonAggregatableIndices != null) {
            builder.array(NON_AGGREGATABLE_INDICES_FIELD.getPreferredName(), nonAggregatableIndices);
        }
        if (meta.isEmpty() == false) {
            builder.startObject("meta");
            List<Map.Entry<String, Set<String>>> entries = new ArrayList<>(meta.entrySet());
            entries.sort(Comparator.comparing(Map.Entry::getKey)); // provide predictable order
            for (Map.Entry<String, Set<String>> entry : entries) {
                List<String> values = new ArrayList<>(entry.getValue());
                values.sort(String::compareTo); // provide predictable order
                builder.stringListField(entry.getKey(), values);
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static FieldCapabilities fromXContent(String name, XContentParser parser) throws IOException {
        return PARSER.parse(parser, name);
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<FieldCapabilities, String> PARSER = new ConstructingObjectParser<>(
        "field_capabilities",
        true,
        (a, name) -> new FieldCapabilities(
            name,
            (String) a[0],
            a[3] == null ? false : (boolean) a[3],
            (boolean) a[1],
            (boolean) a[2],
            a[4] != null ? ((List<String>) a[4]).toArray(new String[0]) : null,
            a[5] != null ? ((List<String>) a[5]).toArray(new String[0]) : null,
            a[6] != null ? ((List<String>) a[6]).toArray(new String[0]) : null,
            a[7] != null ? ((Map<String, Set<String>>) a[7]) : Collections.emptyMap()
        )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), SEARCHABLE_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), AGGREGATABLE_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), IS_METADATA_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), INDICES_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), NON_SEARCHABLE_INDICES_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), NON_AGGREGATABLE_INDICES_FIELD);
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (parser, context) -> parser.map(HashMap::new, p -> Collections.unmodifiableSet(new HashSet<>(p.list()))),
            META_FIELD
        );
    }

    /**
     * The name of the field.
     */
    public String getName() {
        return name;
    }

    /**
     * Whether this field is a metadata field.
     */
    public boolean isMetadataField() {
        return isMetadataField;
    }

    /**
     * Whether this field can be aggregated on all indices.
     */
    public boolean isAggregatable() {
        return isAggregatable;
    }

    /**
     * Whether this field is indexed for search on all indices.
     */
    public boolean isSearchable() {
        return isSearchable;
    }

    /**
     * The type of the field.
     */
    public String getType() {
        return type;
    }

    /**
     * The list of indices where this field name is defined as {@code type},
     * or null if all indices have the same {@code type} for the field.
     */
    public String[] indices() {
        return indices;
    }

    /**
     * The list of indices where this field is not searchable,
     * or null if the field is searchable in all indices.
     */
    public String[] nonSearchableIndices() {
        return nonSearchableIndices;
    }

    /**
     * The list of indices where this field is not aggregatable,
     * or null if the field is aggregatable in all indices.
     */
    public String[] nonAggregatableIndices() {
        return nonAggregatableIndices;
    }

    /**
     * Return merged metadata across indices.
     */
    public Map<String, Set<String>> meta() {
        return meta;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldCapabilities that = (FieldCapabilities) o;
        return isMetadataField == that.isMetadataField
            && isSearchable == that.isSearchable
            && isAggregatable == that.isAggregatable
            && Objects.equals(name, that.name)
            && Objects.equals(type, that.type)
            && Arrays.equals(indices, that.indices)
            && Arrays.equals(nonSearchableIndices, that.nonSearchableIndices)
            && Arrays.equals(nonAggregatableIndices, that.nonAggregatableIndices)
            && Objects.equals(meta, that.meta);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(name, type, isMetadataField, isSearchable, isAggregatable, meta);
        result = 31 * result + Arrays.hashCode(indices);
        result = 31 * result + Arrays.hashCode(nonSearchableIndices);
        result = 31 * result + Arrays.hashCode(nonAggregatableIndices);
        return result;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    static class Builder {
        private final String name;
        private final String type;
        private boolean isMetadataField;
        private int searchableIndices = 0;
        private int aggregatableIndices = 0;
        private final List<IndexCaps> indiceList;
        private final Map<String, Set<String>> meta;

        Builder(String name, String type) {
            this.name = name;
            this.type = type;
            this.indiceList = new ArrayList<>();
            this.meta = new HashMap<>();
        }

        /**
         * Collect the field capabilities for an index.
         */
        void add(String index, boolean isMetadataField, boolean search, boolean agg, Map<String, String> meta) {
            assert indiceList.isEmpty() || indiceList.get(indiceList.size() - 1).name.compareTo(index) < 0
                : "indices aren't sorted; previous [" + indiceList.get(indiceList.size() - 1).name + "], current [" + index + "]";
            indiceList.add(new IndexCaps(index, search, agg));
            if (search) {
                searchableIndices++;
            }
            if (agg) {
                aggregatableIndices++;
            }
            this.isMetadataField |= isMetadataField;
            for (Map.Entry<String, String> entry : meta.entrySet()) {
                this.meta.computeIfAbsent(entry.getKey(), key -> new HashSet<>()).add(entry.getValue());
            }
        }

        void getIndices(Collection<String> indices) {
            indiceList.forEach(cap -> indices.add(cap.name));
        }

        FieldCapabilities build(boolean withIndices) {
            final String[] indices;
            if (withIndices) {
                indices = indiceList.stream().map(caps -> caps.name).toArray(String[]::new);
            } else {
                indices = null;
            }

            // Iff this field is searchable in some indices AND non-searchable in others
            // we record the list of non-searchable indices
            final boolean isSearchable = searchableIndices == indiceList.size();
            final String[] nonSearchableIndices;
            if (isSearchable || searchableIndices == 0) {
                nonSearchableIndices = null;
            } else {
                nonSearchableIndices = new String[indiceList.size() - searchableIndices];
                int index = 0;
                for (IndexCaps indexCaps : indiceList) {
                    if (indexCaps.isSearchable == false) {
                        nonSearchableIndices[index++] = indexCaps.name;
                    }
                }
            }

            // Iff this field is aggregatable in some indices AND non-aggregatable in others
            // we keep the list of non-aggregatable indices
            final boolean isAggregatable = aggregatableIndices == indiceList.size();
            final String[] nonAggregatableIndices;
            if (isAggregatable || aggregatableIndices == 0) {
                nonAggregatableIndices = null;
            } else {
                nonAggregatableIndices = new String[indiceList.size() - aggregatableIndices];
                int index = 0;
                for (IndexCaps indexCaps : indiceList) {
                    if (indexCaps.isAggregatable == false) {
                        nonAggregatableIndices[index++] = indexCaps.name;
                    }
                }
            }
            final Function<Map.Entry<String, Set<String>>, Set<String>> entryValueFunction = Map.Entry::getValue;
            Map<String, Set<String>> immutableMeta = Collections.unmodifiableMap(
                meta.entrySet()
                    .stream()
                    .collect(
                        Collectors.toMap(Map.Entry::getKey, entryValueFunction.andThen(HashSet::new).andThen(Collections::unmodifiableSet))
                    )
            );
            return new FieldCapabilities(
                name,
                type,
                isMetadataField,
                isSearchable,
                isAggregatable,
                indices,
                nonSearchableIndices,
                nonAggregatableIndices,
                immutableMeta
            );
        }
    }

    private static class IndexCaps {
        final String name;
        final boolean isSearchable;
        final boolean isAggregatable;

        IndexCaps(String name, boolean isSearchable, boolean isAggregatable) {
            this.name = name;
            this.isSearchable = isSearchable;
            this.isAggregatable = isAggregatable;
        }
    }
}
