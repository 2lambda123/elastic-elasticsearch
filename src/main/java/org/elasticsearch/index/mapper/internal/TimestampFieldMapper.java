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

package org.elasticsearch.index.mapper.internal;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.Version;
import org.elasticsearch.action.TimestampParsingException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.NumericDateAnalyzer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.RootMapper;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.MapperBuilders.timestamp;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseDateTimeFormatter;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

public class TimestampFieldMapper extends DateFieldMapper implements RootMapper {

    public static final String NAME = "_timestamp";
    public static final String CONTENT_TYPE = "_timestamp";
    public static final String DEFAULT_DATE_TIME_FORMAT = "epoch_millis||dateOptionalTime";

    public static class Defaults extends DateFieldMapper.Defaults {
        public static final String NAME = "_timestamp";

        // TODO: this should be removed
        public static final MappedFieldType PRE_20_FIELD_TYPE;
        public static final FormatDateTimeFormatter DATE_TIME_FORMATTER = Joda.forPattern(DEFAULT_DATE_TIME_FORMAT);
        public static final DateFieldType FIELD_TYPE = new TimestampFieldType();

        static {
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setNumericPrecisionStep(Defaults.PRECISION_STEP_64_BIT);
            FIELD_TYPE.setNames(new MappedFieldType.Names(NAME));
            FIELD_TYPE.setDateTimeFormatter(DATE_TIME_FORMATTER);
            FIELD_TYPE.setIndexAnalyzer(NumericDateAnalyzer.buildNamedAnalyzer(DATE_TIME_FORMATTER, Defaults.PRECISION_STEP_64_BIT));
            FIELD_TYPE.setSearchAnalyzer(NumericDateAnalyzer.buildNamedAnalyzer(DATE_TIME_FORMATTER, Integer.MAX_VALUE));
            FIELD_TYPE.freeze();
            PRE_20_FIELD_TYPE = FIELD_TYPE.clone();
            PRE_20_FIELD_TYPE.setStored(false);
            PRE_20_FIELD_TYPE.freeze();
        }

        public static final EnabledAttributeMapper ENABLED = EnabledAttributeMapper.UNSET_DISABLED;
        public static final String PATH = null;
        public static final String DEFAULT_TIMESTAMP = "now";
    }

    public static class Builder extends NumberFieldMapper.Builder<Builder, TimestampFieldMapper> {

        private EnabledAttributeMapper enabledState = EnabledAttributeMapper.UNSET_DISABLED;
        private String path = Defaults.PATH;
        private String defaultTimestamp = Defaults.DEFAULT_TIMESTAMP;
        private boolean explicitStore = false;
        private Boolean ignoreMissing = null;

        public Builder() {
            super(Defaults.NAME, Defaults.FIELD_TYPE, Defaults.PRECISION_STEP_64_BIT);
        }

        DateFieldType fieldType() {
            return (DateFieldType)fieldType;
        }

        public Builder enabled(EnabledAttributeMapper enabledState) {
            this.enabledState = enabledState;
            return builder;
        }

        public Builder path(String path) {
            this.path = path;
            return builder;
        }

        public Builder dateTimeFormatter(FormatDateTimeFormatter dateTimeFormatter) {
            fieldType().setDateTimeFormatter(dateTimeFormatter);
            return this;
        }

        public Builder defaultTimestamp(String defaultTimestamp) {
            this.defaultTimestamp = defaultTimestamp;
            return builder;
        }

        public Builder ignoreMissing(boolean ignoreMissing) {
            this.ignoreMissing = ignoreMissing;
            return builder;
        }

        @Override
        public Builder store(boolean store) {
            explicitStore = true;
            return super.store(store);
        }

        @Override
        public TimestampFieldMapper build(BuilderContext context) {
            if (explicitStore == false && context.indexCreatedVersion().before(Version.V_2_0_0)) {
                assert fieldType.stored();
                fieldType.setStored(false);
            }
            setupFieldType(context);
            return new TimestampFieldMapper(fieldType, docValues, enabledState, path, defaultTimestamp,
                    ignoreMissing,
                    ignoreMalformed(context), coerce(context), fieldDataSettings, context.indexSettings());
        }

        @Override
        protected NamedAnalyzer makeNumberAnalyzer(int precisionStep) {
            return NumericDateAnalyzer.buildNamedAnalyzer(fieldType().dateTimeFormatter(), precisionStep);
        }

        @Override
        protected int maxPrecisionStep() {
            return 64;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            TimestampFieldMapper.Builder builder = timestamp();
            parseField(builder, builder.name, node, parserContext);
            boolean defaultSet = false;
            Boolean ignoreMissing = null;
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    EnabledAttributeMapper enabledState = nodeBooleanValue(fieldNode) ? EnabledAttributeMapper.ENABLED : EnabledAttributeMapper.DISABLED;
                    builder.enabled(enabledState);
                    iterator.remove();
                } else if (fieldName.equals("path")) {
                    builder.path(fieldNode.toString());
                    iterator.remove();
                } else if (fieldName.equals("format")) {
                    builder.dateTimeFormatter(parseDateTimeFormatter(fieldNode.toString()));
                    iterator.remove();
                } else if (fieldName.equals("default")) {
                    if (fieldNode == null) {
                        if (parserContext.indexVersionCreated().onOrAfter(Version.V_1_4_0_Beta1) &&
                                parserContext.indexVersionCreated().before(Version.V_1_5_0)) {
                            // We are reading an index created in 1.4 with feature #7036
                            // `default: null` was explicitly set. We need to change this index to
                            // `ignore_missing: false`
                            builder.ignoreMissing(false);
                        } else {
                            throw new TimestampParsingException("default timestamp can not be set to null");
                        }
                    } else {
                        builder.defaultTimestamp(fieldNode.toString());
                        defaultSet = true;
                    }
                    iterator.remove();
                } else if (fieldName.equals("ignore_missing")) {
                    ignoreMissing = nodeBooleanValue(fieldNode);
                    builder.ignoreMissing(ignoreMissing);
                    iterator.remove();
                }
            }

            // We can not accept a default value and rejecting null values at the same time
            if (defaultSet && (ignoreMissing != null && ignoreMissing == false)) {
                throw new TimestampParsingException("default timestamp can not be set with ignore_missing set to false");
            }

            return builder;
        }
    }

    static final class TimestampFieldType extends DateFieldType {

        public TimestampFieldType() {}

        protected TimestampFieldType(TimestampFieldType ref) {
            super(ref);
        }

        @Override
        public DateFieldType clone() {
            return new TimestampFieldType(this);
        }

        /**
         * Override the default behavior to return a timestamp
         */
        @Override
        public Object valueForSearch(Object value) {
            return value(value);
        }
    }

    private static MappedFieldType defaultFieldType(Settings settings) {
        return Version.indexCreated(settings).onOrAfter(Version.V_2_0_0) ? Defaults.FIELD_TYPE : Defaults.PRE_20_FIELD_TYPE;
    }

    private EnabledAttributeMapper enabledState;

    private final String path;
    private final String defaultTimestamp;
    private final MappedFieldType defaultFieldType;
    private final Boolean ignoreMissing;

    public TimestampFieldMapper(Settings indexSettings) {
        this(defaultFieldType(indexSettings).clone(), null, Defaults.ENABLED, Defaults.PATH, Defaults.DEFAULT_TIMESTAMP,
             null, Defaults.IGNORE_MALFORMED, Defaults.COERCE, null, indexSettings);
    }

    protected TimestampFieldMapper(MappedFieldType fieldType, Boolean docValues, EnabledAttributeMapper enabledState, String path,
                                   String defaultTimestamp, Boolean ignoreMissing, Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                                   @Nullable Settings fieldDataSettings, Settings indexSettings) {
        super(fieldType, docValues, Defaults.NULL_VALUE, ignoreMalformed, coerce, fieldDataSettings,
                indexSettings, MultiFields.empty(), null);
        this.enabledState = enabledState;
        this.path = path;
        this.defaultTimestamp = defaultTimestamp;
        this.defaultFieldType = defaultFieldType(indexSettings);
        this.ignoreMissing = ignoreMissing;
    }

    @Override
    public MappedFieldType defaultFieldType() {
        return defaultFieldType;
    }

    @Override
    public boolean defaultDocValues() {
        return false;
    }

    public boolean enabled() {
        return this.enabledState.enabled;
    }

    public String path() {
        return this.path;
    }

    public String defaultTimestamp() {
        return this.defaultTimestamp;
    }

    public Boolean ignoreMissing() {
        return this.ignoreMissing;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        // nothing to do here, we call the parent in preParse
        return null;
    }

    @Override
    protected void innerParseCreateField(ParseContext context, List<Field> fields) throws IOException {
        if (enabledState.enabled) {
            long timestamp = context.sourceToParse().timestamp();
            if (fieldType.indexOptions() == IndexOptions.NONE && !fieldType.stored() && !fieldType().hasDocValues()) {
                context.ignoredValue(fieldType.names().indexName(), String.valueOf(timestamp));
            }
            if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
                fields.add(new LongFieldMapper.CustomLongNumericField(this, timestamp, (NumberFieldType)fieldType));
            }
            if (fieldType().hasDocValues()) {
                fields.add(new NumericDocValuesField(fieldType.names().indexName(), timestamp));
            }
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);
        boolean indexed = fieldType.indexOptions() != IndexOptions.NONE;
        boolean indexedDefault = Defaults.FIELD_TYPE.indexOptions() != IndexOptions.NONE;

        // if all are defaults, no sense to write it at all
        if (!includeDefaults && indexed == indexedDefault && customFieldDataSettings == null &&
            fieldType.stored() == Defaults.FIELD_TYPE.stored() && enabledState == Defaults.ENABLED && path == Defaults.PATH
                && fieldType().dateTimeFormatter().format().equals(Defaults.DATE_TIME_FORMATTER.format())
                && Defaults.DEFAULT_TIMESTAMP.equals(defaultTimestamp)
                && defaultDocValues() == fieldType().hasDocValues()) {
            return builder;
        }
        builder.startObject(CONTENT_TYPE);
        if (includeDefaults || enabledState != Defaults.ENABLED) {
            builder.field("enabled", enabledState.enabled);
        }
        if (includeDefaults || (indexed != indexedDefault) || (fieldType.tokenized() != Defaults.FIELD_TYPE.tokenized())) {
            builder.field("index", indexTokenizeOptionToString(indexed, fieldType.tokenized()));
        }
        if (includeDefaults || fieldType.stored() != Defaults.FIELD_TYPE.stored()) {
            builder.field("store", fieldType.stored());
        }
        doXContentDocValues(builder, includeDefaults);
        if (includeDefaults || path != Defaults.PATH) {
            builder.field("path", path);
        }
        if (includeDefaults || !fieldType().dateTimeFormatter().format().equals(Defaults.DATE_TIME_FORMATTER.format())) {
            builder.field("format", fieldType().dateTimeFormatter().format());
        }
        if (includeDefaults || !Defaults.DEFAULT_TIMESTAMP.equals(defaultTimestamp)) {
            builder.field("default", defaultTimestamp);
        }
        if (includeDefaults || ignoreMissing != null) {
            builder.field("ignore_missing", ignoreMissing);
        }
        if (customFieldDataSettings != null) {
            builder.field("fielddata", (Map) customFieldDataSettings.getAsMap());
        } else if (includeDefaults) {
            builder.field("fielddata", (Map) fieldType.fieldDataType().getSettings().getAsMap());
        }

        builder.endObject();
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        TimestampFieldMapper timestampFieldMapperMergeWith = (TimestampFieldMapper) mergeWith;
        super.merge(mergeWith, mergeResult);
        if (!mergeResult.simulate()) {
            if (timestampFieldMapperMergeWith.enabledState != enabledState && !timestampFieldMapperMergeWith.enabledState.unset()) {
                this.enabledState = timestampFieldMapperMergeWith.enabledState;
            }
        } else {
            if (timestampFieldMapperMergeWith.defaultTimestamp() == null && defaultTimestamp == null) {
                return;
            }
            if (defaultTimestamp == null) {
                mergeResult.addConflict("Cannot update default in _timestamp value. Value is null now encountering " + timestampFieldMapperMergeWith.defaultTimestamp());
            } else if (timestampFieldMapperMergeWith.defaultTimestamp() == null) {
                mergeResult.addConflict("Cannot update default in _timestamp value. Value is \" + defaultTimestamp.toString() + \" now encountering null");
            } else if (!timestampFieldMapperMergeWith.defaultTimestamp().equals(defaultTimestamp)) {
                mergeResult.addConflict("Cannot update default in _timestamp value. Value is " + defaultTimestamp.toString() + " now encountering " + timestampFieldMapperMergeWith.defaultTimestamp());
            }
            if (this.path != null) {
                if (path.equals(timestampFieldMapperMergeWith.path()) == false) {
                    mergeResult.addConflict("Cannot update path in _timestamp value. Value is " + path + " path in merged mapping is " + (timestampFieldMapperMergeWith.path() == null ? "missing" : timestampFieldMapperMergeWith.path()));
                }
            } else if (timestampFieldMapperMergeWith.path() != null) {
                mergeResult.addConflict("Cannot update path in _timestamp value. Value is " + path + " path in merged mapping is missing");
            }
        }
    }
}
