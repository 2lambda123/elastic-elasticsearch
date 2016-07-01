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

package org.elasticsearch.index.mapper.size;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.uninverting.UninvertingReader;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.internal.EnabledAttributeMapper;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

final class SizeFieldMapper extends AbstractSizeFieldMapper {
    public static class Defaults {
        public static final SizeFieldType FIELD_TYPE = new SizeFieldType();

        static {
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setName(AbstractSizeFieldMapper.NAME);
            FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends AbstractSizeFieldMapper.Builder {
        Builder(MappedFieldType fieldType) {
            super(fieldType == null ? Defaults.FIELD_TYPE : fieldType, Defaults.FIELD_TYPE);
            builder = this;
        }

        public Builder enabled(EnabledAttributeMapper enabled) {
            this.enabledState = enabled;
            return this;
        }

        @Override
        public SizeFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new SizeFieldMapper(enabledState, fieldType, context.indexSettings());
        }

        @Override
        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);
            if (context.indexCreatedVersion().onOrAfter(Version.V_5_0_0_alpha4)) {
                fieldType.setHasDocValues(true);
                ((SizeFieldType) fieldType).setFielddata(false);
            } else {
                // enables fielddata loading, doc values was disabled on _size until V_5_0_0_alpha4
                fieldType.setHasDocValues(false);
                ((SizeFieldType) fieldType).setFielddata(true);
            }
        }
    }

    SizeFieldMapper(Settings indexSettings, MappedFieldType fieldType) {
        this(AbstractSizeFieldMapper.Defaults.ENABLED_STATE,
            fieldType == null ? Defaults.FIELD_TYPE.clone() : fieldType.clone(), indexSettings);
    }

    SizeFieldMapper(EnabledAttributeMapper enabled, MappedFieldType fieldType, Settings indexSettings) {
        super(enabled, fieldType, Defaults.FIELD_TYPE.clone(), indexSettings);
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        if (enabled() == false) {
            return ;
        }
        boolean indexed = fieldType().indexOptions() != IndexOptions.NONE;
        boolean docValued = fieldType().hasDocValues();
        boolean stored = fieldType().stored();
        final int value = context.sourceToParse().source().length();
        fields.addAll(NumberFieldMapper.NumberType.INTEGER.createFields(name(), value, indexed, docValued, stored));
    }

    static class SizeFieldType extends NumberFieldMapper.NumberFieldType {
        private boolean fielddata;

        public SizeFieldType() {
            super(NumberFieldMapper.NumberType.INTEGER);
        }

        protected SizeFieldType(SizeFieldType ref) {
            super(ref);
            this.fielddata = ref.fielddata;
        }

        @Override
        public SizeFieldType clone() {
            return new SizeFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            if (super.equals(o) == false) {
                return false;
            }
            SizeFieldType that = (SizeFieldType) o;
            return fielddata == that.fielddata;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), fielddata);
        }

        @Override
        public String typeName() {
            return AbstractSizeFieldMapper.CONTENT_TYPE;
        }

        public boolean fielddata() {
            return fielddata;
        }

        public void setFielddata(boolean fielddata) {
            checkIfFrozen();
            this.fielddata = fielddata;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder() {
            if (hasDocValues()) {
                return super.fielddataBuilder();
            }
            if (fielddata) {
                failIfNotIndexed();
                return new LongUninvertingIndexFieldData.Builder(UninvertingReader.Type.INTEGER_POINT);
            }
            return super.fielddataBuilder();
        }

        @Override
        public void checkCompatibility(MappedFieldType other,
                                       List<String> conflicts, boolean strict) {
            super.checkCompatibility(other, conflicts, strict);
            SizeFieldType otherType = (SizeFieldType) other;
            if (strict) {
                if (fielddata() != otherType.fielddata()) {
                    conflicts.add("mapper [" + name() + "] is used by multiple types. Set update_all_types to true to update [fielddata] "
                        + "across all types.");
                }
            }
        }
    }
}
