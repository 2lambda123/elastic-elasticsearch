/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParametrizedFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class ScriptFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "script";

    private final String runtimeType;
    private final Script script;
    private final ScriptService scriptService;

    private static ScriptFieldMapper toType(FieldMapper in) {
        return (ScriptFieldMapper) in;
    }

    protected ScriptFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        String runtimeType,
        Script script,
        ScriptService scriptService
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.runtimeType = runtimeType;
        this.script = script;
        this.scriptService = scriptService;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new ScriptFieldMapper.Builder(simpleName(), scriptService).init(this);
    }

    @Override
    protected void parseCreateField(ParseContext context) {
        // there is no lucene field
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        RuntimeKeywordMappedFieldType fieldType = (RuntimeKeywordMappedFieldType) fieldType();
        fieldType.doXContentBody(builder, includeDefaults, params);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public static class Builder extends ParametrizedFieldMapper.Builder {

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<String> runtimeType = Parameter.stringParam(
            "runtime_type",
            true,
            mapper -> toType(mapper).runtimeType,
            null
        ).setValidator(runtimeType -> {
            if (runtimeType == null) {
                throw new IllegalArgumentException("runtime_type must be specified for script field [" + name + "]");
            }
        });
        // TODO script and runtime_type can be updated: what happens to the currently running queries when they get updated?
        // do all the shards get a consistent view?
        private final Parameter<Script> script = new Parameter<>(
            "script",
            true,
            null,
            Builder::parseScript,
            mapper -> toType(mapper).script
        ).setValidator(script -> {
            if (script == null) {
                throw new IllegalArgumentException("script must be specified for script field [" + name + "]");
            }
        });

        private final ScriptService scriptService;

        protected Builder(String name, ScriptService scriptService) {
            super(name);
            this.scriptService = scriptService;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(meta, runtimeType, script);
        }

        @Override
        public ScriptFieldMapper build(BuilderContext context) {
            MappedFieldType mappedFieldType;
            if (runtimeType.getValue().equals("keyword")) {
                StringScriptFieldScript.Factory factory = scriptService.compile(script.getValue(), StringScriptFieldScript.CONTEXT);
                mappedFieldType = new RuntimeKeywordMappedFieldType(buildFullName(context), script.getValue(), factory, meta.getValue());
            } else {
                throw new IllegalArgumentException("runtime_type [" + runtimeType + "] not supported");
            }
            // TODO copy to and multi_fields should not be supported, parametrized field mapper needs to be adapted
            return new ScriptFieldMapper(
                name,
                mappedFieldType,
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                runtimeType.getValue(),
                script.getValue(),
                scriptService
            );
        }

        static Script parseScript(String name, Mapper.TypeParser.ParserContext parserContext, Object scriptObject) {
            Script script = Script.parse(scriptObject);
            if (script.getType() == ScriptType.STORED) {
                throw new IllegalArgumentException("stored scripts specified but not supported when defining script field [" + name + "]");
            }
            return script;
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        private final SetOnce<ScriptService> scriptService = new SetOnce<>();

        public void setScriptService(ScriptService scriptService) {
            this.scriptService.set(scriptService);
        }

        @Override
        public ScriptFieldMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext)
            throws MapperParsingException {

            ScriptFieldMapper.Builder builder = new ScriptFieldMapper.Builder(name, scriptService.get());
            builder.parse(name, parserContext, node);
            return builder;
        }
    }
}
