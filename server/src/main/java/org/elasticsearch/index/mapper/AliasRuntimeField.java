/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

public class AliasRuntimeField implements RuntimeFieldType {

    public static final String CONTENT_TYPE = "alias";

    private static class Builder extends RuntimeFieldType.Builder {

        final FieldMapper.Parameter<String> path = FieldMapper.Parameter.stringParam(
            "path",
            true,
            RuntimeFieldType.initializerNotSupported(),
            null
        ).setValidator(
            s -> {
                if (s == null) {
                    throw new MapperParsingException("Missing required parameter [path]");
                }
            }
        );

        protected Builder(String name) {
            super(name);
        }

        @Override
        protected List<FieldMapper.Parameter<?>> getParameters() {
            return List.of(path);
        }

        @Override
        protected RuntimeFieldType buildFieldType() {
            return new AliasRuntimeField(name, path.get());
        }
    }

    public static final RuntimeFieldType.Parser PARSER = new RuntimeFieldType.Parser((n, c) -> new Builder(n));

    private final String name;
    private final String path;

    public AliasRuntimeField(String name, String path) {
        this.name = name;
        this.path = path;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("type", "alias");
        builder.field("path", path);
        builder.endObject();
        return builder;
    }

    @Override
    public MappedFieldType asMappedFieldType(Function<String, MappedFieldType> lookup) {
        MappedFieldType ft = lookup.apply(path);
        if (ft == null) {
            throw new IllegalStateException("Cannot resolve alias [" + name + "]: path [" + path + "] does not exist");
        }
        return ft;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String typeName() {
        return CONTENT_TYPE;
    }
}
