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

package org.elasticsearch.painless.tree.node;

import org.elasticsearch.painless.compiler.CompilerSettings;
import org.elasticsearch.painless.compiler.Definition;
import org.elasticsearch.painless.compiler.Definition.Cast;
import org.elasticsearch.painless.compiler.Definition.Sort;
import org.elasticsearch.painless.tree.utility.Variables;
import org.objectweb.asm.commons.GeneratorAdapter;

public class EExplicit extends AExpression {
    protected final String type;
    protected AExpression child;

    protected Cast cast = null;

    public EExplicit(final String location, final String type, final AExpression child) {
        super(location);

        this.type = type;
        this.child = child;
    }

    @Override
    protected void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        try {
            actual = definition.getType(this.type);
        } catch (final IllegalArgumentException exception) {
            throw new IllegalArgumentException(error("Not a type [" + this.type + "]."));
        }

        child.expected = actual;
        child.explicit = true;
        child.analyze(settings, definition, variables);
        child = child.cast(settings, definition, variables);
        child.typesafe |= actual.sort == Sort.DEF;
    }

    @Override
    protected void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        throw new IllegalArgumentException(error("Illegal tree structure."));
    }

    protected AExpression cast(final CompilerSettings settings, final Definition definition, final Variables variables) {
        child.expected = expected;
        child.explicit = explicit;

        return child.cast(settings, definition, variables);
    }
}
