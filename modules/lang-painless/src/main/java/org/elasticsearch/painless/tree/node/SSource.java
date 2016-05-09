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
import org.elasticsearch.painless.tree.utility.Variables;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.Collections;
import java.util.List;

public class SSource extends AStatement {
    protected final List<AStatement> statements;

    public SSource(final String location, final List<AStatement> statements) {
        super(location);

        this.statements = Collections.unmodifiableList(statements);
    }

    @Override
    public void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        variables.incrementScope();

        final AStatement last = statements.get(statements.size() - 1);

        for (final AStatement statement : statements) {
            if (allEscape) {
                throw new IllegalArgumentException(error("Unreachable statement."));
            }

            statement.lastSource = statement == last;
            statement.analyze(settings, definition, variables);

            methodEscape = statement.methodEscape;
            allEscape = statement.allEscape;
        }

        variables.decrementScope();
    }

    @Override
    public void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        for (final AStatement statement : statements) {
            statement.write(settings, definition, adapter);
        }

        if (!methodEscape) {
            adapter.visitInsn(Opcodes.ACONST_NULL);
            adapter.returnValue();
        }
    }
}
