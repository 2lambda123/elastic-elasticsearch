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

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.symbol.ScriptRoot;
import org.elasticsearch.painless.lookup.def;
import org.objectweb.asm.Type;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a method call made on a def type. (Internal only.)
 */
final class PSubDefCall extends AExpression {

    private final String name;
    private final List<AExpression> arguments;

    private final StringBuilder recipe = new StringBuilder();
    private final List<String> pointers = new ArrayList<>();
    private final List<Class<?>> parameterTypes = new ArrayList<>();

    PSubDefCall(Location location, String name, List<AExpression> arguments) {
        super(location);

        this.name = Objects.requireNonNull(name);
        this.arguments = Objects.requireNonNull(arguments);
    }

    @Override
    void extractVariables(Set<String> variables) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Locals locals) {
        parameterTypes.add(Object.class);
        int totalCaptures = 0;

        for (int argument = 0; argument < arguments.size(); ++argument) {
            AExpression expression = arguments.get(argument);

            expression.internal = true;
            expression.analyze(scriptRoot, locals);

            if (expression.actual == void.class) {
                throw createError(new IllegalArgumentException("Argument(s) cannot be of [void] type when calling method [" + name + "]."));
            }

            expression.expected = expression.actual;
            arguments.set(argument, expression.cast(scriptRoot, locals));
            parameterTypes.add(expression.actual);

            if (expression instanceof ILambda) {
                ILambda lambda = (ILambda) expression;
                pointers.add(lambda.getPointer());
                // encode this parameter as a deferred reference
                char ch = (char) (argument + totalCaptures);
                recipe.append(ch);
                totalCaptures += lambda.getCaptureCount();
                parameterTypes.addAll(lambda.getCaptures());
            }
        }

        // TODO: remove ZonedDateTime exception when JodaCompatibleDateTime is removed
        actual = expected == null || expected == ZonedDateTime.class || explicit ? def.class : expected;
    }

    @Override
    void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        for (AExpression argument : arguments) {
            argument.write(classWriter, methodWriter, globals);
        }

        // create method type from return value and arguments
        Type[] asmParameterTypes = new Type[parameterTypes.size()];
        for (int index = 0; index < asmParameterTypes.length; ++index) {
            asmParameterTypes[index] = MethodWriter.getType(parameterTypes.get(index));
        }
        Type methodType = Type.getMethodType(MethodWriter.getType(actual), asmParameterTypes);

        List<Object> args = new ArrayList<>();
        args.add(recipe.toString());
        args.addAll(pointers);
        methodWriter.invokeDefCall(name, methodType, DefBootstrap.METHOD_CALL, args.toArray());
    }

    @Override
    public String toString() {
        return singleLineToStringWithOptionalArgs(arguments, prefix, name);
    }
}
