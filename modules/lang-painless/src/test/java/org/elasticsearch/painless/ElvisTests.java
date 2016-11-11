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

package org.elasticsearch.painless;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * Tests for the Elvis operator ({@code ?:}).
 */
public class ElvisTests extends ScriptTestCase {
    public void testBasics() {
        // Basics
        assertEquals("str", exec("return params.a ?: 'str'"));
        assertEquals("str", exec("return params.a ?: 'str2'", singletonMap("a", "str"), true));
        assertEquals("str", exec("return params.a ?: 'asdf'", singletonMap("a", "str"), true));

        // Assigning to a primitive
        assertEquals(1, exec("int i = params.a ?: 1; return i"));
        assertEquals(1, exec("int i = params.a ?: 2; return i", singletonMap("a", 1), true));

        // Explicit casting
        assertEquals(1, exec("return (Integer)(params.a ?: Integer.valueOf(1))"));
        assertEquals(1, exec("return (Integer)(params.a ?: Integer.valueOf(2))", singletonMap("a", 1), true));
        assertEquals(1, exec("return (int)(params.a ?: 1)"));
        assertEquals(1, exec("return (int)(params.a ?: 2)", singletonMap("a", 1), true));

        // Now some chains
        assertEquals(1, exec("return params.a ?: params.a ?: 1"));
        assertEquals(1, exec("return params.a ?: params.b ?: 'j'", singletonMap("b", 1), true));
        assertEquals(1, exec("return params.a ?: params.b ?: 'j'", singletonMap("a", 1), true));

        // Precedence
        assertEquals(1, exec("return params.a ?: 2 + 2", singletonMap("a", 1), true));
        assertEquals(4, exec("return params.a ?: 2 + 2"));
        assertEquals(2, exec("return params.a + 1 ?: 2 + 2", singletonMap("a", 1), true)); // Yes, this is silly, but it should be valid

        // Weird casts
        assertEquals(1,     exec("int i = params.i;     String s = params.s; return s ?: i", singletonMap("i", 1), true));
        assertEquals("str", exec("Integer i = params.i; String s = params.s; return s ?: i", singletonMap("s", "str"), true));

        // Combining
        assertEquals(2, exec("return (params.a ?: 0) + 1", singletonMap("a", 1), true));
        assertEquals(1, exec("return (params.a ?: 0) + 1"));
        assertEquals(2, exec("return (params.a ?: ['b': 10]).b + 1", singletonMap("a", singletonMap("b", 1)), true));
        assertEquals(11, exec("return (params.a ?: ['b': 10]).b + 1"));
    }

    public void testWithNullSafeDereferences() {
        assertEquals(1, exec("return params.a?.b ?: 1"));
        assertEquals(1, exec("return params.a?.b ?: 2", singletonMap("a", singletonMap("b", 1)), true));

        // TODO we can save an extra box/unbox when the null safe dereference would output a primitive and the rhs is also a primitive
    }

    public void testLazy() {
        assertEquals(1, exec("def fail() {throw new RuntimeException('test')} return params.a ?: fail()", singletonMap("a", 1), true));
        Exception e = expectScriptThrows(RuntimeException.class, () ->
            exec("def fail() {throw new RuntimeException('test')} return params.a ?: fail()"));
        assertEquals(e.getMessage(), "test");
    }

    /**
     * Checks that {@code a ?: b ?: c} is be parsed as {@code a ?: (b ?: c)} instead of {@code (a ?: b) ?: c} which is nice because the
     * first one only needs one comparison if the {@code a} is non-null while the second one needs two.
     */
    public void testRightAssociative() {
        checkOneBranch("params.a ?: (params.b ?: params.c)", true);
        checkOneBranch("(params.a ?: params.b) ?: params.c", false);
        checkOneBranch("params.a ?: params.b ?: params.c", true);
    }

    private void checkOneBranch(String code, boolean expectOneBranch) {
        /* Sadly this is a super finicky about the output of the disassembly but I think it is worth having because it makes sure that
         * the code generated for the elvis operator is as efficient as possible. */
        String disassembled = Debugger.toString(code);
        int firstLookup = disassembled.indexOf("INVOKEINTERFACE java/util/Map.get (Ljava/lang/Object;)Ljava/lang/Object;");
        assertThat(disassembled, firstLookup, greaterThan(-1));
        int firstElvisDestinationLabelIndex = disassembled.indexOf("IFNONNULL L", firstLookup);
        assertThat(disassembled, firstElvisDestinationLabelIndex, greaterThan(-1));
        String firstElvisDestinationLabel = disassembled.substring(firstElvisDestinationLabelIndex + "IFNONNULL ".length(),
                disassembled.indexOf('\n', firstElvisDestinationLabelIndex));
        int firstElvisDestionation = disassembled.indexOf("   " + firstElvisDestinationLabel);
        assertThat(disassembled, firstElvisDestionation, greaterThan(-1));
        int ifAfterFirstElvisDestination = disassembled.indexOf("IF", firstElvisDestionation);
        if (expectOneBranch) {
            assertThat(disassembled, ifAfterFirstElvisDestination, lessThan(0));
        } else {
            assertThat(disassembled, ifAfterFirstElvisDestination, greaterThan(-1));
        }
        int returnAfterFirstElvisDestination = disassembled.indexOf("RETURN", firstElvisDestionation);
        assertThat(disassembled, returnAfterFirstElvisDestination, greaterThan(-1));
    }

    public void testExtraneous() {
        Exception e = expectScriptThrows(IllegalArgumentException.class, () -> exec("int i = params.a; return i ?: 1"));
        assertEquals("Extraneous elvis operator. LHS is a primitive.", e.getMessage());
        expectScriptThrows(IllegalArgumentException.class, () -> exec("int i = params.a; return i + 10 ?: 'ignored'"));
        assertEquals("Extraneous elvis operator. LHS is a primitive.", e.getMessage());
        e = expectScriptThrows(IllegalArgumentException.class, () -> exec("return 'cat' ?: 1"));
        assertEquals("Extraneous elvis operator. LHS is a constant.", e.getMessage());
        e = expectScriptThrows(IllegalArgumentException.class, () -> exec("return null ?: 'j'"));
        assertEquals("Extraneous elvis operator. LHS is null.", e.getMessage());
        e = expectScriptThrows(IllegalArgumentException.class, () -> exec("return params.a ?: null ?: 'j'"));
        assertEquals("Extraneous elvis operator. LHS is null.", e.getMessage());
        e = expectScriptThrows(IllegalArgumentException.class, () -> exec("return params.a ?: null"));
        assertEquals("Extraneous elvis operator. RHS is null.", e.getMessage());
    }
}
