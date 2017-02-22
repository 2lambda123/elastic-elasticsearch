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

/**
 * Tests the interactions between casts and operators.
 */
public class OperatorCastTests extends ScriptTestCase {
    /**
     * Unary operator with explicit cast
     */
    public void testUnaryOperator() {
        assertEquals((byte)5, exec("long x = 5L; return (byte) (+x);"));
        assertEquals((short)5, exec("long x = 5L; return (short) (+x);"));
        assertEquals((char)5, exec("long x = 5L; return (char) (+x);"));
        assertEquals(5, exec("long x = 5L; return (int) (+x);"));
        assertEquals(5F, exec("long x = 5L; return (float) (+x);"));
        assertEquals(5L, exec("long x = 5L; return (long) (+x);"));
        assertEquals(5D, exec("long x = 5L; return (double) (+x);"));
    }

    /**
     * Binary operators with explicit cast
     */
    public void testBinaryOperator() {
        assertEquals((byte)6, exec("long x = 5L; return (byte) (x + 1);"));
        assertEquals((short)6, exec("long x = 5L; return (short) (x + 1);"));
        assertEquals((char)6, exec("long x = 5L; return (char) (x + 1);"));
        assertEquals(6, exec("long x = 5L; return (int) (x + 1);"));
        assertEquals(6F, exec("long x = 5L; return (float) (x + 1);"));
        assertEquals(6L, exec("long x = 5L; return (long) (x + 1);"));
        assertEquals(6D, exec("long x = 5L; return (double) (x + 1);"));
    }

    /**
     * Binary compound assignment with explicit cast
     */
    public void testBinaryCompoundAssignment() {
        assertEquals((byte)6, exec("long x = 5L; return (byte) (x += 1);"));
        assertEquals((short)6, exec("long x = 5L; return (short) (x += 1);"));
        assertEquals((char)6, exec("long x = 5L; return (char) (x += 1);"));
        assertEquals(6, exec("long x = 5L; return (int) (x += 1);"));
        assertEquals(6F, exec("long x = 5L; return (float) (x += 1);"));
        assertEquals(6L, exec("long x = 5L; return (long) (x += 1);"));
        assertEquals(6D, exec("long x = 5L; return (double) (x += 1);"));
    }

    /**
     * Binary compound prefix with explicit cast
     */
    public void testBinaryPrefix() {
        assertEquals((byte)6, exec("long x = 5L; return (byte) (++x);"));
        assertEquals((short)6, exec("long x = 5L; return (short) (++x);"));
        assertEquals((char)6, exec("long x = 5L; return (char) (++x);"));
        assertEquals(6, exec("long x = 5L; return (int) (++x);"));
        assertEquals(6F, exec("long x = 5L; return (float) (++x);"));
        assertEquals(6L, exec("long x = 5L; return (long) (++x);"));
        assertEquals(6D, exec("long x = 5L; return (double) (++x);"));
    }

    /**
     * Binary compound postifx with explicit cast
     */
    public void testBinaryPostfix() {
        assertEquals((byte)5, exec("long x = 5L; return (byte) (x++);"));
        assertEquals((short)5, exec("long x = 5L; return (short) (x++);"));
        assertEquals((char)5, exec("long x = 5L; return (char) (x++);"));
        assertEquals(5, exec("long x = 5L; return (int) (x++);"));
        assertEquals(5F, exec("long x = 5L; return (float) (x++);"));
        assertEquals(5L, exec("long x = 5L; return (long) (x++);"));
        assertEquals(5D, exec("long x = 5L; return (double) (x++);"));
    }

    /**
     * Shift operators with explicit cast
     */
    public void testShiftOperator() {
        assertEquals((byte)10, exec("long x = 5L; return (byte) (x << 1);"));
        assertEquals((short)10, exec("long x = 5L; return (short) (x << 1);"));
        assertEquals((char)10, exec("long x = 5L; return (char) (x << 1);"));
        assertEquals(10, exec("long x = 5L; return (int) (x << 1);"));
        assertEquals(10F, exec("long x = 5L; return (float) (x << 1);"));
        assertEquals(10L, exec("long x = 5L; return (long) (x << 1);"));
        assertEquals(10D, exec("long x = 5L; return (double) (x << 1);"));
    }

    /**
     * Shift compound assignment with explicit cast
     */
    public void testShiftCompoundAssignment() {
        assertEquals((byte)10, exec("long x = 5L; return (byte) (x <<= 1);"));
        assertEquals((short)10, exec("long x = 5L; return (short) (x <<= 1);"));
        assertEquals((char)10, exec("long x = 5L; return (char) (x <<= 1);"));
        assertEquals(10, exec("long x = 5L; return (int) (x <<= 1);"));
        assertEquals(10F, exec("long x = 5L; return (float) (x <<= 1);"));
        assertEquals(10L, exec("long x = 5L; return (long) (x <<= 1);"));
        assertEquals(10D, exec("long x = 5L; return (double) (x <<= 1);"));
    }

    /**
     * Currently these do not adopt the argument value, we issue a separate cast!
     */
    public void testArgumentsDef() {
        assertEquals(5, exec("def x = 5L; return (+(int)x);"));
        assertEquals(6, exec("def x = 5; def y = 1L; return x + (int)y"));
        assertEquals('b', exec("def x = 'abcdeg'; def y = 1L; x.charAt((int)y)"));
    }

    /**
     * Unary operators adopt the return value
     */
    public void testUnaryOperatorDef() {
        assertEquals((byte)5, exec("def x = 5L; return (byte) (+x);"));
        assertEquals((short)5, exec("def x = 5L; return (short) (+x);"));
        assertEquals((char)5, exec("def x = 5L; return (char) (+x);"));
        assertEquals(5, exec("def x = 5L; return (int) (+x);"));
        assertEquals(5F, exec("def x = 5L; return (float) (+x);"));
        assertEquals(5L, exec("def x = 5L; return (long) (+x);"));
        assertEquals(5D, exec("def x = 5L; return (double) (+x);"));
    }

    /**
     * Binary operators adopt the return value
     */
    public void testBinaryOperatorDef() {
        assertEquals((byte)6, exec("def x = 5L; return (byte) (x + 1);"));
        assertEquals((short)6, exec("def x = 5L; return (short) (x + 1);"));
        assertEquals((char)6, exec("def x = 5L; return (char) (x + 1);"));
        assertEquals(6, exec("def x = 5L; return (int) (x + 1);"));
        assertEquals(6F, exec("def x = 5L; return (float) (x + 1);"));
        assertEquals(6L, exec("def x = 5L; return (long) (x + 1);"));
        assertEquals(6D, exec("def x = 5L; return (double) (x + 1);"));
    }

    /**
     * Binary operators don't yet adopt the return value with compound assignment
     */
    public void testBinaryCompoundAssignmentDef() {
        assertEquals((byte)6, exec("def x = 5L; return (byte) (x += 1);"));
        assertEquals((short)6, exec("def x = 5L; return (short) (x += 1);"));
        assertEquals((char)6, exec("def x = 5L; return (char) (x += 1);"));
        assertEquals(6, exec("def x = 5L; return (int) (x += 1);"));
        assertEquals(6F, exec("def x = 5L; return (float) (x += 1);"));
        assertEquals(6L, exec("def x = 5L; return (long) (x += 1);"));
        assertEquals(6D, exec("def x = 5L; return (double) (x += 1);"));
    }

    /**
     * Binary operators don't yet adopt the return value with compound assignment
     */
    public void testBinaryCompoundAssignmentPrefix() {
        assertEquals((byte)6, exec("def x = 5L; return (byte) (++x);"));
        assertEquals((short)6, exec("def x = 5L; return (short) (++x);"));
        assertEquals((char)6, exec("def x = 5L; return (char) (++x);"));
        assertEquals(6, exec("def x = 5L; return (int) (++x);"));
        assertEquals(6F, exec("def x = 5L; return (float) (++x);"));
        assertEquals(6L, exec("def x = 5L; return (long) (++x);"));
        assertEquals(6D, exec("def x = 5L; return (double) (++x);"));
    }

    /**
     * Binary operators don't yet adopt the return value with compound assignment
     */
    public void testBinaryCompoundAssignmentPostfix() {
        assertEquals((byte)5, exec("def x = 5L; return (byte) (x++);"));
        assertEquals((short)5, exec("def x = 5L; return (short) (x++);"));
        assertEquals((char)5, exec("def x = 5L; return (char) (x++);"));
        assertEquals(5, exec("def x = 5L; return (int) (x++);"));
        assertEquals(5F, exec("def x = 5L; return (float) (x++);"));
        assertEquals(5L, exec("def x = 5L; return (long) (x++);"));
        assertEquals(5D, exec("def x = 5L; return (double) (x++);"));
    }

    /**
     * Shift operators adopt the return value
     */
    public void testShiftOperatorDef() {
        assertEquals((byte)10, exec("def x = 5L; return (byte) (x << 1);"));
        assertEquals((short)10, exec("def x = 5L; return (short) (x << 1);"));
        assertEquals((char)10, exec("def x = 5L; return (char) (x << 1);"));
        assertEquals(10, exec("def x = 5L; return (int) (x << 1);"));
        assertEquals(10F, exec("def x = 5L; return (float) (x << 1);"));
        assertEquals(10L, exec("def x = 5L; return (long) (x << 1);"));
        assertEquals(10D, exec("def x = 5L; return (double) (x << 1);"));
    }

    /**
     * Shift operators don't yet adopt the return value with compound assignment
     */
    public void testShiftCompoundAssignmentDef() {
        assertEquals((byte)10, exec("def x = 5L; return (byte) (x <<= 1);"));
        assertEquals((short)10, exec("def x = 5L; return (short) (x <<= 1);"));
        assertEquals((char)10, exec("def x = 5L; return (char) (x <<= 1);"));
        assertEquals(10, exec("def x = 5L; return (int) (x <<= 1);"));
        assertEquals(10F, exec("def x = 5L; return (float) (x <<= 1);"));
        assertEquals(10L, exec("def x = 5L; return (long) (x <<= 1);"));
        assertEquals(10D, exec("def x = 5L; return (double) (x <<= 1);"));
    }
}
