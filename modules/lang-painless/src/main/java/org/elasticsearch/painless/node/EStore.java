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

import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Definition.Cast;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.MethodWriter;

import java.util.Objects;
import java.util.Set;

/**
 * Represents the entirety of a variable/method chain for read/write operations.
 */
public final class EStore extends AExpression {

    AExpression lhs;
    AExpression rhs;
    final boolean pre;
    final boolean post;
    Operation operation;

    boolean cat = false;
    Type promote = null;
    Type shiftDistance; // for shifts, the RHS is promoted independently
    Cast there = null;
    Cast back = null;

    public EStore(Location location, AExpression lhs, AExpression rhs, boolean pre, boolean post, Operation operation) {
        super(location);

        this.lhs = Objects.requireNonNull(lhs);
        this.rhs = rhs;
        this.pre = pre;
        this.post = post;
        this.operation = operation;
    }

    @Override
    void extractVariables(Set<String> variables) {
        lhs.extractVariables(variables);
        rhs.extractVariables(variables);
    }

    @Override
    void analyze(Locals locals) {
        analyzeLHS(locals);
        analyzeIncrDecr();

        if (operation != null) {
            analyzeCompound(locals);
        } else if (rhs != null) {
            analyzeSimple(locals);
        } else {
            throw new IllegalStateException("Illegal tree structure.");
        }
    }

    private void analyzeLHS(Locals locals) {
        if (lhs instanceof AStoreable) {
            AStoreable storeable = (AStoreable)lhs;
            storeable.store = true;
            storeable.analyze(locals);
        } else {
            throw new IllegalArgumentException("Illegal left-hand side.");
        }
    }

    private void analyzeIncrDecr() {
        if (pre && post) {
            throw createError(new IllegalStateException("Illegal tree structure."));
        } else if (pre || post) {
            if (rhs != null) {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }

            Sort sort = lhs.actual.sort;

            if (operation == Operation.INCR) {
                if (sort == Sort.DOUBLE) {
                    rhs = new EConstant(location, 1D);
                } else if (sort == Sort.FLOAT) {
                    rhs = new EConstant(location, 1F);
                } else if (sort == Sort.LONG) {
                    rhs = new EConstant(location, 1L);
                } else {
                    rhs = new EConstant(location, 1);
                }

                operation = Operation.ADD;
            } else if (operation == Operation.DECR) {
                if (sort == Sort.DOUBLE) {
                    rhs = new EConstant(location, 1D);
                } else if (sort == Sort.FLOAT) {
                    rhs = new EConstant(location, 1F);
                } else if (sort == Sort.LONG) {
                    rhs = new EConstant(location, 1L);
                } else {
                    rhs = new EConstant(location, 1);
                }

                operation = Operation.SUB;
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }
    }

    private void analyzeCompound(Locals locals) {
        rhs.analyze(locals);

        boolean shift = false;

        if (operation == Operation.MUL) {
            promote = AnalyzerCaster.promoteNumeric(lhs.actual, rhs.actual, true);
        } else if (operation == Operation.DIV) {
            promote = AnalyzerCaster.promoteNumeric(lhs.actual, rhs.actual, true);
        } else if (operation == Operation.REM) {
            promote = AnalyzerCaster.promoteNumeric(lhs.actual, rhs.actual, true);
        } else if (operation == Operation.ADD) {
            promote = AnalyzerCaster.promoteAdd(lhs.actual, rhs.actual);
        } else if (operation == Operation.SUB) {
            promote = AnalyzerCaster.promoteNumeric(lhs.actual, rhs.actual, true);
        } else if (operation == Operation.LSH) {
            promote = AnalyzerCaster.promoteNumeric(lhs.actual, false);
            shiftDistance = AnalyzerCaster.promoteNumeric(rhs.actual, false);
            shift = true;
        } else if (operation == Operation.RSH) {
            promote = AnalyzerCaster.promoteNumeric(lhs.actual, false);
            shiftDistance = AnalyzerCaster.promoteNumeric(rhs.actual, false);
            shift = true;
        } else if (operation == Operation.USH) {
            promote = AnalyzerCaster.promoteNumeric(lhs.actual, false);
            shiftDistance = AnalyzerCaster.promoteNumeric(rhs.actual, false);
            shift = true;
        } else if (operation == Operation.BWAND) {
            promote = AnalyzerCaster.promoteXor(lhs.actual, rhs.actual);
        } else if (operation == Operation.XOR) {
            promote = AnalyzerCaster.promoteXor(lhs.actual, rhs.actual);
        } else if (operation == Operation.BWOR) {
            promote = AnalyzerCaster.promoteXor(lhs.actual, rhs.actual);
        } else {
            throw createError(new IllegalStateException("Illegal tree structure."));
        }

        if (promote == null || (shift && shiftDistance == null)) {
            throw createError(new ClassCastException("Cannot apply compound assignment " +
                "[" + operation.symbol + "=] to types [" + lhs.actual + "] and [" + rhs.actual + "]."));
        }

        cat = operation == Operation.ADD && promote.sort == Sort.STRING;

        if (cat) {
            if (rhs instanceof EBinary && ((EBinary)rhs).operation == Operation.ADD &&
                rhs.actual.sort == Sort.STRING) {
                ((EBinary)rhs).cat = true;
            }

            rhs.expected = rhs.actual;
        } else if (shift) {
            if (promote.sort == Sort.DEF) {
                // shifts are promoted independently, but for the def type, we need object.
                rhs.expected = promote;
            } else if (shiftDistance.sort == Sort.LONG) {
                rhs.expected = Definition.INT_TYPE;
                rhs.explicit = true;
            } else {
                rhs.expected = shiftDistance;
            }
        } else {
            rhs.expected = promote;
        }

        rhs = rhs.cast(locals);

        there = AnalyzerCaster.getLegalCast(location, lhs.actual, promote, false, false);
        back = AnalyzerCaster.getLegalCast(location, promote, lhs.actual, true, false);

        this.statement = true;
        this.actual = read ? lhs.actual : Definition.VOID_TYPE;
    }

    private void analyzeSimple(Locals locals) {
        // If the lhs node is a def node we update the actual type to remove the need for a cast.
        if (((AStoreable)lhs).updateActual(rhs.actual)) {
            rhs.analyze(locals);
            rhs.expected = rhs.actual;
        // Otherwise, we must adapt the rhs type to the lhs type with a cast.
        } else {
            rhs.expected = lhs.actual;
            rhs.analyze(locals);
        }

        rhs = rhs.cast(locals);

        this.statement = true;
        this.actual = read ? lhs.actual : Definition.VOID_TYPE;
    }

    /**
     * Handles writing byte code for variable/method chains for all given possibilities
     * including String concatenation, compound assignment, regular assignment, and simple
     * reads.  Includes proper duplication for chained assignments and assignments that are
     * also read from.
     */
    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        // For the case where the assignment represents a String concatenation
        // we must, depending on the Java version, write a StringBuilder or
        // track types going onto the stack.  This must be done before the
        // lhs is read because we need the StringBuilder to be placed on the
        // stack ahead of any potential concatenation arguments.
        int catElementStackSize = 0;

        if (cat) {
            catElementStackSize = writer.writeNewStrings();
        }

        // Load any prefixes for the storable lhs node.  The lhs node will not write the final link in the chain during a store.
        lhs.write(writer, globals);

        // Cast the lhs to a storeable to perform the necessary operations to store the rhs.
        AStoreable lhs = (AStoreable)this.lhs;
        lhs.prestore(writer, globals); // call the prestore method on the lhs to prepare for a load/store operation

        if (cat) {
            // Handle the case where we are doing a compound assignment
            // representing a String concatenation.

            writer.writeDup(lhs.size(), catElementStackSize);  // dup the top element and insert it before concat helper on stack
            lhs.load(writer, globals);                         // read the current lhs's value
            writer.writeAppendStrings(lhs.actual);             // append the lhs's value using the StringBuilder

            rhs.write(writer, globals); // write the bytecode for the rhs

            if (!(rhs instanceof EBinary) ||
                ((EBinary)rhs).operation != Operation.ADD || rhs.actual.sort != Sort.STRING) {
                writer.writeAppendStrings(rhs.actual); // append the rhs's value unless it's also a concatenation
            }

            writer.writeToStrings(); // put the value for string concat onto the stack
            writer.writeCast(back);  // if necessary, cast the String to the lhs actual type

            if (lhs.read) {
                writer.writeDup(lhs.actual.sort.size, lhs.size()); // if this lhs is also read
                                                                               // from dup the value onto the stack
            }

            lhs.store(writer, globals); // store the lhs's value from the stack in its respective variable/field/array
        } else if (operation != null) {
            // Handle the case where we are doing a compound assignment that
            // does not represent a String concatenation.

            writer.writeDup(lhs.size(), 0); // if necessary, dup the previous lhs's value to be both loaded from and stored to
            lhs.load(writer, globals);      // load the current lhs's value

            if (lhs.read && post) {
                writer.writeDup(lhs.actual.sort.size, lhs.size()); // dup the value if the lhs is also
                                                                               // read from and is a post increment
            }

            writer.writeCast(there);    // if necessary cast the current lhs's value
                                        // to the promotion type between the lhs and rhs types
            rhs.write(writer, globals); // write the bytecode for the rhs

        // XXX: fix these types, but first we need def compound assignment tests.
        // its tricky here as there are possibly explicit casts, too.
        // write the operation instruction for compound assignment
            if (promote.sort == Sort.DEF) {
                writer.writeDynamicBinaryInstruction(location, promote,
                    Definition.DEF_TYPE, Definition.DEF_TYPE, operation, DefBootstrap.OPERATOR_COMPOUND_ASSIGNMENT);
            } else {
                writer.writeBinaryInstruction(location, promote, operation);
            }

            writer.writeCast(back); // if necessary cast the promotion type value back to the lhs's type

            if (lhs.read && !post) {
                writer.writeDup(lhs.actual.sort.size, lhs.size()); // dup the value if the lhs is also
                                                                   // read from and is not a post increment
            }

            lhs.store(writer, globals); // store the lhs's value from the stack in its respective variable/field/array
        } else {
            // Handle the case for a simple write.

            rhs.write(writer, globals); // write the bytecode for the rhs rhs

            if (lhs.read) {
                writer.writeDup(lhs.actual.sort.size, lhs.size()); // dup the value if the lhs is also read from
            }

            lhs.store(writer, globals); // store the lhs's value from the stack in its respective variable/field/array
        }

        writer.writeBranch(tru, fals); // if this is a branch node, write the bytecode to make an appropiate jump
    }
}
