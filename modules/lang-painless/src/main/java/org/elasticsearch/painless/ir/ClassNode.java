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

package org.elasticsearch.painless.ir;

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.Constant;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.ScriptClassInfo;
import org.elasticsearch.painless.symbol.ScriptRoot;
import org.elasticsearch.painless.WriterConstants;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.util.Printer;

import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.painless.WriterConstants.BASE_INTERFACE_TYPE;
import static org.elasticsearch.painless.WriterConstants.BITSET_TYPE;
import static org.elasticsearch.painless.WriterConstants.BOOTSTRAP_METHOD_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.CLASS_TYPE;
import static org.elasticsearch.painless.WriterConstants.COLLECTIONS_TYPE;
import static org.elasticsearch.painless.WriterConstants.CONVERT_TO_SCRIPT_EXCEPTION_METHOD;
import static org.elasticsearch.painless.WriterConstants.DEFINITION_TYPE;
import static org.elasticsearch.painless.WriterConstants.DEF_BOOTSTRAP_DELEGATE_METHOD;
import static org.elasticsearch.painless.WriterConstants.DEF_BOOTSTRAP_DELEGATE_TYPE;
import static org.elasticsearch.painless.WriterConstants.DEF_BOOTSTRAP_METHOD;
import static org.elasticsearch.painless.WriterConstants.EMPTY_MAP_METHOD;
import static org.elasticsearch.painless.WriterConstants.EXCEPTION_TYPE;
import static org.elasticsearch.painless.WriterConstants.FUNCTION_TABLE_TYPE;
import static org.elasticsearch.painless.WriterConstants.GET_NAME_METHOD;
import static org.elasticsearch.painless.WriterConstants.GET_SOURCE_METHOD;
import static org.elasticsearch.painless.WriterConstants.GET_STATEMENTS_METHOD;
import static org.elasticsearch.painless.WriterConstants.OUT_OF_MEMORY_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.PAINLESS_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.PAINLESS_EXPLAIN_ERROR_GET_HEADERS_METHOD;
import static org.elasticsearch.painless.WriterConstants.PAINLESS_EXPLAIN_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.STACK_OVERFLOW_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.STRING_TYPE;

public class ClassNode extends IRNode {

    /* ---- begin tree structure ---- */

    protected final List<FieldNode> fieldNodes = new ArrayList<>();
    protected final List<FunctionNode> functionNodes = new ArrayList<>();
    protected final List<StatementNode> statementNodes = new ArrayList<>();

    public ClassNode addFieldNode(FieldNode fieldNode) {
        fieldNodes.add(fieldNode);
        return this;
    }

    public ClassNode setFieldNode(int index, FieldNode fieldNode) {
        fieldNodes.set(index, fieldNode);
        return this;
    }

    public FieldNode getFieldNode(int index) {
        return fieldNodes.get(index);
    }

    public ClassNode removeFieldNode(FieldNode fieldNode) {
        fieldNodes.remove(fieldNode);
        return this;
    }

    public ClassNode removeFieldNode(int index) {
        fieldNodes.remove(index);
        return this;
    }

    public int getFieldsSize() {
        return fieldNodes.size();
    }

    public List<FieldNode> getFieldsNodes() {
        return fieldNodes;
    }

    public ClassNode clearFieldNodes() {
        fieldNodes.clear();
        return this;
    }
    
    public ClassNode addFunctionNode(FunctionNode functionNode) {
        functionNodes.add(functionNode);
        return this;
    }

    public ClassNode setFunctionNode(int index, FunctionNode functionNode) {
        functionNodes.set(index, functionNode);
        return this;
    }

    public FunctionNode getFunctionNode(int index) {
        return functionNodes.get(index);
    }

    public ClassNode removeFunctionNode(FunctionNode functionNode) {
        functionNodes.remove(functionNode);
        return this;
    }

    public ClassNode removeFunctionNode(int index) {
        functionNodes.remove(index);
        return this;
    }

    public int getFunctionsSize() {
        return functionNodes.size();
    }

    public List<FunctionNode> getFunctionsNodes() {
        return functionNodes;
    }

    public ClassNode clearFunctionNodes() {
        functionNodes.clear();
        return this;
    }

    public ClassNode addStatementNode(StatementNode statementNode) {
        statementNodes.add(statementNode);
        return this;
    }

    public ClassNode setStatementNode(int index, StatementNode statementNode) {
        statementNodes.set(index, statementNode);
        return this;
    }

    public StatementNode getStatementNode(int index) {
        return statementNodes.get(index);
    }

    public ClassNode removeStatementNode(StatementNode statementNode) {
        statementNodes.remove(statementNode);
        return this;
    }

    public ClassNode removeStatementNode(int index) {
        statementNodes.remove(index);
        return this;
    }

    public int getStatementsSize() {
        return statementNodes.size();
    }

    public List<StatementNode> getStatementsNodes() {
        return statementNodes;
    }

    public ClassNode clearStatementNodes() {
        statementNodes.clear();
        return this;
    }
    
    /* ---- end tree structure, begin node data ---- */

    protected ScriptClassInfo scriptClassInfo;
    protected String name;
    protected String sourceText;
    protected Printer debugStream;
    protected ScriptRoot scriptRoot;
    protected Locals mainMethod;
    protected boolean doesMethodEscape;
    protected final Set<String> extractedVariables = new HashSet<>();
    protected final List<org.objectweb.asm.commons.Method> getMethods = new ArrayList<>();

    public ClassNode setScriptClassInfo(ScriptClassInfo scriptClassInfo) {
        this.scriptClassInfo = scriptClassInfo;
        return this;
    }

    public ScriptClassInfo getScriptClassInfo() {
        return scriptClassInfo;
    }

    public ClassNode setName(String name) {
        this.name = name;
        return this;
    }

    public String getName() {
        return name;
    }

    public ClassNode setSourceText(String sourceText) {
        this.sourceText = sourceText;
        return this;
    }

    public String getSourceText() {
        return sourceText;
    }

    public ClassNode setDebugStream(Printer debugStream) {
        this.debugStream = debugStream;
        return this;
    }

    public Printer getDebugStream() {
        return debugStream;
    }

    public ClassNode setScriptRoot(ScriptRoot scriptRoot) {
        this.scriptRoot = scriptRoot;
        return this;
    }

    public ScriptRoot getScriptRoot() {
        return scriptRoot;
    }

    public ClassNode setMainMethod(Locals mainMethod) {
        this.mainMethod = mainMethod;
        return this;
    }

    public Locals getMainMethod() {
        return mainMethod;
    }

    public ClassNode setMethodEscape(boolean doesMethodEscape) {
        this.doesMethodEscape = doesMethodEscape;
        return this;
    }

    public boolean doesMethodEscape() {
        return doesMethodEscape;
    }

    public ClassNode addExtractedVariable(String extractedVariable) {
        extractedVariables.add(extractedVariable);
        return this;
    }

    public boolean containsExtractedVariable(String extractedVariable) {
        return extractedVariables.contains(extractedVariable);
    }

    public ClassNode removeExtractedVariable(String extractedVariable) {
        extractedVariables.remove(extractedVariable);
        return this;
    }

    public int getExtractedVariablesSize() {
        return extractedVariables.size();
    }

    public Set<String> getExtractedVariables() {
        return extractedVariables;
    }

    public ClassNode clearExtractedVariables() {
        extractedVariables.clear();
        return this;
    }
    
    public ClassNode addGetMethod(org.objectweb.asm.commons.Method getMethod) {
        getMethods.add(getMethod);
        return this;
    }

    public ClassNode setGetMethod(int index, org.objectweb.asm.commons.Method getMethod) {
        getMethods.set(index, getMethod);
        return this;
    }

    public org.objectweb.asm.commons.Method getGetMethod(int index) {
        return getMethods.get(index);
    }

    public ClassNode removeGetMethod(org.objectweb.asm.commons.Method getMethod) {
        getMethods.remove(getMethod);
        return this;
    }

    public ClassNode removeGetMethod(int index) {
        getMethods.remove(index);
        return this;
    }

    public int getGetMethodsSize() {
        return getMethods.size();
    }

    public List<org.objectweb.asm.commons.Method> getGetMethods() {
        return getMethods;
    }

    public ClassNode clearGetMethods() {
        getMethods.clear();
        return this;
    }
    
    /* ---- end node data ---- */

    protected Globals globals;
    protected byte[] bytes;

    public ClassNode() {
        // do nothing
    }

    public BitSet getStatements() {
        return globals.getStatements();
    }

    public byte[] getBytes() {
        return bytes;
    }

    public Map<String, Object> write() {
        this.globals = new Globals(new BitSet(sourceText.length()));

        // Create the ClassWriter.

        int classFrames = org.objectweb.asm.ClassWriter.COMPUTE_FRAMES | org.objectweb.asm.ClassWriter.COMPUTE_MAXS;
        int classAccess = Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | Opcodes.ACC_FINAL;
        String interfaceBase = BASE_INTERFACE_TYPE.getInternalName();
        String className = CLASS_TYPE.getInternalName();
        String[] classInterfaces = new String[] { interfaceBase };

        ClassWriter classWriter = new ClassWriter(scriptRoot.getCompilerSettings(), globals.getStatements(), debugStream,
                scriptClassInfo.getBaseClass(), classFrames, classAccess, className, classInterfaces);
        ClassVisitor classVisitor = classWriter.getClassVisitor();
        classVisitor.visitSource(Location.computeSourceName(name), null);

        // Write the a method to bootstrap def calls
        MethodWriter bootstrapDef = classWriter.newMethodWriter(Opcodes.ACC_STATIC | Opcodes.ACC_VARARGS, DEF_BOOTSTRAP_METHOD);
        bootstrapDef.visitCode();
        bootstrapDef.getStatic(CLASS_TYPE, "$DEFINITION", DEFINITION_TYPE);
        bootstrapDef.getStatic(CLASS_TYPE, "$FUNCTIONS", FUNCTION_TABLE_TYPE);
        bootstrapDef.loadArgs();
        bootstrapDef.invokeStatic(DEF_BOOTSTRAP_DELEGATE_TYPE, DEF_BOOTSTRAP_DELEGATE_METHOD);
        bootstrapDef.returnValue();
        bootstrapDef.endMethod();

        // Write static variables for name, source and statements used for writing exception messages
        classVisitor.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "$NAME", STRING_TYPE.getDescriptor(), null, null).visitEnd();
        classVisitor.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "$SOURCE", STRING_TYPE.getDescriptor(), null, null).visitEnd();
        classVisitor.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "$STATEMENTS", BITSET_TYPE.getDescriptor(), null, null).visitEnd();

        // Write the static variables used by the method to bootstrap def calls
        classVisitor.visitField(
                Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "$DEFINITION", DEFINITION_TYPE.getDescriptor(), null, null).visitEnd();
        classVisitor.visitField(
                Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "$FUNCTIONS", FUNCTION_TABLE_TYPE.getDescriptor(), null, null).visitEnd();

        org.objectweb.asm.commons.Method init;

        if (scriptClassInfo.getBaseClass().getConstructors().length == 0) {
            init = new org.objectweb.asm.commons.Method("<init>", MethodType.methodType(void.class).toMethodDescriptorString());
        } else {
            init = new org.objectweb.asm.commons.Method("<init>", MethodType.methodType(void.class,
                scriptClassInfo.getBaseClass().getConstructors()[0].getParameterTypes()).toMethodDescriptorString());
        }

        // Write the constructor:
        MethodWriter constructor = classWriter.newMethodWriter(Opcodes.ACC_PUBLIC, init);
        constructor.visitCode();
        constructor.loadThis();
        constructor.loadArgs();
        constructor.invokeConstructor(Type.getType(scriptClassInfo.getBaseClass()), init);
        constructor.returnValue();
        constructor.endMethod();

        // Write a method to get static variable source
        MethodWriter nameMethod = classWriter.newMethodWriter(Opcodes.ACC_PUBLIC, GET_NAME_METHOD);
        nameMethod.visitCode();
        nameMethod.getStatic(CLASS_TYPE, "$NAME", STRING_TYPE);
        nameMethod.returnValue();
        nameMethod.endMethod();

        // Write a method to get static variable source
        MethodWriter sourceMethod = classWriter.newMethodWriter(Opcodes.ACC_PUBLIC, GET_SOURCE_METHOD);
        sourceMethod.visitCode();
        sourceMethod.getStatic(CLASS_TYPE, "$SOURCE", STRING_TYPE);
        sourceMethod.returnValue();
        sourceMethod.endMethod();

        // Write a method to get static variable statements
        MethodWriter statementsMethod = classWriter.newMethodWriter(Opcodes.ACC_PUBLIC, GET_STATEMENTS_METHOD);
        statementsMethod.visitCode();
        statementsMethod.getStatic(CLASS_TYPE, "$STATEMENTS", BITSET_TYPE);
        statementsMethod.returnValue();
        statementsMethod.endMethod();

        // Write the method defined in the interface:
        MethodWriter executeMethod = classWriter.newMethodWriter(Opcodes.ACC_PUBLIC, scriptClassInfo.getExecuteMethod());
        executeMethod.visitCode();
        write(classWriter, executeMethod, globals);
        executeMethod.endMethod();

        // Write all fields:
        for (FieldNode fieldNode : fieldNodes) {
            fieldNode.write(classWriter, null, null);
        }

        // Write all functions:
        for (FunctionNode functionNode : functionNodes) {
            functionNode.write(classWriter, null, globals);
        }

        // Write the constants
        if (false == globals.getConstantInitializers().isEmpty()) {
            Collection<Constant> inits = globals.getConstantInitializers().values();

            // Initialize the constants in a static initializerNode
            final MethodWriter clinit = new MethodWriter(Opcodes.ACC_STATIC,
                    WriterConstants.CLINIT, classVisitor, globals.getStatements(), scriptRoot.getCompilerSettings());
            clinit.visitCode();
            for (Constant constant : inits) {
                constant.initializer.accept(clinit);
                clinit.putStatic(CLASS_TYPE, constant.name, constant.type);
            }
            clinit.returnValue();
            clinit.endMethod();
        }

        // Write any needsVarName methods for used variables
        for (org.objectweb.asm.commons.Method needsMethod : scriptClassInfo.getNeedsMethods()) {
            String name = needsMethod.getName();
            name = name.substring(5);
            name = Character.toLowerCase(name.charAt(0)) + name.substring(1);
            MethodWriter ifaceMethod = classWriter.newMethodWriter(Opcodes.ACC_PUBLIC, needsMethod);
            ifaceMethod.visitCode();
            ifaceMethod.push(extractedVariables.contains(name));
            ifaceMethod.returnValue();
            ifaceMethod.endMethod();
        }

        // End writing the class and store the generated bytes.

        classVisitor.visitEnd();
        bytes = classWriter.getClassBytes();

        Map<String, Object> statics = new HashMap<>();
        statics.put("$FUNCTIONS", scriptRoot.getFunctionTable());

        for (FieldNode fieldNode : fieldNodes) {
            if (fieldNode.getInstance() != null) {
                statics.put(fieldNode.getName(), fieldNode.getInstance());
            }
        }

        return statics;
    }

    @Override
    protected void write(org.elasticsearch.painless.ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        // We wrap the whole method in a few try/catches to handle and/or convert other exceptions to ScriptException
        Label startTry = new Label();
        Label endTry = new Label();
        Label startExplainCatch = new Label();
        Label startOtherCatch = new Label();
        Label endCatch = new Label();
        methodWriter.mark(startTry);

        if (scriptRoot.getCompilerSettings().getMaxLoopCounter() > 0) {
            // if there is infinite loop protection, we do this once:
            // int #loop = settings.getMaxLoopCounter()

            Variable loop = mainMethod.getVariable(null, Locals.LOOP);

            methodWriter.push(scriptRoot.getCompilerSettings().getMaxLoopCounter());
            methodWriter.visitVarInsn(Opcodes.ISTORE, loop.getSlot());
        }

        for (org.objectweb.asm.commons.Method method : getMethods) {
            String name = method.getName().substring(3);
            name = Character.toLowerCase(name.charAt(0)) + name.substring(1);
            Variable variable = mainMethod.getVariable(null, name);

            methodWriter.loadThis();
            methodWriter.invokeVirtual(Type.getType(scriptClassInfo.getBaseClass()), method);
            methodWriter.visitVarInsn(method.getReturnType().getOpcode(Opcodes.ISTORE), variable.getSlot());
        }

        for (StatementNode statementNode : statementNodes) {
            statementNode.write(classWriter, methodWriter, globals);
        }

        if (doesMethodEscape == false) {
            switch (scriptClassInfo.getExecuteMethod().getReturnType().getSort()) {
                case org.objectweb.asm.Type.VOID:
                    break;
                case org.objectweb.asm.Type.BOOLEAN:
                    methodWriter.push(false);
                    break;
                case org.objectweb.asm.Type.BYTE:
                    methodWriter.push(0);
                    break;
                case org.objectweb.asm.Type.SHORT:
                    methodWriter.push(0);
                    break;
                case org.objectweb.asm.Type.INT:
                    methodWriter.push(0);
                    break;
                case org.objectweb.asm.Type.LONG:
                    methodWriter.push(0L);
                    break;
                case org.objectweb.asm.Type.FLOAT:
                    methodWriter.push(0f);
                    break;
                case org.objectweb.asm.Type.DOUBLE:
                    methodWriter.push(0d);
                    break;
                default:
                    methodWriter.visitInsn(Opcodes.ACONST_NULL);
            }
            methodWriter.returnValue();
        }

        methodWriter.mark(endTry);
        methodWriter.goTo(endCatch);
        // This looks like:
        // } catch (PainlessExplainError e) {
        //   throw this.convertToScriptException(e, e.getHeaders($DEFINITION))
        // }
        methodWriter.visitTryCatchBlock(startTry, endTry, startExplainCatch, PAINLESS_EXPLAIN_ERROR_TYPE.getInternalName());
        methodWriter.mark(startExplainCatch);
        methodWriter.loadThis();
        methodWriter.swap();
        methodWriter.dup();
        methodWriter.getStatic(CLASS_TYPE, "$DEFINITION", DEFINITION_TYPE);
        methodWriter.invokeVirtual(PAINLESS_EXPLAIN_ERROR_TYPE, PAINLESS_EXPLAIN_ERROR_GET_HEADERS_METHOD);
        methodWriter.invokeInterface(BASE_INTERFACE_TYPE, CONVERT_TO_SCRIPT_EXCEPTION_METHOD);
        methodWriter.throwException();
        // This looks like:
        // } catch (PainlessError | BootstrapMethodError | OutOfMemoryError | StackOverflowError | Exception e) {
        //   throw this.convertToScriptException(e, e.getHeaders())
        // }
        // We *think* it is ok to catch OutOfMemoryError and StackOverflowError because Painless is stateless
        methodWriter.visitTryCatchBlock(startTry, endTry, startOtherCatch, PAINLESS_ERROR_TYPE.getInternalName());
        methodWriter.visitTryCatchBlock(startTry, endTry, startOtherCatch, BOOTSTRAP_METHOD_ERROR_TYPE.getInternalName());
        methodWriter.visitTryCatchBlock(startTry, endTry, startOtherCatch, OUT_OF_MEMORY_ERROR_TYPE.getInternalName());
        methodWriter.visitTryCatchBlock(startTry, endTry, startOtherCatch, STACK_OVERFLOW_ERROR_TYPE.getInternalName());
        methodWriter.visitTryCatchBlock(startTry, endTry, startOtherCatch, EXCEPTION_TYPE.getInternalName());
        methodWriter.mark(startOtherCatch);
        methodWriter.loadThis();
        methodWriter.swap();
        methodWriter.invokeStatic(COLLECTIONS_TYPE, EMPTY_MAP_METHOD);
        methodWriter.invokeInterface(BASE_INTERFACE_TYPE, CONVERT_TO_SCRIPT_EXCEPTION_METHOD);
        methodWriter.throwException();
        methodWriter.mark(endCatch);
    }
}
