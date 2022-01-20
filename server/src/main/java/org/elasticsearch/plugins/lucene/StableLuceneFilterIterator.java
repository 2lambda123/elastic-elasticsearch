/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.lucene;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.plugins.analysis.AnalyzeToken;
import org.elasticsearch.plugins.analysis.PortableAnalyzeIterator;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/**
 * This is currently implemented using method handles in contrast of ESTokenStream, it may not be required after we
 * try with different class loaders.
 */
@SuppressForbidden(reason = "using method handles required for multiple class loaders")
public class StableLuceneFilterIterator implements PortableAnalyzeIterator {
    protected final Object stream;
    protected final Object term;
    protected final Object posIncr;
    protected final Object offset;
    protected final Object type;
    protected final Object posLen;

    private final MethodHandle mhEnd;
    private final MethodHandle mhReset;
    private final MethodHandle mhClose;
    private final MethodHandle mhIncrementToken;
    private final MethodHandle mhAddAttribute;
    private final MethodHandle mhAttrGetPositionIncrement;
    private final MethodHandle mhAttrStartOffset;
    private final MethodHandle mhAttrEndOffset;
    private final MethodHandle mhAttrGetPositionLength;
    private final MethodHandle mhAttrType;

    public StableLuceneFilterIterator(Object stream) {
        ensureClassCompatibility(stream.getClass(), "org.apache.lucene.analysis.TokenStream");
        this.stream = stream;
        MethodHandles.Lookup lookup = MethodHandles.lookup();

        try {
            // TokenStream method handles
            Class<?> tokenStreamClass = lookupClass(stream, "org.apache.lucene.analysis.TokenStream");

            mhEnd = lookup.findVirtual(tokenStreamClass, "end", MethodType.methodType(void.class));
            mhReset = lookup.findVirtual(tokenStreamClass, "reset", MethodType.methodType(void.class));
            mhClose = lookup.findVirtual(tokenStreamClass, "close", MethodType.methodType(void.class));
            mhIncrementToken = lookup.findVirtual(tokenStreamClass, "incrementToken", MethodType.methodType(boolean.class));
            mhAddAttribute = lookup.findVirtual(
                tokenStreamClass,
                "addAttribute",
                MethodType.methodType(lookupClass(stream, "org.apache.lucene.util.Attribute"), Class.class)
            );

            // Lucene analysis Attribute method handles and object creation
            term = mhAddAttribute.invoke(stream, lookupClass(stream, "org.apache.lucene.analysis.tokenattributes.CharTermAttribute"));

            posIncr = mhAddAttribute.invoke(
                stream,
                lookupClass(stream, "org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute")
            );
            mhAttrGetPositionIncrement = lookup.findVirtual(posIncr.getClass(), "getPositionIncrement", MethodType.methodType(int.class));

            offset = mhAddAttribute.invoke(stream, lookupClass(stream, "org.apache.lucene.analysis.tokenattributes.OffsetAttribute"));

            mhAttrStartOffset = lookup.findVirtual(offset.getClass(), "startOffset", MethodType.methodType(int.class));
            mhAttrEndOffset = lookup.findVirtual(offset.getClass(), "endOffset", MethodType.methodType(int.class));

            type = mhAddAttribute.invoke(stream, lookupClass(stream, "org.apache.lucene.analysis.tokenattributes.TypeAttribute"));

            mhAttrType = lookup.findVirtual(type.getClass(), "type", MethodType.methodType(String.class));

            posLen = mhAddAttribute.invoke(
                stream,
                lookupClass(stream, "org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute")
            );

            mhAttrGetPositionLength = lookup.findVirtual(posLen.getClass(), "getPositionLength", MethodType.methodType(int.class));

        } catch (Throwable x) {
            throw new IllegalArgumentException("Incompatible Lucene library provided", x);
        }
    }

    @Override
    public AnalyzeToken reset() {
        try {
            mhReset.invoke(stream);
            return currentState();
        } catch (Throwable t) {
            throw new IllegalArgumentException("Unsupported token stream operation", t);
        }
    }

    @Override
    public AnalyzeToken next() {
        try {
            if ((boolean) mhIncrementToken.invoke(stream)) {
                return currentState();
            }
            return null;
        } catch (Throwable t) {
            throw new IllegalArgumentException("Unsupported token stream operation", t);
        }
    }

    private AnalyzeToken currentState() throws Throwable {
        return new AnalyzeToken(
            term.toString(),
            (int) mhAttrGetPositionIncrement.invoke(posIncr),
            (int) mhAttrStartOffset.invoke(offset),
            (int) mhAttrEndOffset.invoke(offset),
            (int) mhAttrGetPositionLength.invoke(posLen),
            (String) mhAttrType.invoke(type)
        );
    }

    @Override
    public AnalyzeToken end() {
        try {
            mhEnd.invoke(stream);
            return currentState();
        } catch (Throwable t) {
            throw new IllegalArgumentException("Unsupported token stream operation", t);
        }
    }

    @Override
    public void close() {
        try {
            mhClose.invoke(stream);
        } catch (Throwable t) {
            throw new IllegalArgumentException("Unsupported token stream operation", t);
        }
    }

    protected static boolean instanceOrSubclass(Class<?> clazz, String matchingClassName) {
        while (Object.class.equals(clazz) == false) {
            if (clazz.getCanonicalName().equals(matchingClassName)) {
                return true;
            }

            Class<?> superClass = clazz.getSuperclass();
            if (superClass == null) {
                break;
            }
            clazz = superClass;
        }

        return false;
    }

    protected static void ensureClassCompatibility(Class<?> clazz, String matchingName) {
        if (instanceOrSubclass(clazz, matchingName) == false) {
            throw new IllegalArgumentException("You must provide a Lucene TokenStream.class instance");
        }
    }

    protected static Class<?> lookupClass(Object source, String className) {
        try {
            return source.getClass().getClassLoader().loadClass(className);
        } catch (ClassNotFoundException notFoundException) {
            throw new IllegalArgumentException("You must provide a Lucene TokenStream.class instance");
        }
    }
}
