/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.hash.MurmurHash3;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * ID Generator for creating unique but deterministic document ids.
 *
 * uses MurmurHash with 128 bits
 */
public final class IDGenerator {
    private static final byte[] NULL_VALUE = "__NULL_VALUE__".getBytes(StandardCharsets.UTF_8);
    private static final BytesRef DELIM = new BytesRef("$");
    private static final long SEED = 19;
    private static final int MAX_FIRST_BYTES = 5;

    private final BytesRefBuilder buffer = new BytesRefBuilder();
    private final BytesRefBuilder firstBytes =  new BytesRefBuilder();

    public IDGenerator() {
    }

    /**
     * Add a value to the generator
     * @param value the value
     */
    public void add(Object value) {
        byte[] v = getBytes(value);

        buffer.append(v, 0, v.length);
        buffer.append(DELIM);

        // keep the 1st byte of every object
        if (firstBytes.length() <= MAX_FIRST_BYTES) {
            firstBytes.append(v[0]);
        }
    }

    /**
     * Create a document id based on the input objects
     *
     * @return a document id as string
     */
    public String getID() {
        if (buffer.length() == 0) {
            throw new RuntimeException("Add at least 1 object before generating the ID");
        }

        MurmurHash3.Hash128 hasher = MurmurHash3.hash128(buffer.bytes(), 0, buffer.length(), SEED, new MurmurHash3.Hash128());
        byte[] hashedBytes = new byte[16 + firstBytes.length()];
        System.arraycopy(firstBytes.bytes(), 0, hashedBytes, 0, firstBytes.length());
        System.arraycopy(Numbers.longToBytes(hasher.h1), 0, hashedBytes, firstBytes.length(), 8);
        System.arraycopy(Numbers.longToBytes(hasher.h2), 0, hashedBytes, firstBytes.length() + 8, 8);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(hashedBytes);
    }

    /**
     * Turns objects into byte arrays, only supporting types returned groupBy
     *
     * @param value the value as object
     * @return a byte representation of the input object
     */
    private static byte[] getBytes(Object value) {
        if (value == null) {
            return NULL_VALUE;
        } else if (value instanceof String) {
            return ((String) value).getBytes(StandardCharsets.UTF_8);
        } else if (value instanceof Long) {
            return Numbers.longToBytes((Long) value);
        } else if (value instanceof Double) {
            return Numbers.doubleToBytes((Double) value);
        } else if (value instanceof Integer) {
            return Numbers.intToBytes((Integer) value);
        }

        throw new IllegalArgumentException("Value of type [" + value.getClass() + "] is not supported");
    }
}
