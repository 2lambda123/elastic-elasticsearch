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

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class BytesArray implements BytesReference {

    public static final BytesArray EMPTY = new BytesArray(BytesRef.EMPTY_BYTES, 0, 0);

    private byte[] bytes;
    private int offset;
    private int length;

    public BytesArray(String bytes) {
        BytesRef bytesRef = new BytesRef(bytes);
        this.bytes = bytesRef.bytes;
        this.offset = bytesRef.offset;
        this.length = bytesRef.length;
    }

    public BytesArray(BytesRef bytesRef) {
        this(bytesRef, false);
    }

    public BytesArray(BytesRef bytesRef, boolean deepCopy) {
        if (deepCopy) {
            BytesRef copy = BytesRef.deepCopyOf(bytesRef);
            bytes = copy.bytes;
            offset = copy.offset;
            length = copy.length;
        } else {
            bytes = bytesRef.bytes;
            offset = bytesRef.offset;
            length = bytesRef.length;
        }
    }

    public BytesArray(byte[] bytes) {
        this.bytes = bytes;
        this.offset = 0;
        this.length = bytes.length;
    }

    public BytesArray(byte[] bytes, int offset, int length) {
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public byte get(int index) {
        return bytes[offset + index];
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public BytesReference slice(int from, int length) {
        if (from < 0 || (from + length) > this.length) {
            throw new IllegalArgumentException("can't slice a buffer with length [" + this.length + "], with slice parameters from [" + from + "], length [" + length + "]");
        }
        return new BytesArray(bytes, offset + from, length);
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
        os.write(bytes, offset, length);
    }

    public byte[] array() {
        return bytes;
    }

    public int offset() {
        return offset;
    }

    @Override
    public BytesRef toBytesRef() {
        return new BytesRef(bytes, offset, length);
    }

    @Override
    public int hashCode() {
        return BytesReference.hashCode(bytes, offset, length);
    }

    @Override
    public boolean equals(Object obj) {
        return BytesReference.bytesEqual(this, (BytesReference) obj);
    }

    @Override
    public long ramBytesUsed() {
        return bytes.length;
    }
}
