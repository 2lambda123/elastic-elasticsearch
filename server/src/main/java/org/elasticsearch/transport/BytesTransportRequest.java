/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.RefCounted;

import java.io.IOException;

/**
 * A specialized, bytes only request, that can potentially be optimized on the network
 * layer, specifically for the same large buffer send to several nodes.
 */
public class BytesTransportRequest extends TransportRequest implements RefCounted {

    public static final Version COMPRESSION_SCHEME_VERSION = Version.V_8_0_0;

    private final Compression.Scheme compressionScheme;
    final ReleasableBytesReference bytes;
    private final Version version;

    public BytesTransportRequest(StreamInput in) throws IOException {
        super(in);

        if (in.getVersion().onOrAfter(COMPRESSION_SCHEME_VERSION)) {
            compressionScheme = in.readEnum(Compression.Scheme.class);
        } else {
            compressionScheme = null;
        }
        bytes = in.readReleasableBytesReference();
        version = in.getVersion();
    }

    public BytesTransportRequest(Compression.Scheme compressionScheme, ReleasableBytesReference bytes, Version version) {
        this.compressionScheme = compressionScheme;
        this.bytes = bytes;
        this.version = version;
    }

    public Version version() {
        return this.version;
    }

    public Compression.Scheme compressionScheme() {
        return compressionScheme;
    }

    public BytesReference bytes() {
        return this.bytes;
    }

    /**
     * Writes the data in a "thin" manner, without the actual bytes, assumes
     * the actual bytes will be appended right after this content.
     */
    public void writeThin(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(COMPRESSION_SCHEME_VERSION)) {
            out.writeEnum(compressionScheme);
        } else {
            assert compressionScheme == Compression.Scheme.DEFLATE;
        }
        out.writeVInt(bytes.length());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(COMPRESSION_SCHEME_VERSION)) {
            out.writeOptionalEnum(compressionScheme);
        } else {
            assert compressionScheme == Compression.Scheme.DEFLATE;
        }
        out.writeBytesReference(bytes);
    }

    @Override
    public void incRef() {
        bytes.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return bytes.tryIncRef();
    }

    @Override
    public boolean decRef() {
        return bytes.decRef();
    }

    @Override
    public boolean hasReferences() {
        return bytes.hasReferences();
    }
}
