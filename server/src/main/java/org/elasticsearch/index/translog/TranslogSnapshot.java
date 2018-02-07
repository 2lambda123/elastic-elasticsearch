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
package org.elasticsearch.index.translog;

import java.io.IOException;

/**
 * A forward-only iterator over a fixed snapshot of a single translog generation.
 */
final class TranslogSnapshot extends TranslogSnapshotReader {

    /**
     * Create a snapshot of the translog file channel. This gives a forward-only iterator over the operations in the snapshot.
     *
     * @param reader the underlying reader
     * @param length the size in bytes of the underlying snapshot
     */
    TranslogSnapshot(final BaseTranslogReader reader, final long length) {
        super(reader, length);
    }

    /**
     * The current operation. The iterator is advanced to the next operation. If the iterator reaches the end of the snapshot, further
     * invocations of this method will return null.
     *
     * @return the current operation, or null if at the end of this this snapshot
     * @throws IOException if an I/O exception occurs reading the snapshot
     */
    Translog.OperationWithPosition next() throws IOException {
        if (getReadOperations() < totalOperations()) {
            return readOperation();
        } else {
            return null;
        }
    }

}
