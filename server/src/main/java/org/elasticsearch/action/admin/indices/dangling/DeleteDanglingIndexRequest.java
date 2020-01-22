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

package org.elasticsearch.action.admin.indices.dangling;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Represents a request to delete a particular dangling index, specified by its UUID. The {@link #acceptDataLoss}
 * flag must also be explicitly set to true, or later validation will fail.
 */
public class DeleteDanglingIndexRequest extends MasterNodeRequest<DeleteDanglingIndexRequest> {
    private final String indexUUID;
    private final boolean acceptDataLoss;

    public DeleteDanglingIndexRequest(StreamInput in) throws IOException {
        super(in);
        this.indexUUID = in.readString();
        this.acceptDataLoss = in.readBoolean();
    }

    public DeleteDanglingIndexRequest(String indexUUID, boolean acceptDataLoss) {
        super();
        this.indexUUID = Strings.requireNonEmpty(indexUUID, "indexUUID cannot be null or empty");
        this.acceptDataLoss = acceptDataLoss;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (this.indexUUID == null) {
            ActionRequestValidationException e = new ActionRequestValidationException();
            e.addValidationError("No index UUID specified");
            return e;
        }

        // acceptDataLoss is validated later in the transport action, so that the API call can
        // be made to check that the UUID exists.

        return null;
    }

    public String getIndexUUID() {
        return indexUUID;
    }

    public boolean isAcceptDataLoss() {
        return acceptDataLoss;
    }

    @Override
    public String toString() {
        return "DeleteDanglingIndexRequest{" + "indexUUID='" + indexUUID + '\'' + ", acceptDataLoss=" + acceptDataLoss + '}';
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(this.indexUUID);
        out.writeBoolean(this.acceptDataLoss);
    }
}
