/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.dlm;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class AuthorizeDataLifecycleAction extends ActionType<AcknowledgedResponse> {
    public static final AuthorizeDataLifecycleAction INSTANCE = new AuthorizeDataLifecycleAction();
    public static final String NAME = "cluster:admin/dlm/authorize";

    private AuthorizeDataLifecycleAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static final class Request extends AcknowledgedRequest<Request> {
        private final String[] dataStreamPatterns;

        public Request(String[] dataStreamPatterns) {
            this.dataStreamPatterns = dataStreamPatterns;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.dataStreamPatterns = in.readOptionalStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalStringArray(dataStreamPatterns);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public String[] getDataStreamPatterns() {
            return dataStreamPatterns;
        }
    }
}
