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

package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Objects;

/**
 * A based request for master based operation.
 */
public abstract class MasterNodeRequest<Request extends MasterNodeRequest<Request>> extends ActionRequest {

    public static final TimeValue DEFAULT_MASTER_NODE_TIMEOUT = TimeValue.timeValueSeconds(30);

    protected TimeValue masterNodeTimeout = DEFAULT_MASTER_NODE_TIMEOUT;

    protected MasterNodeRequest() {
    }

    protected MasterNodeRequest(StreamInput in) throws IOException {
        super(in);
        masterNodeTimeout = new TimeValue(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        masterNodeTimeout.writeTo(out);
    }

    /**
     * A timeout value in case the master has not been discovered yet or disconnected.
     */
    @SuppressWarnings("unchecked")
    public final Request masterNodeTimeout(TimeValue timeout) {
        this.masterNodeTimeout = timeout;
        return (Request) this;
    }

    /**
     * A timeout value in case the master has not been discovered yet or disconnected.
     */
    public final Request masterNodeTimeout(String timeout) {
        return masterNodeTimeout(TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".masterNodeTimeout"));
    }

    public final TimeValue masterNodeTimeout() {
        return this.masterNodeTimeout;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        // TODO(talevy): throw exception once all MasterNodeRequest
        //               subclasses have been migrated to Writeable Readers
        super.readFrom(in);
        masterNodeTimeout = new TimeValue(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MasterNodeRequest<Request> that = (MasterNodeRequest<Request>) o;
        return Objects.equals(masterNodeTimeout, that.masterNodeTimeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(masterNodeTimeout);
    }

}
