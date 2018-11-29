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

package org.elasticsearch.action.admin.cluster.repositories.delete;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DeleteInternalRepositoryAction extends Action<AcknowledgedResponse> {

    public static final DeleteInternalRepositoryAction INSTANCE = new DeleteInternalRepositoryAction();
    public static final String NAME = "cluster:admin/internal_repository/delete";

    protected DeleteInternalRepositoryAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writeable.Reader<AcknowledgedResponse> getResponseReader() {
        return in -> {
            AcknowledgedResponse acknowledgedResponse = new AcknowledgedResponse();
            acknowledgedResponse.readFrom(in);
            return acknowledgedResponse;
        };
    }

    public static class DeleteInternalRepositoryRequest extends ActionRequest {

        private String name;

        public DeleteInternalRepositoryRequest(String name) {
            this.name = name;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (name == null) {
                validationException = addValidationError("name is missing", validationException);
            }
            return validationException;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DeleteInternalRepositoryRequest that = (DeleteInternalRepositoryRequest) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        @Override
        public String toString() {
            return "DeleteInternalRepositoryRequest{" +
                "name='" + name + '\'' +
                '}';
        }
    }

    public static class TransportDeleteInternalRepositoryAction
        extends TransportAction<DeleteInternalRepositoryRequest, AcknowledgedResponse> {

        private final RepositoriesService repositoriesService;

        @Inject
        public TransportDeleteInternalRepositoryAction(RepositoriesService repositoriesService, ActionFilters actionFilters,
                                                       TransportService transportService) {
            super(NAME, actionFilters, transportService.getTaskManager());
            this.repositoriesService = repositoriesService;
        }

        @Override
        protected void doExecute(Task task, DeleteInternalRepositoryRequest request, ActionListener<AcknowledgedResponse> listener) {
            repositoriesService.unregisterInternalRepository(request.name);
            listener.onResponse(new AcknowledgedResponse(true));
        }
    }
}
