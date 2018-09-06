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

package org.elasticsearch.client.watcher;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.protocol.xpack.watcher.PutWatchRequest;

import java.util.Locale;
import java.util.Optional;

/**
 * A request to explicitly acknowledge a watch.
 */
public class AckWatchRequest implements Validatable {

    private final String watchId;
    private final String[] actionIds;

    public AckWatchRequest(String watchId, String... actionIds) {
        this.watchId = watchId;
        this.actionIds = actionIds;
    }

    /**
     * @return The ID of the watch to be acked.
     */
    public String getWatchId() {
        return watchId;
    }

    /**
     * @return The IDs of the actions to be acked. If omitted,
     * all actions for the given watch will be acknowledged.
     */
    public String[] getActionIds() {
        return actionIds;
    }

    @Override
    public Optional<ValidationException> validate() {
        ValidationException exception = new ValidationException();

        if (watchId == null) {
            exception.addValidationError("watch id is missing");
        } else if (PutWatchRequest.isValidId(watchId) == false) {
            exception.addValidationError("watch id contains whitespace");
        }

        if (actionIds != null) {
            for (String actionId : actionIds) {
                if (actionId == null) {
                    exception.addValidationError(String.format(Locale.ROOT, "action id may not be null"));
                } else if (PutWatchRequest.isValidId(actionId) == false) {
                    exception.addValidationError(
                        String.format(Locale.ROOT, "action id [%s] contains whitespace", actionId));
                }
            }
        }

        return !exception.validationErrors().isEmpty()
            ? Optional.of(exception)
            : Optional.empty();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ack [").append(watchId).append("]");
        if (actionIds.length > 0) {
            sb.append("[");
            for (int i = 0; i < actionIds.length; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(actionIds[i]);
            }
            sb.append("]");
        }
        return sb.toString();
    }
}
