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

package org.elasticsearch.rest;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.rest.RestRequest.Method;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.unmodifiableList;

/**
 * Helper subclass for describing REST handlers that map multiple HTTP method to the same paths.
 */
public abstract class BaseMultiMethodRestHandler extends BaseRestHandler {
    private final Method[] methods;
    private final String[] paths;

    public BaseMultiMethodRestHandler(RestGlobalContext context, Method[] methods, String... paths) {
        super(context);
        this.methods = methods;
        this.paths = paths;
    }

    protected String[] paths() {
        return paths;
    }

    @Override
    public final Collection<Tuple<Method, String>> registrations() {
        List<Tuple<Method, String>> registrations = new ArrayList<>(paths.length * methods.length);
        for (String path : paths) {
            for (Method method: methods) {
                registrations.add(new Tuple<>(method, path));
            }
        }
        return unmodifiableList(registrations);
    }

}
