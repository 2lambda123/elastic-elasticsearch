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

package org.elasticsearch.graphql.gql.schema;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.StaticDataFetcher;
import org.elasticsearch.graphql.gql.GqlBuilder;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLNonNull.nonNull;
import static graphql.Scalars.*;

import java.util.function.Function;

public class GqlPingSchema {

    /**
     * - Adds `Query.ping: String` resolver.
     * - Adds `Mutation.ping: String` resolver.
     * - Adds `Subscription.ping: String` resolver.
     */
    public Function<GqlBuilder, GqlBuilder> use = builder -> builder
        .queryField(newFieldDefinition()
            .description("Ping server if it is available.")
            .name("ping")
            .type(nonNull(GraphQLString)))
        .mutationField(newFieldDefinition()
            .description("Ping server if it is available.")
            .name("ping")
            .type(nonNull(GraphQLString)))
        .subscriptionField(newFieldDefinition()
            .description("Ping subscription fot testing the server.")
            .name("ping")
            .type(nonNull(GraphQLString)))
        .fetcher("Query", "ping", new StaticDataFetcher("pong"))
        .fetcher("Mutation", "ping", new StaticDataFetcher("pong"))
        .fetcher("Subscription", "ping",  new DataFetcher<Object>() {
            @Override
            public Publisher<String> get(DataFetchingEnvironment environment) {
                return s -> {
                    s.onSubscribe(new Subscription() {
                        @Override
                        public void request(long n) {
                            while (n --> 0) {
                                try {
                                    Thread.sleep(3000);
                                    s.onNext("pong");
                                } catch (Exception e) {}
                            }
                        }

                        @Override
                        public void cancel() {
                            s.onComplete();
                        }
                    });
                };
            }
        });
}
