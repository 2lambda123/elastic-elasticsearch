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

package org.elasticsearch.plugin.indexbysearch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.TransportClearScrollAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.search.TransportSearchScrollAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportIndexBySearchAction extends HandledTransportAction<IndexBySearchRequest, IndexBySearchResponse> {
    private final TransportSearchAction searchAction;
    private final TransportSearchScrollAction scrollAction;
    private final TransportBulkAction bulkAction;
    private final TransportClearScrollAction clearScrollAction;

    @Inject
    public TransportIndexBySearchAction(Settings settings, ThreadPool threadPool, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver, TransportSearchAction transportSearchAction,
            TransportSearchScrollAction transportSearchScrollAction, TransportBulkAction bulkAction,
            TransportClearScrollAction clearScrollAction, TransportService transportService) {
        super(settings, IndexBySearchAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                IndexBySearchRequest::new);
        this.searchAction = transportSearchAction;
        this.scrollAction = transportSearchScrollAction;
        this.bulkAction = bulkAction;
        this.clearScrollAction = clearScrollAction;
    }

    @Override
    protected void doExecute(IndexBySearchRequest request, ActionListener<IndexBySearchResponse> listener) {
        new AsyncIndexBySearchAction(request, listener).start();
    }



    /**
     * Simple implementation of index-by-search scrolling and bulk. There are
     * tons of optimizations that can be done on certain types of index-by-query
     * requests but this makes no attempt to do any of them so it can be as
     * simple possible.
     */
    class AsyncIndexBySearchAction extends AbstractAsyncBulkIndexByScrollAction<IndexBySearchRequest, IndexBySearchResponse> {
        public AsyncIndexBySearchAction(IndexBySearchRequest request, ActionListener<IndexBySearchResponse> listener) {
            super(logger, searchAction, scrollAction, bulkAction, clearScrollAction, request, request.search(), listener);
        }

        @Override
        protected BulkRequest buildBulk(Iterable<SearchHit> docs) {
            BulkRequest bulkRequest = new BulkRequest(mainRequest);

            for (SearchHit doc : docs) {
                IndexRequest index = new IndexRequest(mainRequest.index(), mainRequest);

                // We want the index from the copied request, not the doc.
                index.id(doc.id());
                if (index.type() == null) {
                    /*
                     * Default to doc's type if not specified in request so its
                     * easy to do a scripted update.
                     */
                    index.type(doc.type());
                }
                index.source(doc.sourceRef());
                switch (mainRequest.opType()) {
                case REFRESH:
                    index.versionType(VersionType.EXTERNAL);
                    index.version(doc.version());
                    break;
                case OVERWRITE:
                    index.versionType(VersionType.INTERNAL);
                    index.version(Versions.MATCH_ANY);
                    break;
                case CREATE:
                    // Matches deleted or absent entirely.
                    index.versionType(VersionType.INTERNAL);
                    index.version(Versions.MATCH_DELETED);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown op_type [" + mainRequest.opType() + ']');
                }

                copyMetadata(index, doc);

                bulkRequest.add(index);
            }
            return bulkRequest;
        }

        /**
         * Override the simple copy behavior to allow more fine grained control.
         */
        @Override
        protected void copyRouting(IndexRequest index, SearchHit doc) {
            String routingSpec = mainRequest.index().routing();
            if (routingSpec == null) {
                super.copyRouting(index, doc);
                return;
            }
            if (routingSpec.startsWith("=")) {
                index.routing(mainRequest.index().routing().substring(1));
                return;
            }
            switch (routingSpec) {
            case "keep":
                super.copyRouting(index, doc);
                break;
            case "discard":
                index.routing(null);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported routing command");
            }
        }

        @Override
        protected IndexBySearchResponse buildResponse(long took) {
            return new IndexBySearchResponse(took, created(), updated(), batches(), versionConflicts(), noops(), failures());
        }
    }
}
