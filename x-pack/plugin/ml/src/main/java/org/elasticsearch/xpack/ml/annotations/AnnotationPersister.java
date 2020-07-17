/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.annotations;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditor;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Persists annotations to Elasticsearch index.
 */
public class AnnotationPersister {

    private static final Logger logger = LogManager.getLogger(AnnotationPersister.class);

    private static final int DEFAULT_BULK_LIMIT = 10_000;

    private final ResultsPersisterService resultsPersisterService;
    private final AbstractAuditor<?> auditor;
    /**
     * Execute bulk requests when they reach this size
     */
    private final int bulkLimit;

    public AnnotationPersister(ResultsPersisterService resultsPersisterService, AbstractAuditor<?> auditor) {
        this(resultsPersisterService, auditor, DEFAULT_BULK_LIMIT);
    }

    // For testing
    AnnotationPersister(ResultsPersisterService resultsPersisterService, AbstractAuditor<?> auditor, int bulkLimit) {
        this.resultsPersisterService = Objects.requireNonNull(resultsPersisterService);
        this.auditor = Objects.requireNonNull(auditor);
        this.bulkLimit = bulkLimit;
    }

    /**
     * Persists the given annotation to annotations index.
     *
     * @param annotationId existing annotation id. If {@code null}, a new annotation will be created and id will be assigned automatically
     * @param annotation annotation to be persisted
     * @return tuple of the form (annotation id, annotation object)
     */
    public Tuple<String, Annotation> persistAnnotation(@Nullable String annotationId, Annotation annotation) {
        Objects.requireNonNull(annotation);
        String jobId = annotation.getJobId();
        BulkResponse bulkResponse = bulkPersisterBuilder(jobId).persistAnnotation(annotationId, annotation).executeRequest();
        assert bulkResponse.getItems().length == 1;
        return Tuple.tuple(bulkResponse.getItems()[0].getId(), annotation);
    }

    public Builder bulkPersisterBuilder(String jobId) {
        return new Builder(jobId);
    }

    public class Builder {

        private final String jobId;
        private BulkRequest bulkRequest = new BulkRequest(AnnotationIndex.WRITE_ALIAS_NAME);
        private Supplier<Boolean> shouldRetry = () -> true;

        private Builder(String jobId) {
            this.jobId = Objects.requireNonNull(jobId);
        }

        public Builder shouldRetry(Supplier<Boolean> shouldRetry) {
            this.shouldRetry = Objects.requireNonNull(shouldRetry);
            return this;
        }

        public Builder persistAnnotation(Annotation annotation) {
            return persistAnnotation(null, annotation);
        }

        public Builder persistAnnotation(@Nullable String annotationId, Annotation annotation) {
            Objects.requireNonNull(annotation);
            try (XContentBuilder xContentBuilder = annotation.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)) {
                bulkRequest.add(new IndexRequest().id(annotationId).source(xContentBuilder));
            } catch (IOException e) {
                logger.error(new ParameterizedMessage("[{}] Error serialising annotation", jobId), e);
            }

            if (bulkRequest.numberOfActions() >= bulkLimit) {
                executeRequest();
            }
            return this;
        }

        /**
         * Execute the bulk action
         */
        public BulkResponse executeRequest() {
            if (bulkRequest.numberOfActions() == 0) {
                return null;
            }
            logger.trace("[{}] ES API CALL: bulk request with {} actions", () -> jobId, () -> bulkRequest.numberOfActions());
            BulkResponse bulkResponse =
                resultsPersisterService.bulkIndexWithRetry(
                    bulkRequest, jobId, shouldRetry, msg -> auditor.warning(jobId, "Bulk indexing of annotations failed " + msg));
            bulkRequest = new BulkRequest(AnnotationIndex.WRITE_ALIAS_NAME);
            return bulkResponse;
        }
    }
}
