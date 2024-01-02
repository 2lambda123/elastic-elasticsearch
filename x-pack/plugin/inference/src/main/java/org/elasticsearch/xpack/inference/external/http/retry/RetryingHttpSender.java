/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.concurrent.Executor;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

public class RetryingHttpSender implements RequestSender {
    private final HttpClient httpClient;
    private final ThrottlerManager throttlerManager;
    private final RetrySettings retrySettings;
    private final ThreadPool threadPool;
    private final Executor executor;

    public RetryingHttpSender(
        HttpClient httpClient,
        ThrottlerManager throttlerManager,
        RetrySettings retrySettings,
        ThreadPool threadPool
    ) {
        this(httpClient, throttlerManager, retrySettings, threadPool, threadPool.executor(UTILITY_THREAD_POOL_NAME));
    }

    // For testing only
    RetryingHttpSender(
        HttpClient httpClient,
        ThrottlerManager throttlerManager,
        RetrySettings retrySettings,
        ThreadPool threadPool,
        Executor executor
    ) {
        this.httpClient = Objects.requireNonNull(httpClient);
        this.throttlerManager = Objects.requireNonNull(throttlerManager);
        this.retrySettings = Objects.requireNonNull(retrySettings);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.executor = Objects.requireNonNull(executor);
    }

    private class InternalRetrier extends RetryableAction<InferenceServiceResults> {
        private Request request;
        private final ResponseHandler responseHandler;
        private final Logger logger;
        private final HttpClientContext context;

        InternalRetrier(
            Logger logger,
            Request request,
            HttpClientContext context,
            ResponseHandler responseHandler,
            ActionListener<InferenceServiceResults> listener
        ) {
            super(
                Objects.requireNonNull(logger),
                threadPool,
                retrySettings.getSettings().initialDelay(),
                retrySettings.getSettings().maxDelayBound(),
                retrySettings.getSettings().timeoutValue(),
                listener,
                executor
            );
            this.logger = logger;
            this.request = Objects.requireNonNull(request);
            this.context = Objects.requireNonNull(context);
            this.responseHandler = Objects.requireNonNull(responseHandler);
        }

        @Override
        public void tryAction(ActionListener<InferenceServiceResults> listener) {
            var httpRequest = request.createRequest();

            ActionListener<HttpResult> responseListener = ActionListener.wrap(result -> {
                try {
                    responseHandler.validateResponse(throttlerManager, logger, httpRequest, result);
                    InferenceServiceResults inferenceResults = responseHandler.parseResult(request, result);

                    listener.onResponse(inferenceResults);
                } catch (Exception e) {
                    logException(logger, httpRequest, result, responseHandler.getRequestType(), e);
                    listener.onFailure(e);
                }
            }, e -> {
                logException(logger, httpRequest, responseHandler.getRequestType(), e);
                listener.onFailure(transformIfRetryable(e));
            });

            try {
                httpClient.send(httpRequest, context, responseListener);
            } catch (Exception e) {
                logException(logger, httpRequest, responseHandler.getRequestType(), e);

                // TODO should we log?
                // TODO is this the right exception to return? This is what we were doing for the runnable that the executor would execute
                // listener.onFailure(new ElasticsearchException(format("Failed to send request [%s]", request.getRequestLine()), e));
                listener.onFailure(transformIfRetryable(e));
            }
        }

        @Override
        public boolean shouldRetry(Exception e) {
            if (e instanceof Retryable retry) {
                request = retry.rebuildRequest(request);
                return retry.shouldRetry();
            }

            return false;
        }

        /**
         * If the connection gets closed by the server or because of the connections time to live is exceeded we'll likely get a
         * {@link org.apache.http.ConnectionClosedException} exception which is a child of IOException.
         *
         * @param e the Exception received while sending the request
         * @return a {@link RetryException} if this exception can be retried
         */
        private Exception transformIfRetryable(Exception e) {
            var exceptionToReturn = e;

            if (e instanceof UnknownHostException) {
                return new ElasticsearchStatusException(
                    format("Invalid host [%s], please check that the URL is correct.", request.getURI()),
                    RestStatus.BAD_REQUEST,
                    e
                );
            }

            if (e instanceof IOException) {
                exceptionToReturn = new RetryException(true, e);
            }

            return exceptionToReturn;
        }
    }

    @Override
    public void send(
        Logger logger,
        Request request,
        HttpClientContext context,
        ResponseHandler responseHandler,
        ActionListener<InferenceServiceResults> listener
    ) {
        InternalRetrier retrier = new InternalRetrier(logger, request, context, responseHandler, listener);
        retrier.run();
    }

    private void logException(Logger logger, HttpRequestBase request, String requestType, Exception exception) {
        var causeException = ExceptionsHelper.unwrapCause(exception);

        throttlerManager.warn(
            logger,
            format("Failed while sending request [%s] of type [%s]", request.getRequestLine(), requestType),
            causeException
        );
    }

    private void logException(Logger logger, HttpRequestBase request, HttpResult result, String requestType, Exception exception) {
        var causeException = ExceptionsHelper.unwrapCause(exception);

        throttlerManager.warn(
            logger,
            format(
                "Failed to process the response for request [%s] of type [%s] with status [%s] [%s]",
                request.getRequestLine(),
                requestType,
                result.response().getStatusLine().getStatusCode(),
                result.response().getStatusLine().getReasonPhrase()
            ),
            causeException
        );
    }
}
