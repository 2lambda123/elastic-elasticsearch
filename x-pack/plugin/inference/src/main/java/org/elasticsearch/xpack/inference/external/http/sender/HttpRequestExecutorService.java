/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.client.protocol.HttpClientContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.batching.RequestBatcher;
import org.elasticsearch.xpack.inference.external.http.batching.RequestBatcherFactory;
import org.elasticsearch.xpack.inference.external.http.batching.RequestCreator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.core.Strings.format;

// TODO I think this should be moved into the batching package
// TODO rewrite this comment, this no longer has an http client so the timeout stuff doesn't really apply
/**
 * An {@link java.util.concurrent.ExecutorService} for queuing and executing {@link RequestTask} containing
 * {@link org.apache.http.client.methods.HttpRequestBase}. This class is useful because the
 * {@link org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager} will block when leasing a connection if no
 * connections are available. To avoid blocking the inference transport threads, this executor will queue up the
 * requests until connections are available.
 *
 * <b>NOTE:</b> It is the responsibility of the class constructing the
 * {@link org.apache.http.client.methods.HttpUriRequest} to set a timeout for how long this executor will wait
 * attempting to execute a task (aka waiting for the connection manager to lease a connection). See
 * {@link org.apache.http.client.config.RequestConfig.Builder#setConnectionRequestTimeout} for more info.
 */
// TODO rename
class HttpRequestExecutorService<K> implements ExecutorService {
    private static final Logger logger = LogManager.getLogger(HttpRequestExecutorService.class);

    private final String serviceName;
    private final BlockingQueue<Task<K>> queue;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final CountDownLatch terminationLatch = new CountDownLatch(1);
    private final HttpClientContext httpContext;
    private final ThreadPool threadPool;
    private final CountDownLatch startupLatch;
    private final RequestBatcherFactory<K> batcherFactory;

    @SuppressForbidden(reason = "wraps a queue and handles errors appropriately")
    HttpRequestExecutorService(
        String serviceName,
        ThreadPool threadPool,
        @Nullable CountDownLatch startupLatch,
        RequestBatcherFactory<K> batcherFactory
    ) {
        this(serviceName, threadPool, new LinkedBlockingQueue<>(), startupLatch, batcherFactory);
    }

    @SuppressForbidden(reason = "wraps a queue and handles errors appropriately")
    HttpRequestExecutorService(
        String serviceName,
        ThreadPool threadPool,
        int capacity,
        @Nullable CountDownLatch startupLatch,
        RequestBatcherFactory<K> batcherFactory
    ) {
        this(serviceName, threadPool, new LinkedBlockingQueue<>(capacity), startupLatch, batcherFactory);
    }

    /**
     * This constructor should only be used directly for testing.
     */
    @SuppressForbidden(reason = "wraps a queue and handles errors appropriately")
    HttpRequestExecutorService(
        String serviceName,
        ThreadPool threadPool,
        BlockingQueue<Task<K>> queue,
        @Nullable CountDownLatch startupLatch,
        RequestBatcherFactory<K> batcherFactory
    ) {
        this.serviceName = Objects.requireNonNull(serviceName);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.batcherFactory = Objects.requireNonNull(batcherFactory);
        this.httpContext = HttpClientContext.create();
        this.queue = queue;
        this.startupLatch = startupLatch;
    }

    /**
     * Begin servicing tasks.
     */
    public void start() {
        try {
            signalStartInitiated();

            while (running.get()) {
                handleTasks();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            running.set(false);
            notifyRequestsOfShutdown();
            terminationLatch.countDown();
        }
    }

    private void signalStartInitiated() {
        if (startupLatch != null) {
            startupLatch.countDown();
        }
    }

    /**
     * Protects the task retrieval logic from an unexpected exception.
     *
     * @throws InterruptedException rethrows the exception if it occurred retrieving a task because the thread is likely attempting to
     * shut down
     */
    private void handleTasks() throws InterruptedException {
        try {
            var task = queue.take();
            if (task.shouldShutdown() || running.get() == false) {
                running.set(false);
                logger.debug(() -> format("Http executor service [%s] exiting", serviceName));
            } else {
                batchRequests(task);
            }
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            logger.warn(format("Http executor service [%s] failed while retrieving task for execution", serviceName), e);
        }
    }

    private void batchRequests(Task<K> initialTask) {
        try {
            var batcher = batcherFactory.create(httpContext);

            // TODO make this a setting
            // or make this auto adjustable some how
            int batchSize = 5;
            List<Task<K>> requests = new ArrayList<>(batchSize);
            requests.add(initialTask);
            queue.drainTo(requests, batchSize);

            logger.warn(() -> format("Dequeued requests size: %s", requests.size()));

            if (hasAShutdownTask(requests)) {
                running.set(false);
                logger.debug(() -> format("Http executor service [%s] exiting", serviceName));

                rejectTasks(requests);
                return;
            }

            batcher.add(requests);

            runBatch(batcher);
        } catch (Exception e) {
            logger.warn(format("Http executor service [%s] failed to execute batch request", serviceName), e);
        }
    }

    private static <K> boolean hasAShutdownTask(List<Task<K>> requests) {
        return requests.stream().anyMatch(Task::shouldShutdown);
    }

    private static <K> void runBatch(RequestBatcher<K> batcher) {
        for (var runnable : batcher) {
            runnable.run();
        }
    }

    private synchronized void notifyRequestsOfShutdown() {
        assert isShutdown() : "Requests should only be notified if the executor is shutting down";

        try {
            List<Task<K>> notExecuted = new ArrayList<>();
            queue.drainTo(notExecuted);

            for (Task<K> task : notExecuted) {
                rejectTask(task);
            }
        } catch (Exception e) {
            logger.warn(format("Failed to notify tasks of queuing service [%s] shutdown", serviceName));
        }
    }

    private void rejectTasks(List<Task<K>> tasks) {
        for (var task : tasks) {
            rejectTask(task);
        }
    }

    private void rejectTask(Task<K> task) {
        try {
            task.onRejection(
                new EsRejectedExecutionException(
                    format("Failed to send request, queue service [%s] has shutdown prior to executing request", serviceName),
                    true
                )
            );
        } catch (Exception e) {
            logger.warn(
                format("Failed to notify request [%s] for service [%s] of rejection after queuing service shutdown", task, serviceName)
            );
        }
    }

    public int queueSize() {
        return queue.size();
    }

    @Override
    public void shutdown() {
        if (running.compareAndSet(true, false)) {
            // if this fails because the queue is full, that's ok, we just want to ensure that queue.take() returns
            queue.offer(new ShutdownTask<>());
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        throw new UnsupportedOperationException("use shutdown instead");
    }

    @Override
    public boolean isShutdown() {
        return running.get() == false;
    }

    @Override
    public boolean isTerminated() {
        return terminationLatch.getCount() == 0;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return terminationLatch.await(timeout, unit);
    }

    public void send(
        RequestCreator<K> requestCreator,
        List<String> input,
        @Nullable TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        var preservingListener = ContextPreservingActionListener.wrapPreservingContext(listener, threadPool.getThreadContext());
        RequestTask2<K> task = new RequestTask2<>(requestCreator, input, timeout, threadPool, preservingListener);

        if (isShutdown()) {
            EsRejectedExecutionException rejected = new EsRejectedExecutionException(
                format("Failed to enqueue task because the http executor service [%s] has already shutdown", serviceName),
                true
            );

            task.onRejection(rejected);
            return;
        }

        boolean added = queue.offer(task);
        if (added == false) {
            EsRejectedExecutionException rejected = new EsRejectedExecutionException(
                format("Failed to execute task because the http executor service [%s] queue is full", serviceName),
                false
            );

            task.onRejection(rejected);
        } else if (isShutdown()) {
            // It is possible that a shutdown and notification request occurred after we initially checked for shutdown above
            // If the task was added after the queue was already drained it could sit there indefinitely. So let's check again if
            // we shut down and if so we'll redo the notification
            notifyRequestsOfShutdown();
        }
    }

    /**
     * This method is not supported. Use {@link #send} instead.
     * @param runnable the runnable task
     */
    @Override
    public void execute(Runnable runnable) {
        throw new UnsupportedOperationException("use send instead");
    }

    /**
     * This method is not supported. Use {@link #send} instead.
     */
    @Override
    public <T> Future<T> submit(Callable<T> task) {
        throw new UnsupportedOperationException("use send instead");
    }

    /**
     * This method is not supported. Use {@link #send} instead.
     */
    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        throw new UnsupportedOperationException("use send instead");
    }

    /**
     * This method is not supported. Use {@link #send} instead.
     */
    @Override
    public Future<?> submit(Runnable task) {
        throw new UnsupportedOperationException("use send instead");
    }

    /**
     * This method is not supported. Use {@link #send} instead.
     */
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        throw new UnsupportedOperationException("use send instead");
    }

    /**
     * This method is not supported. Use {@link #send} instead.
     */
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException("use send instead");
    }

    /**
     * This method is not supported. Use {@link #send} instead.
     */
    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException("use send instead");
    }

    /**
     * This method is not supported. Use {@link #send} instead.
     */
    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException,
        ExecutionException, TimeoutException {
        throw new UnsupportedOperationException("use send instead");
    }
}
