/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;

public class PendingListenersQueue {

    private record PendingListener(long index, ActionListener<Void> listener) {}

    private final ThreadPool threadPool;
    private final Queue<PendingListener> pendingListeners = new LinkedList<>();
    private volatile long completedIndex = -1;
    private volatile boolean paused = false;

    public PendingListenersQueue(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    public void add(long index, ActionListener<Void> listener) {
        synchronized (pendingListeners) {
            pendingListeners.add(new PendingListener(index, listener));
        }
    }

    public void complete(long index) {
        advance(index);
        if (paused == false) {
            executeListeners(completedIndex, true);
        }
    }

    public void completeAllAsNotMaster() {
        completedIndex = -1;
        paused = false;
        executeListeners(Long.MAX_VALUE, false);
    }

    public void pause() {
        paused = true;
    }

    public void resume() {
        paused = false;
        executeListeners(completedIndex, true);
    }

    public long getCompletedIndex() {
        return completedIndex;
    }

    public boolean isPaused() {
        return paused;
    }

    private void executeListeners(long convergedIndex, boolean isMaster) {
        var listeners = pollListeners(convergedIndex);
        if (listeners.isEmpty() == false) {
            threadPool.generic().execute(() -> {
                if (isMaster) {
                    ActionListener.onResponse(listeners, null);
                } else {
                    ActionListener.onFailure(listeners, new NotMasterException("no longer master"));
                }
            });
        }
    }

    private void advance(long index) {
        synchronized (pendingListeners) {
            if (index > completedIndex) {
                completedIndex = index;
            }
        }
    }

    private Collection<ActionListener<Void>> pollListeners(long maxIndex) {
        var listeners = new ArrayList<ActionListener<Void>>();
        PendingListener listener;
        synchronized (pendingListeners) {
            while ((listener = pendingListeners.peek()) != null && listener.index <= maxIndex) {
                listeners.add(pendingListeners.poll().listener);
            }
        }
        return listeners;
    }
}
