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

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.coordination.Coordinator.Mode;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportRequestOptions.Type;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * The FollowersChecker is responsible for allowing a leader to check that its followers are still connected and healthy. On deciding that a
 * follower has failed the leader will remove it from the cluster. We are fairly lenient, possibly allowing multiple checks to fail before
 * considering a follower to be faulty, to allow for a brief network partition or a long GC cycle to occur without triggering the removal of
 * a node and the consequent shard reallocation.
 */
public class FollowersChecker extends AbstractComponent {

    public static final String FOLLOWER_CHECK_ACTION_NAME = "internal:coordination/fault_detection/follower_check";

    // the time between checks sent to each node
    public static final Setting<TimeValue> FOLLOWER_CHECK_INTERVAL_SETTING =
        Setting.timeSetting("cluster.fault_detection.follower_check.interval",
            TimeValue.timeValueMillis(1000), TimeValue.timeValueMillis(100), Setting.Property.NodeScope);

    // the timeout for each check sent to each node
    public static final Setting<TimeValue> FOLLOWER_CHECK_TIMEOUT_SETTING =
        Setting.timeSetting("cluster.fault_detection.follower_check.timeout",
            TimeValue.timeValueMillis(30000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    // the number of failed checks that must happen before the follower is considered to have failed.
    public static final Setting<Integer> FOLLOWER_CHECK_RETRY_COUNT_SETTING =
        Setting.intSetting("cluster.fault_detection.follower_check.retry_count", 3, 1, Setting.Property.NodeScope);

    private final TimeValue followerCheckInterval;
    private final TimeValue followerCheckTimeout;
    private final int followerCheckRetryCount;
    private final Consumer<DiscoveryNode> onNodeFailure;
    private final Consumer<FollowerCheckRequest> handleRequestAndUpdateState;

    private final Object mutex = new Object();
    private final Map<DiscoveryNode, FollowerChecker> followerCheckers = new HashMap<>();
    private final Set<DiscoveryNode> faultyNodes = new HashSet<>();

    private final TransportService transportService;

    private volatile Responder responder;

    public FollowersChecker(Settings settings, TransportService transportService,
                            Consumer<FollowerCheckRequest> handleRequestAndUpdateState,
                            Consumer<DiscoveryNode> onNodeFailure) {
        super(settings);
        this.transportService = transportService;
        this.handleRequestAndUpdateState = handleRequestAndUpdateState;
        this.onNodeFailure = onNodeFailure;

        followerCheckInterval = FOLLOWER_CHECK_INTERVAL_SETTING.get(settings);
        followerCheckTimeout = FOLLOWER_CHECK_TIMEOUT_SETTING.get(settings);
        followerCheckRetryCount = FOLLOWER_CHECK_RETRY_COUNT_SETTING.get(settings);

        updateResponder(0, Mode.CANDIDATE);
        transportService.registerRequestHandler(FOLLOWER_CHECK_ACTION_NAME, Names.SAME, FollowerCheckRequest::new,
            (request, transportChannel, task) -> responder.handleFollowerCheck(request, transportChannel));
    }

    /**
     * Update the set of known nodes, starting to check any new ones and stopping checking any previously-known-but-now-unknown ones.
     */
    public void setCurrentNodes(DiscoveryNodes discoveryNodes) {
        synchronized (mutex) {
            final Predicate<DiscoveryNode> isUnknownNode = n -> discoveryNodes.nodeExists(n) == false;
            followerCheckers.keySet().removeIf(isUnknownNode);
            faultyNodes.removeIf(isUnknownNode);

            for (final DiscoveryNode discoveryNode : discoveryNodes) {
                if (discoveryNode.equals(discoveryNodes.getLocalNode()) == false
                    && followerCheckers.containsKey(discoveryNode) == false
                    && faultyNodes.contains(discoveryNode) == false) {

                    final FollowerChecker followerChecker = new FollowerChecker(discoveryNode);
                    followerCheckers.put(discoveryNode, followerChecker);
                    followerChecker.start();
                }
            }
        }
    }

    /**
     * The system is normally in a state in which every follower remains a follower of a stable leader in a single term for an extended
     * period of time, and therefore our response to every follower check is the same. We handle this case with a single volatile read
     * entirely on the network thread, and only if the fast path fails do we perform some work in the background, by notifying the
     * FollowersChecker whenever our term or mode changes here.
     */
    public void updateResponder(final long term, final Mode mode) {
        responder = new Responder(logger, term, mode, transportService.getThreadPool().generic()::execute, handleRequestAndUpdateState);
    }

    // TODO in the PoC a faulty node was considered non-faulty again if it sent us a PeersRequest:
    // - node disconnects, detected faulty, removal is enqueued
    // - node reconnects, pings us, finds we are master, requests to join, all before removal is applied
    // - join is processed before removal, but we do not publish to known-faulty nodes so the joining node does not receive this publication
    // - it doesn't start its leader checker since it receives nothing to cause it to become a follower
    // Apparently this meant that it remained a candidate for too long, leading to a test failure.  At the time this logic was added, we did
    // not have gossip-based discovery which would (I think) have retried this joining process a short time later. It's therefore possible
    // that this is no longer required, so it's omitted here until we can be sure if it's necessary or not.

    /**
     * @return nodes in the current cluster state which have failed their follower checks.
     */
    public Set<DiscoveryNode> getFaultyNodes() {
        synchronized (mutex) {
            return new HashSet<>(this.faultyNodes);
        }
    }

    @Override
    public String toString() {
        return "FollowersChecker{" +
            "followerCheckInterval=" + followerCheckInterval +
            ", followerCheckTimeout=" + followerCheckTimeout +
            ", followerCheckRetryCount=" + followerCheckRetryCount +
            ", followerCheckers=" + followerCheckers +
            ", faultyNodes=" + faultyNodes +
            ", responder=" + responder +
            '}';
    }

    static class Responder {
        // static immutable class to be sure that it's usable without locks; exposed for testing and assertions

        private final Logger logger;
        private final long term;
        private final Mode mode;
        private final Consumer<Runnable> executeRunnable;
        private final Consumer<FollowerCheckRequest> handleRequestAndUpdateState;

        Responder(final Logger logger, final long term, final Mode mode, final Consumer<Runnable> executeRunnable,
                  final Consumer<FollowerCheckRequest> handleRequestAndUpdateState) {
            this.logger = logger;
            this.term = term;
            this.mode = mode;
            this.executeRunnable = executeRunnable;
            this.handleRequestAndUpdateState = handleRequestAndUpdateState;
        }

        long getTerm() {
            return term;
        }

        @Override
        public String toString() {
            return "Responder{" +
                "term=" + term +
                ", mode=" + mode +
                '}';
        }

        void handleFollowerCheck(final FollowerCheckRequest request, final TransportChannel transportChannel) throws IOException {
            if (this.mode == Mode.FOLLOWER && this.term == request.term) {
                // TODO trigger a term bump if we voted for a different leader in this term
                logger.trace("responding to {} on fast path", request);
                transportChannel.sendResponse(Empty.INSTANCE);
                return;
            }

            if (request.term < this.term) {
                throw new CoordinationStateRejectedException(new ParameterizedMessage("rejecting {} since local state is {}",
                    request, this).getFormattedMessage());
            }

            executeRunnable.accept(new AbstractRunnable() {
                @Override
                protected void doRun() throws IOException {
                    logger.trace("responding to {} on slow path", request);
                    handleRequestAndUpdateState.accept(request);
                    transportChannel.sendResponse(Empty.INSTANCE);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug(new ParameterizedMessage("exception while responding to {}", request), e);
                }

                @Override
                public String toString() {
                    return "calculating response to " + request;
                }
            });
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Responder responder = (Responder) o;
            return term == responder.term &&
                mode == responder.mode;
        }

        @Override
        public int hashCode() {
            return Objects.hash(term, mode);
        }
    }

    /**
     * A checker for an individual follower.
     */
    private class FollowerChecker {
        private final DiscoveryNode discoveryNode;
        private int failureCountSinceLastSuccess;

        FollowerChecker(DiscoveryNode discoveryNode) {
            this.discoveryNode = discoveryNode;
        }

        private boolean running() {
            assert Thread.holdsLock(mutex) : "FollowersChecker mutex not held";
            return this == followerCheckers.get(discoveryNode);
        }

        void start() {
            assert running();
            handleWakeUp();
        }

        private void handleWakeUp() {
            synchronized (mutex) {
                if (running() == false) {
                    logger.trace("handleWakeUp: not running");
                    return;
                }

                final FollowerCheckRequest request = new FollowerCheckRequest(responder.getTerm());
                logger.trace("handleWakeUp: checking {} with {}", discoveryNode, request);
                transportService.sendRequest(discoveryNode, FOLLOWER_CHECK_ACTION_NAME, request,
                    TransportRequestOptions.builder().withTimeout(followerCheckTimeout).withType(Type.PING).build(),

                    new TransportResponseHandler<TransportResponse.Empty>() {

                        @Override
                        public void handleResponse(TransportResponse.Empty response) {
                            synchronized (mutex) {
                                if (running() == false) {
                                    logger.trace("{} no longer running", FollowerChecker.this);
                                    return;
                                }

                                failureCountSinceLastSuccess = 0;
                                logger.trace("{} check successful", FollowerChecker.this);
                                scheduleNextWakeUp();
                            }
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            synchronized (mutex) {
                                if (running() == false) {
                                    logger.debug(new ParameterizedMessage("{} no longer running", FollowerChecker.this), exp);
                                    return;
                                }

                                failureCountSinceLastSuccess++;

                                if (failureCountSinceLastSuccess >= followerCheckRetryCount) {
                                    logger.debug(() -> new ParameterizedMessage("{} failed too many times", FollowerChecker.this), exp);
                                } else if (exp instanceof ConnectTransportException
                                    || exp.getCause() instanceof ConnectTransportException) {
                                    logger.debug(() -> new ParameterizedMessage("{} disconnected", FollowerChecker.this), exp);
                                } else {
                                    logger.debug(() -> new ParameterizedMessage("{} failed, retrying", FollowerChecker.this), exp);
                                    scheduleNextWakeUp();
                                    return;
                                }

                                faultyNodes.add(discoveryNode);
                                followerCheckers.remove(discoveryNode);

                                transportService.getThreadPool().generic().execute(new Runnable() {
                                    @Override
                                    public void run() {
                                        onNodeFailure.accept(discoveryNode);
                                    }

                                    @Override
                                    public String toString() {
                                        return "detected failure of " + discoveryNode;
                                    }
                                });
                            }
                        }

                        @Override
                        public String executor() {
                            return Names.SAME;
                        }
                    });
            }
        }

        private void scheduleNextWakeUp() {
            transportService.getThreadPool().schedule(followerCheckInterval, Names.SAME, new Runnable() {
                @Override
                public void run() {
                    handleWakeUp();
                }

                @Override
                public String toString() {
                    return FollowerChecker.this + "::handleWakeUp";
                }
            });
        }

        @Override
        public String toString() {
            return "FollowerChecker{" +
                "discoveryNode=" + discoveryNode +
                ", failureCountSinceLastSuccess=" + failureCountSinceLastSuccess +
                ", [" + FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey() + "]=" + followerCheckRetryCount +
                '}';
        }
    }

    public static class FollowerCheckRequest extends TransportRequest {

        private final long term;

        public long getTerm() {
            return term;
        }

        public FollowerCheckRequest(final long term) {
            this.term = term;
        }

        public FollowerCheckRequest(final StreamInput in) throws IOException {
            super(in);
            term = in.readLong();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(term);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FollowerCheckRequest that = (FollowerCheckRequest) o;
            return term == that.term;
        }

        @Override
        public String toString() {
            return "FollowerCheckRequest{" +
                "term=" + term +
                '}';
        }

        @Override
        public int hashCode() {
            return Objects.hash(term);
        }
    }
}
