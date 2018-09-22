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

import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.VotingConfiguration;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.coordination.CoordinationState.PersistedState;
import org.elasticsearch.cluster.coordination.CoordinationStateTests.InMemoryPersistedState;
import org.elasticsearch.cluster.coordination.CoordinatorTests.Cluster.ClusterNode;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNode.Role;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.zen.UnicastHostsProvider.HostsResolver;
import org.elasticsearch.indices.cluster.FakeThreadPoolMasterService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.disruption.DisruptableMockTransport;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.coordination.CoordinationStateTests.clusterState;
import static org.elasticsearch.cluster.coordination.CoordinationStateTests.setValue;
import static org.elasticsearch.cluster.coordination.CoordinationStateTests.value;
import static org.elasticsearch.cluster.coordination.Coordinator.Mode.FOLLOWER;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.transport.TransportService.HANDSHAKE_ACTION_NAME;
import static org.elasticsearch.transport.TransportService.NOOP_TRANSPORT_INTERCEPTOR;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@TestLogging("org.elasticsearch.cluster.coordination:TRACE,org.elasticsearch.cluster.discovery:TRACE")
public class CoordinatorTests extends ESTestCase {

    public void testCanUpdateClusterStateAfterStabilisation() {
        final Cluster cluster = new Cluster(randomIntBetween(1, 5));
        cluster.stabilise();

        final ClusterNode leader = cluster.getAnyLeader();
        long finalValue = randomLong();
        leader.submitValue(finalValue);
        cluster.stabilise(); // TODO this should only need a short stabilisation

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            final String nodeId = clusterNode.getId();
            final ClusterState committedState = clusterNode.coordinator.getLastCommittedState().get();
            assertThat(nodeId + " has the committed value", value(committedState), is(finalValue));
        }
    }

    private static String nodeIdFromIndex(int nodeIndex) {
        return "node" + nodeIndex;
    }

    class Cluster {

        static final long DEFAULT_STABILISATION_TIME = 3000L; // TODO use a real stabilisation time - needs fault detection and disruption

        final List<ClusterNode> clusterNodes;
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(
            Settings.builder().put(NODE_NAME_SETTING.getKey(), "deterministic-task-queue").build());
        private final VotingConfiguration initialConfiguration;

        Cluster(int initialNodeCount) {
            logger.info("--> creating cluster of {} nodes", initialNodeCount);

            Set<String> initialNodeIds = new HashSet<>(initialNodeCount);
            for (int i = 0; i < initialNodeCount; i++) {
                initialNodeIds.add(nodeIdFromIndex(i));
            }
            initialConfiguration = new VotingConfiguration(initialNodeIds);

            clusterNodes = new ArrayList<>(initialNodeCount);
            for (int i = 0; i < initialNodeCount; i++) {
                final ClusterNode clusterNode = new ClusterNode(i);
                clusterNodes.add(clusterNode);
            }
        }

        void stabilise() {
            final long stabilisationStartTime = deterministicTaskQueue.getCurrentTimeMillis();
            while (deterministicTaskQueue.getCurrentTimeMillis() < stabilisationStartTime + DEFAULT_STABILISATION_TIME) {

                while (deterministicTaskQueue.hasRunnableTasks()) {
                    try {
                        deterministicTaskQueue.runRandomTask(random());
                    } catch (CoordinationStateRejectedException e) {
                        logger.debug("ignoring benign exception thrown when stabilising", e);
                    }
                    for (final ClusterNode clusterNode : clusterNodes) {
                        clusterNode.coordinator.invariant();
                    }
                }

                if (deterministicTaskQueue.hasDeferredTasks() == false) {
                    break; // TODO when fault detection is enabled this should be removed, as there should _always_ be deferred tasks
                }

                deterministicTaskQueue.advanceTime();
            }

            assertUniqueLeaderAndExpectedModes();
        }

        private void assertUniqueLeaderAndExpectedModes() {
            final ClusterNode leader = getAnyLeader();
            final long leaderTerm = leader.coordinator.getCurrentTerm();
            Matcher<Optional<Long>> isPresentAndEqualToLeaderVersion
                = equalTo(Optional.of(leader.coordinator.getLastAcceptedState().getVersion()));

            assertThat(leader.coordinator.getLastCommittedState().map(ClusterState::getVersion), isPresentAndEqualToLeaderVersion);

            for (final ClusterNode clusterNode : clusterNodes) {
                if (clusterNode == leader) {
                    continue;
                }

                final String nodeId = clusterNode.getId();
                assertThat(nodeId + " has the same term as the leader", clusterNode.coordinator.getCurrentTerm(), is(leaderTerm));
                assertTrue("leader should have received a vote from " + nodeId,
                    leader.coordinator.hasJoinVoteFrom(clusterNode.getLocalNode()));

                assertThat(nodeId + " is a follower", clusterNode.coordinator.getMode(), is(FOLLOWER));
                assertThat(nodeId + " is at the same accepted version as the leader",
                    Optional.of(clusterNode.coordinator.getLastAcceptedState().getVersion()), isPresentAndEqualToLeaderVersion);
                assertThat(nodeId + " is at the same committed version as the leader",
                    clusterNode.coordinator.getLastCommittedState().map(ClusterState::getVersion), isPresentAndEqualToLeaderVersion);
            }
        }

        ClusterNode getAnyLeader() {
            List<ClusterNode> allLeaders = clusterNodes.stream().filter(ClusterNode::isLeader).collect(Collectors.toList());
            assertThat(allLeaders, not(empty()));
            return randomFrom(allLeaders);
        }

        class ClusterNode extends AbstractComponent {
            private final int nodeIndex;
            private Coordinator coordinator;
            private DiscoveryNode localNode;
            private final PersistedState persistedState;
            private MasterService masterService;
            private TransportService transportService;
            private DisruptableMockTransport mockTransport;

            ClusterNode(int nodeIndex) {
                super(Settings.builder().put(NODE_NAME_SETTING.getKey(), nodeIdFromIndex(nodeIndex)).build());
                this.nodeIndex = nodeIndex;
                localNode = createDiscoveryNode();
                persistedState = new InMemoryPersistedState(1L,
                    clusterState(1L, 1L, localNode, initialConfiguration, initialConfiguration, 0L));
                onNode(localNode, this::setUp).run();
            }

            private DiscoveryNode createDiscoveryNode() {
                final TransportAddress transportAddress = buildNewFakeTransportAddress();
                // Generate the ephemeral ID deterministically, for repeatable tests. This means we have to pass everything else into the
                // constructor explicitly too.
                return new DiscoveryNode("", nodeIdFromIndex(nodeIndex), UUIDs.randomBase64UUID(random()),
                    transportAddress.address().getHostString(),
                    transportAddress.getAddress(), transportAddress, Collections.emptyMap(),
                    EnumSet.allOf(Role.class), Version.CURRENT);
            }

            private void setUp() {
                mockTransport = new DisruptableMockTransport(logger) {
                    @Override
                    protected DiscoveryNode getLocalNode() {
                        return localNode;
                    }

                    @Override
                    protected ConnectionStatus getConnectionStatus(DiscoveryNode sender, DiscoveryNode destination) {
                        return ConnectionStatus.CONNECTED;
                    }

                    @Override
                    protected Optional<DisruptableMockTransport> getDisruptedCapturingTransport(DiscoveryNode node, String action) {
                        final Predicate<ClusterNode> matchesDestination;
                        if (action.equals(HANDSHAKE_ACTION_NAME)) {
                            matchesDestination = n -> n.getLocalNode().getAddress().equals(node.getAddress());
                        } else {
                            matchesDestination = n -> n.getLocalNode().equals(node);
                        }
                        return clusterNodes.stream().filter(matchesDestination).findAny().map(cn -> cn.mockTransport);
                    }

                    @Override
                    protected void handle(DiscoveryNode sender, DiscoveryNode destination, String action, Runnable doDelivery) {
                        // handshake needs to run inline as the caller blockingly waits on the result
                        if (action.equals(HANDSHAKE_ACTION_NAME)) {
                            onNode(destination, doDelivery).run();
                        } else {
                            deterministicTaskQueue.scheduleNow(onNode(destination, doDelivery));
                        }
                    }
                };

                masterService = new FakeThreadPoolMasterService("test",
                    runnable -> deterministicTaskQueue.scheduleNow(onNode(localNode, runnable)));
                transportService = mockTransport.createTransportService(
                    settings, deterministicTaskQueue.getThreadPool(runnable -> onNode(localNode, runnable)), NOOP_TRANSPORT_INTERCEPTOR,
                    a -> localNode, null, emptySet());
                coordinator = new Coordinator(settings, transportService, ESAllocationTestCase.createAllocationService(Settings.EMPTY),
                    masterService, this::getPersistedState, Cluster.this::provideUnicastHosts, Randomness.get());
                masterService.setClusterStatePublisher(coordinator);

                transportService.start();
                transportService.acceptIncomingRequests();
                masterService.start();
                coordinator.start();
                coordinator.startInitialJoin();
            }

            private PersistedState getPersistedState() {
                return persistedState;
            }

            String getId() {
                return localNode.getId();
            }

            public DiscoveryNode getLocalNode() {
                return localNode;
            }

            boolean isLeader() {
                return coordinator.getMode() == Coordinator.Mode.LEADER;
            }

            void submitValue(final long value) {
                masterService.submitStateUpdateTask("new value [" + value + "]", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return setValue(currentState, value);
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.debug(() -> new ParameterizedMessage("failed to publish: [{}]", source), e);
                    }
                });
            }
        }

        private List<TransportAddress> provideUnicastHosts(HostsResolver ignored) {
            return clusterNodes.stream().map(ClusterNode::getLocalNode).map(DiscoveryNode::getAddress).collect(Collectors.toList());
        }
    }

    private static Runnable onNode(DiscoveryNode node, Runnable runnable) {
        final String nodeId = "{" + node.getId() + "}{" + node.getEphemeralId() + "}";
        return new Runnable() {
            @Override
            public void run() {
                try (CloseableThreadContext.Instance ignored = CloseableThreadContext.put("nodeId", nodeId)) {
                    runnable.run();
                }
            }

            @Override
            public String toString() {
                return nodeId + ": " + runnable.toString();
            }
        };
    }
}
