package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.test.AbstractWireTestCase;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class ShardAllocationDecisionTest extends AbstractWireTestCase<ShardAllocationDecision> {

    @Override
    protected ShardAllocationDecision createTestInstance() {
        return new ShardAllocationDecision(
            AllocateUnassignedDecision.no(
                randomFrom(
                    UnassignedInfo.AllocationStatus.DECIDERS_NO,
                    UnassignedInfo.AllocationStatus.DELAYED_ALLOCATION,
                    UnassignedInfo.AllocationStatus.NO_VALID_SHARD_COPY,
                    UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA
                ),
                List.of(),
                randomBoolean()
            ),
            MoveDecision.cannotRemain(
                Decision.NO,
                AllocationDecision.NO,
                null,
                List.of(
                    new NodeAllocationResult(
                        new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
                        randomFrom(Decision.NO, Decision.THROTTLE, Decision.YES),
                        1
                    )
                )
            )
        );
    }

    @Override
    protected ShardAllocationDecision copyInstance(ShardAllocationDecision instance, Version version) throws IOException {
        return new ShardAllocationDecision(instance.getAllocateDecision(), instance.getMoveDecision());
    }
}
