package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.test.ESTestCase.buildNewFakeTransportAddress;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomFrom;

class NodeDecisionTestUtils {

    static NodeDecision randomNodeDecision() {
        return new NodeDecision(
            new DiscoveryNode(randomAlphaOfLength(6), buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            randomFrom(
                new Decision.Single(Decision.Type.NO, DiskThresholdDecider.NAME, "Unable to allocate on disk"),
                new Decision.Single(Decision.Type.YES, FilterAllocationDecider.NAME, "Filter allows allocation"),
                new Decision.Single(Decision.Type.THROTTLE, "throttle label", "Throttling the consumer"),
                new Decision.Multi().add(randomFrom(Decision.NO, Decision.YES, Decision.THROTTLE))
                    .add(new Decision.Single(Decision.Type.NO, "multi_no", "No multi decision"))
                    .add(new Decision.Single(Decision.Type.YES, "multi_yes", "Yes multi decision"))
            )
        );
    }
}
