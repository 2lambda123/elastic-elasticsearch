package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.Locale;

/**
 */
public class EnableAllocationDecider extends AllocationDecider implements NodeSettingsService.Listener {

    public static final String CLUSTER_ROUTING_ALLOCATION_ENABLE = "cluster.routing.allocation.enable";
    public static final String INDEX_ROUTING_ALLOCATION_ENABLE = "index.routing.allocation.enable";

    private volatile Allocation enable;

    @Inject
    public EnableAllocationDecider(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        this.enable = Allocation.parse(settings.get(CLUSTER_ROUTING_ALLOCATION_ENABLE, Allocation.ALL.name()));
        nodeSettingsService.addListener(this);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (allocation.ignoreDisable()) {
            return Decision.YES;
        }

        Settings indexSettings = allocation.routingNodes().metaData().index(shardRouting.index()).settings();
        String enableIndexValue = indexSettings.get(INDEX_ROUTING_ALLOCATION_ENABLE);
        final Allocation enable;
        if (enableIndexValue != null) {
            enable = Allocation.parse(enableIndexValue);
        } else {
            enable = this.enable;
        }
        switch (enable) {
            case ALL:
                return Decision.YES;
            case NONE:
                return Decision.NO;
            case NEW_PRIMARIES:
                if (shardRouting.primary() && !allocation.routingNodes().routingTable().index(shardRouting.index()).shard(shardRouting.id()).primaryAllocatedPostApi()) {
                    return Decision.YES;
                } else {
                    return Decision.NO;
                }
            case PRIMARIES:
                return shardRouting.primary() ? Decision.YES : Decision.NO;
            default:
                throw new ElasticSearchIllegalStateException("Unknown allocation enable option");
        }
    }

    @Override
    public void onRefreshSettings(Settings settings) {
        Allocation enable = Allocation.parse(settings.get(CLUSTER_ROUTING_ALLOCATION_ENABLE, this.enable.name()));
        if (enable != this.enable) {
            logger.info("updating [cluster.routing.allocation.enable] from [{}] to [{}]", this.enable, enable);
            EnableAllocationDecider.this.enable = enable;
        }
    }

    public enum Allocation {

        NONE,
        NEW_PRIMARIES,
        PRIMARIES,
        ALL;

        public static Allocation parse(String strValue) {
            if (strValue == null) {
                return null;
            } else {
                strValue = strValue.toUpperCase(Locale.ROOT);
                return Allocation.valueOf(strValue);
            }
        }
    }

}
