/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.plugins.Platforms;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

public class MlPlatformArchitecturesUtil {

    public static ActionListener<NodesInfoResponse> getArchitecturesSetFromNodesInfoResponseListener(
        ThreadPool threadPool,
        ActionListener<Set<String>> architecturesListener
    ) {
        return ActionListener.wrap(nodesInfoResponse -> {
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
                architecturesListener.onResponse(getArchitecturesSetFromNodesInfoResponse(nodesInfoResponse));
            });
        }, architecturesListener::onFailure);
    }

    public static NodesInfoRequestBuilder getNodesInfoBuilderWithMlNodeArchitectureInfo(Client client) {
        return client.admin().cluster().prepareNodesInfo().clear().setNodesIds("ml:true").setOs(true).setPlugins(true);
    }

    private static Set<String> getArchitecturesSetFromNodesInfoResponse(NodesInfoResponse nodesInfoResponse) {
        return nodesInfoResponse.getNodes()
            .stream()
            .filter(node -> node.getNode().hasRole(DiscoveryNodeRole.ML_ROLE.roleName()))
            .map(node -> {
                OsInfo osInfo = node.getInfo(OsInfo.class);
                return Platforms.platformName(osInfo.getName(), osInfo.getArch());
            })
            .collect(Collectors.toUnmodifiableSet());
    }

    public static void verifyMlNodesAndModelArchitectures(Set<String> architectures, String modelPlatformArchitecture, String modelID)
        throws IllegalArgumentException, IllegalStateException {

        String architecture = null;
        Iterator<String> architecturesIterator = architectures.iterator();

        if (architecturesIterator.hasNext()) {
            architecture = architectures.iterator().next();

            String architecturesStr = architectures.toString();
            architecturesStr = architecturesStr.substring(1, architecturesStr.length() - 1); // Remove the brackets

            if (Objects.isNull(modelPlatformArchitecture) == false) { // null value indicates platform agnostic, so these errors are
                                                                      // irrelevant
                if (architectures.size() > 1) { // Platform architectures are not homogeneous among ML nodes

                    throw new IllegalStateException(
                        format(
                            "ML nodes in this cluster have multiple platform architectures, but can only have one for this model ([%s]); "
                                + "expected [%s]; "
                                + "but was [%s]",
                            modelID,
                            modelPlatformArchitecture,
                            architecturesStr
                        )
                    );

                } else if (Objects.equals(architecture, modelPlatformArchitecture) == false) {

                    throw new IllegalArgumentException(
                        format(
                            "The model being deployed ([%s]) is platform specific and incompatible with ML nodes in the cluster; "
                                + "expected [%s]; "
                                + "but was [%s]",
                            modelID,
                            modelPlatformArchitecture,
                            architecturesStr
                        )
                    );
                }
            }
        }
    }
}
