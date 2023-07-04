/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.utils.TransportVersionUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class TransportVersionUtilsTests extends ESTestCase {

    private static Map<String, TransportVersion> transportVersions;
    static {
        transportVersions = new HashMap<>();
        transportVersions.put("Alfredo", TransportVersion.V_7_0_0);
        transportVersions.put("Bertram", TransportVersion.V_7_0_1);
        transportVersions.put("Charles", TransportVersion.V_8_500_003);
        transportVersions.put("Dominic", TransportVersion.V_8_0_0);
    }

    private static ClusterState state = new ClusterState(
        new ClusterName("fred"),
        0L,
        "EC7C0637-1644-43AB-AEAB-D8B7970CAECA",
        null,
        null,
        null,
        transportVersions,
        null,
        null,
        false,
        null
    );

    public void testGetMinTransportVersion() {
        assertThat(TransportVersionUtils.getMinTransportVersion(state), equalTo(TransportVersion.V_7_0_0));
    }

    public void testGetMaxTransportVersion() {
        assertThat(TransportVersionUtils.getMaxTransportVersion(state), equalTo(TransportVersion.V_8_500_003));

        Map<String, TransportVersion> transportVersions1 = Collections.emptyMap();

        ClusterState state1 = new ClusterState(
            new ClusterName("george"),
            0L,
            "E3D1B079-9EA2-47B1-84F9-730038B25043",
            null,
            null,
            null,
            transportVersions1,
            null,
            null,
            false,
            null
        );

        assertThat(TransportVersionUtils.getMaxTransportVersion(state1), equalTo(TransportVersion.current()));
    }

    public void testAreAllTransformVersionsTheSame() {
        assertThat(TransportVersionUtils.areAllTransformVersionsTheSame(state), equalTo(false));

        // mutate the versions so that they are al the same
        transportVersions.replaceAll((k, v) -> v = TransportVersion.V_7_0_0);

        state = new ClusterState(
            new ClusterName("harry"),
            0L,
            "20F833F2-7C48-4522-BA78-6821C9DCD5D8",
            null,
            null,
            null,
            transportVersions,
            null,
            null,
            false,
            null
        );

        assertThat(TransportVersionUtils.areAllTransformVersionsTheSame(state), equalTo(true));
    }
}
