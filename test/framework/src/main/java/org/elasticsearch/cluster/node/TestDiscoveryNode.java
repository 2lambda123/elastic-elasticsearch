/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.transport.TransportAddress;

import java.security.AccessControlException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import static org.apache.lucene.tests.util.LuceneTestCase.random;
import static org.elasticsearch.test.ESTestCase.buildNewFakeTransportAddress;

/**
 * Various shortcuts to constructors in {@link DiscoveryNode}.
 */
public class TestDiscoveryNode {

    public static DiscoveryNode create(String id) {
        return builder(id).build();
    }

    public static DiscoveryNode create(String name, String id) {
        return builder(id).name(name).build();
    }

    public static DiscoveryNode create(String id, TransportAddress address) {
        return builder(id).address(address).build();
    }

    public static DiscoveryNode create(String name, String id, TransportAddress address) {
        return builder(id).name(name).address(address).build();
    }

    public static DiscoveryNode create(String id, Version version) {
        return builder(id).version(version).build();
    }

    public static DiscoveryNode create(String id, TransportAddress address, Version version) {
        return builder(id).address(address).version(version).build();
    }

    public static DiscoveryNode create(String id, Map<String, String> attributes, Set<DiscoveryNodeRole> roles) {
        return builder(id).attributes(attributes).roles(roles).build();
    }

    public static DiscoveryNode create(String id, TransportAddress address, Map<String, String> attributes, Set<DiscoveryNodeRole> roles) {
        return builder(id).address(address).attributes(attributes).roles(roles).build();
    }

    public static DiscoveryNode create(String id, Map<String, String> attributes, Set<DiscoveryNodeRole> roles, Version version) {
        return builder(id).attributes(attributes).roles(roles).version(version).build();
    }

    public static DiscoveryNode create(
        String id,
        TransportAddress address,
        Map<String, String> attributes,
        Set<DiscoveryNodeRole> roles,
        Version version
    ) {
        return builder(id).address(address).attributes(attributes).roles(roles).version(version).build();
    }

    public static DiscoveryNode create(String nodeName, String nodeId, Map<String, String> attributes, Set<DiscoveryNodeRole> roles) {
        return builder(nodeId).name(nodeName).attributes(attributes).roles(roles).build();
    }

    public static DiscoveryNode create(
        String nodeName,
        String nodeId,
        TransportAddress address,
        Map<String, String> attributes,
        Set<DiscoveryNodeRole> roles
    ) {
        return builder(nodeId).name(nodeName).address(address).attributes(attributes).roles(roles).build();
    }

    public static DiscoveryNode create(
        String nodeName,
        String nodeId,
        TransportAddress address,
        Map<String, String> attributes,
        Set<DiscoveryNodeRole> roles,
        String externalId
    ) {
        return builder(nodeId).name(nodeName).address(address).attributes(attributes).roles(roles).externalId(externalId).build();
    }

    private static String newEphemeralId() {
        try {
            return UUIDs.randomBase64UUID(random());
        } catch (AccessControlException e) {
            // don't have SM permissions to access thread group context - probably part of a static init
            return UUID.randomUUID().toString();
        }
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    public static class Builder {
        private final String id;
        private String name;
        private String ephemeralId = newEphemeralId();
        private String hostName;
        private String hostAddress;
        private TransportAddress address;
        private Map<String, String> attributes = Map.of();
        private Set<DiscoveryNodeRole> roles = DiscoveryNodeRole.roles();
        private Version version;
        private String externalId;

        private Builder(String id) {
            this.id = Objects.requireNonNull(id);
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder ephemeralId(String ephemeralId) {
            this.ephemeralId = Objects.requireNonNull(ephemeralId);
            return this;
        }

        public Builder address(TransportAddress address) {
            return address(null, null, address);
        }

        public Builder address(String hostName, String hostAddress, TransportAddress address) {
            this.hostName = hostName;
            this.hostAddress = hostAddress;
            this.address = Objects.requireNonNull(address);
            return this;
        }

        public Builder attributes(Map<String, String> attributes) {
            this.attributes = Objects.requireNonNull(attributes);
            return this;
        }

        public Builder roles(Set<DiscoveryNodeRole> roles) {
            this.roles = Objects.requireNonNull(roles);
            return this;
        }

        public Builder version(Version version) {
            this.version = version;
            return this;
        }

        public Builder externalId(String externalId) {
            this.externalId = externalId;
            return this;
        }

        public DiscoveryNode build() {
            if (address == null) {
                address = buildNewFakeTransportAddress();
            }
            if (hostName == null) {
                hostName = address.address().getHostString();
            }
            if (hostAddress == null) {
                hostAddress = address.getAddress();
            }

            return new DiscoveryNode(name, id, ephemeralId, hostName, hostAddress, address, attributes, roles, version, externalId);
        }
    }
}
