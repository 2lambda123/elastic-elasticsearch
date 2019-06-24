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

package org.elasticsearch.nio;

import java.net.InetSocketAddress;

public class SocketConfig {

    private final boolean tcpNoDelay;
    private final boolean tcpKeepAlive;
    private final boolean tcpReuseAddress;
    private final int tcpSendBufferSize;
    private final int tcpReceiveBufferSize;
    private final InetSocketAddress localAddress;
    private final InetSocketAddress remoteAddress;

    public SocketConfig(boolean tcpNoDelay, boolean tcpKeepAlive, boolean tcpReuseAddress, int tcpSendBufferSize,
                        int tcpReceiveBufferSize, InetSocketAddress localAddress, InetSocketAddress remoteAddress) {
        this.tcpNoDelay = tcpNoDelay;
        this.tcpKeepAlive = tcpKeepAlive;
        this.tcpReuseAddress = tcpReuseAddress;
        this.tcpSendBufferSize = tcpSendBufferSize;
        this.tcpReceiveBufferSize = tcpReceiveBufferSize;
        this.localAddress = localAddress;
        this.remoteAddress = remoteAddress;
    }

    public boolean tcpNoDelay() {
        return tcpNoDelay;
    }

    public boolean tcpKeepAlive() {
        return tcpKeepAlive;
    }

    public boolean tcpReuseAddress() {
        return tcpReuseAddress;
    }

    public int tcpSendBufferSize() {
        return tcpSendBufferSize;
    }

    public int tcpReceiveBufferSize() {
        return tcpReceiveBufferSize;
    }

    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }
}
