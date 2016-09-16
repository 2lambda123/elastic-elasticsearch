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

package org.elasticsearch.common.network;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.http.HttpServerAdapter;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.cat.AbstractCatAction;
import org.elasticsearch.test.transport.AssertingLocalTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;

public class NetworkModuleTests extends ModuleTestCase {

    static class FakeTransport extends AssertingLocalTransport {
        public FakeTransport() {
            super(null, null, null, null);
        }
    }

    static class FakeTransportFactory extends NetworkPlugin.TransportFactory<Transport> {

        public FakeTransportFactory(String name) {
            super(name);
        }

        @Override
        public FakeTransport createTransport(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                                             CircuitBreakerService circuitBreakerService, NamedWriteableRegistry namedWriteableRegistry,
                                             NetworkService networkService) {
            return new FakeTransport();
        }
    }

    static class FakeHttpTransport extends AbstractLifecycleComponent implements HttpServerTransport {
        public FakeHttpTransport() {
            super(null);
        }
        @Override
        protected void doStart() {}
        @Override
        protected void doStop() {}
        @Override
        protected void doClose() {}
        @Override
        public BoundTransportAddress boundAddress() {
            return null;
        }
        @Override
        public HttpInfo info() {
            return null;
        }
        @Override
        public HttpStats stats() {
            return null;
        }
        @Override
        public void httpServerAdapter(HttpServerAdapter httpServerAdapter) {}
    }

    static class FakeHttpTransportFactory extends NetworkPlugin.TransportFactory<HttpServerTransport> {

        public FakeHttpTransportFactory(String name) {
            super(name);
        }

        @Override
        public FakeHttpTransport createTransport(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                                             CircuitBreakerService circuitBreakerService, NamedWriteableRegistry namedWriteableRegistry,
                                             NetworkService networkService) {
            return new FakeHttpTransport();
        }
    }

    static class FakeRestHandler extends BaseRestHandler {
        public FakeRestHandler() {
            super(null);
        }
        @Override
        public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {}
    }

    static class FakeCatRestHandler extends AbstractCatAction {
        public FakeCatRestHandler() {
            super(null);
        }
        @Override
        protected void doRequest(RestRequest request, RestChannel channel, NodeClient client) {}
        @Override
        protected void documentation(StringBuilder sb) {}
        @Override
        protected Table getTableWithHeader(RestRequest request) {
            return null;
        }
    }

    public void testRegisterTransport() {
        Settings settings = Settings.builder().put(NetworkModule.TRANSPORT_TYPE_KEY, "custom")
            .put(NetworkModule.HTTP_ENABLED.getKey(), false)
            .build();
        NetworkModule module = new NetworkModule(settings, false);
        FakeTransportFactory custom = new FakeTransportFactory("custom");
        module.registerTransport(custom);
        assertFalse(module.isTransportClient());
        assertFalse(module.isHttpEnabled());
        assertSame(custom, module.getTransportFactory());

        // check it works with transport only as well
        module = new NetworkModule(settings, true);
        module.registerTransport(custom);
        assertSame(custom, module.getTransportFactory());
        assertTrue(module.isTransportClient());
        assertFalse(module.isHttpEnabled());
    }

    public void testRegisterHttpTransport() {
        Settings settings = Settings.builder()
            .put(NetworkModule.HTTP_TYPE_SETTING.getKey(), "custom")
            .put(NetworkModule.TRANSPORT_TYPE_KEY, "local").build();
        NetworkModule module = new NetworkModule(settings, false);
        FakeHttpTransportFactory custom = new FakeHttpTransportFactory("custom");
        module.registerHttpTransport(custom);
        assertSame(custom, module.getHttpServerTransportFactory());
        assertFalse(module.isTransportClient());
        assertTrue(module.isHttpEnabled());

        // check registration not allowed for transport only
        module = new NetworkModule(settings, true);
        assertTrue(module.isTransportClient());
        try {
            module.registerHttpTransport(custom);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Cannot register http transport"));
            assertTrue(e.getMessage().contains("for transport client"));
        }

        settings = Settings.builder().put(NetworkModule.HTTP_ENABLED.getKey(), false)
            .put(NetworkModule.TRANSPORT_TYPE_KEY, "local").build();
        NetworkModule newModule = new NetworkModule(settings, false);
        assertFalse(newModule.isTransportClient());
        assertFalse(newModule.isHttpEnabled());
        expectThrows(IllegalStateException.class, () -> newModule.getHttpServerTransportFactory());
    }

    public void testOverrideDefault() {
        Settings settings = Settings.builder()
            .put(NetworkModule.HTTP_TYPE_SETTING.getKey(), "custom")
            .put(NetworkModule.HTTP_DEFAULT_TYPE_SETTING.getKey(), "default_custom")
            .put(NetworkModule.TRANSPORT_DEFAULT_TYPE_SETTING.getKey(), "local")
            .put(NetworkModule.TRANSPORT_TYPE_KEY, "default_custom").build();
        NetworkModule module = new NetworkModule(settings, false);
        FakeHttpTransportFactory custom = new FakeHttpTransportFactory("custom");
        FakeHttpTransportFactory def = new FakeHttpTransportFactory("default_custom");
        FakeTransportFactory customTransport = new FakeTransportFactory("default_custom");
        module.registerHttpTransport(custom);
        module.registerHttpTransport(def);
        module.registerTransport(customTransport);
        assertSame(custom, module.getHttpServerTransportFactory());
        assertSame(customTransport, module.getTransportFactory());
    }

    public void testDefaultKeys() {
        Settings settings = Settings.builder()
            .put(NetworkModule.HTTP_DEFAULT_TYPE_SETTING.getKey(), "default_custom")
            .put(NetworkModule.TRANSPORT_DEFAULT_TYPE_SETTING.getKey(), "default_custom").build();
        NetworkModule module = new NetworkModule(settings, false);
        FakeHttpTransportFactory custom = new FakeHttpTransportFactory("custom");
        FakeHttpTransportFactory def = new FakeHttpTransportFactory("default_custom");
        FakeTransportFactory customTransport = new FakeTransportFactory("default_custom");
        module.registerHttpTransport(custom);
        module.registerHttpTransport(def);
        module.registerTransport(customTransport);
        assertSame(def, module.getHttpServerTransportFactory());
        assertSame(customTransport, module.getTransportFactory());
    }

    public void testRegisterInterceptor() {
        Settings settings = Settings.builder()
            .put(NetworkModule.HTTP_ENABLED.getKey(), false)
            .put(NetworkModule.TRANSPORT_TYPE_KEY, "local").build();

        NetworkModule module = new NetworkModule(settings, false);

        TransportInterceptor interceptor = new TransportInterceptor() {
        };
        module.registerTransportInterceptor(interceptor);
        TransportInterceptor transportInterceptor = module.getTransportInterceptor();
        assertTrue(transportInterceptor instanceof  NetworkModule.CompositeTransportInterceptor);
        assertEquals(((NetworkModule.CompositeTransportInterceptor)transportInterceptor).transportInterceptors.size(), 1);
        assertSame(((NetworkModule.CompositeTransportInterceptor)transportInterceptor).transportInterceptors.get(0), interceptor);

        NullPointerException nullPointerException = expectThrows(NullPointerException.class, () -> {
            module.registerTransportInterceptor(null);
        });
        assertEquals("interceptor must not be null", nullPointerException.getMessage());

    }
}
