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

package org.elasticsearch.discovery.file;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class FileBasedDiscoveryPluginTests extends ESTestCase {

    public void testHostsProviderBwc() throws IOException {
        FileBasedDiscoveryPlugin plugin = new FileBasedDiscoveryPlugin(Settings.EMPTY);
        Settings additionalSettings = plugin.additionalSettings();
        assertEquals("file", additionalSettings.get(DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING.getKey()));
        assertWarnings("Using discovery.type setting to set hosts provider is deprecated. " +
                "Set \"discovery.zen.hosts_provider: file\" instead");
    }

    public void testHostsProviderExplicit() throws IOException {
        Settings settings = Settings.builder().put(DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING.getKey(), "foo").build();
        FileBasedDiscoveryPlugin plugin = new FileBasedDiscoveryPlugin(settings);
        assertEquals(Settings.EMPTY, plugin.additionalSettings());
        assertWarnings("Using discovery.type setting to set hosts provider is deprecated. " +
                "Set \"discovery.zen.hosts_provider: file\" instead");
    }
}