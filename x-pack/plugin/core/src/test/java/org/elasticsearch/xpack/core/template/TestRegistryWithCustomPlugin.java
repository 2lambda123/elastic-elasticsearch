/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.template;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class TestRegistryWithCustomPlugin extends IndexTemplateRegistry {

    public static final int REGISTRY_VERSION = 3;
    public static final String TEMPLATE_VERSION_VARIABLE = "xpack.custom_plugin.template.version";

    private boolean policyUpgradeRequired = false;

    TestRegistryWithCustomPlugin(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
    }

    @Override
    protected Map<String, ComponentTemplate> getComponentTemplateConfigs() {
        String settingsConfigName = "custom-plugin-settings";
        IndexTemplateConfig config = new IndexTemplateConfig(
            settingsConfigName,
            "/org/elasticsearch/xpack/core/template/custom-plugin-settings.json",
            REGISTRY_VERSION,
            TEMPLATE_VERSION_VARIABLE
        );
        ComponentTemplate componentTemplate = null;
        try {
            componentTemplate = ComponentTemplate.parse(
                JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, config.loadBytes())
            );
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return Map.of(settingsConfigName, componentTemplate);
    }

    @Override
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        return IndexTemplateRegistry.parseComposableTemplates(
            new IndexTemplateConfig(
                "custom-plugin-template",
                "/org/elasticsearch/xpack/core/template/custom-plugin-template.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            )
        );
    }

    @Override
    protected List<IngestPipelineConfig> getIngestPipelines() {
        return List.of(
            new JsonIngestPipelineConfig(
                "custom-plugin-default_pipeline",
                "/org/elasticsearch/xpack/core/template/custom-plugin-default_pipeline.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE,
                Collections.singletonList("custom-plugin-final_pipeline")
            ),
            new JsonIngestPipelineConfig(
                "custom-plugin-final_pipeline",
                "/org/elasticsearch/xpack/core/template/custom-plugin-final_pipeline.json",
                REGISTRY_VERSION,
                TEMPLATE_VERSION_VARIABLE
            )
        );
    }

    @Override
    protected List<LifecyclePolicy> getLifecyclePolicies() {
        return List.of(
            new LifecyclePolicyConfig("custom-plugin-policy", "/org/elasticsearch/xpack/core/template/custom-plugin-policy.json").load(
                LifecyclePolicyConfig.DEFAULT_X_CONTENT_REGISTRY
            )
        );
    }

    @Override
    protected boolean isUpgradeRequired(LifecyclePolicy currentPolicy, LifecyclePolicy newPolicy) {
        return policyUpgradeRequired;
    }

    public void setPolicyUpgradeRequired(boolean policyUpgradeRequired) {
        this.policyUpgradeRequired = policyUpgradeRequired;
    }

    @Override
    protected String getOrigin() {
        return "test";
    }
}
