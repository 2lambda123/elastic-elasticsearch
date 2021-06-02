/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.cluster.metadata.MetadataMigrateToDataTiersRoutingService.MigratedEntities;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING;
import static org.elasticsearch.xpack.cluster.metadata.MetadataMigrateToDataTiersRoutingService.allocateActionDefinesRoutingRules;
import static org.elasticsearch.xpack.cluster.metadata.MetadataMigrateToDataTiersRoutingService.convertAttributeValueToTierPreference;
import static org.elasticsearch.xpack.cluster.metadata.MetadataMigrateToDataTiersRoutingService.migrateIlmPolicies;
import static org.elasticsearch.xpack.cluster.metadata.MetadataMigrateToDataTiersRoutingService.migrateIndices;
import static org.elasticsearch.xpack.cluster.metadata.MetadataMigrateToDataTiersRoutingService.migrateToDataTiersRouting;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.INDEX_ROUTING_PREFER;
import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class MetadataMigrateToDataTiersRoutingServiceTests extends ESTestCase {

    private static final String DATA_ROUTING_REQUIRE_SETTING = INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "data";
    private static final String BOX_ROUTING_REQUIRE_SETTING = INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "box";
    private static final NamedXContentRegistry REGISTRY;

    static {
        REGISTRY = new NamedXContentRegistry(List.of(
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ShrinkAction.NAME), ShrinkAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(AllocateAction.NAME), AllocateAction::parse)
        ));
    }

    private String lifecycleName;
    private String indexName;
    private Client client;

    @Before
    public void setupTestEntities() {
        lifecycleName = randomAlphaOfLengthBetween(10, 15);
        indexName = randomAlphaOfLengthBetween(10, 15);
        client = mock(Client.class);
        logger.info("--> running [{}] with indexName [{}] and ILM policy [{}]", getTestName(), indexName, lifecycleName);
    }

    public void testMigrateIlmPolicyForIndexWithoutILMMetadata() {
        ShrinkAction shrinkAction = new ShrinkAction(2, null);
        AllocateAction warmAllocateAction = new AllocateAction(null, Map.of("data", "warm"), null, Map.of("rack", "rack1"));
        AllocateAction coldAllocateAction = new AllocateAction(0, null, null, Map.of("data", "cold"));
        LifecyclePolicyMetadata policyMetadata = getWarmColdPolicyMeta(shrinkAction, warmAllocateAction, coldAllocateAction);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder()
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(
                Collections.singletonMap(policyMetadata.getName(), policyMetadata), OperationMode.RUNNING))
            .put(IndexMetadata.builder(indexName).settings(getBaseIndexSettings())).build())
            .build();

        Metadata.Builder newMetadata = Metadata.builder(state.metadata());
        List<String> migratedPolicies = migrateIlmPolicies(newMetadata, state, "data", REGISTRY, client);
        assertThat(migratedPolicies.size(), is(1));
        assertThat(migratedPolicies.get(0), is(lifecycleName));

        ClusterState newState = ClusterState.builder(state).metadata(newMetadata).build();
        IndexLifecycleMetadata updatedLifecycleMetadata = newState.metadata().custom(IndexLifecycleMetadata.TYPE);
        LifecyclePolicy lifecyclePolicy = updatedLifecycleMetadata.getPolicies().get(lifecycleName);
        Map<String, LifecycleAction> warmActions = lifecyclePolicy.getPhases().get("warm").getActions();
        assertThat("allocate action in the warm phase didn't specify any number of replicas so it must be removed",
            warmActions.size(), is(1));
        assertThat(warmActions.get(shrinkAction.getWriteableName()), is(shrinkAction));

        Map<String, LifecycleAction> coldActions = lifecyclePolicy.getPhases().get("cold").getActions();
        assertThat(coldActions.size(), is(1));
        AllocateAction migratedColdAllocateAction = (AllocateAction) coldActions.get(coldAllocateAction.getWriteableName());
        assertThat(migratedColdAllocateAction.getNumberOfReplicas(), is(0));
        assertThat(migratedColdAllocateAction.getRequire().size(), is(0));
    }

    public void testMigrateIlmPolicyRefreshesCachedPhase() {
        ShrinkAction shrinkAction = new ShrinkAction(2, null);
        AllocateAction warmAllocateAction = new AllocateAction(null, Map.of("data", "warm"), null, Map.of("rack", "rack1"));
        AllocateAction coldAllocateAction = new AllocateAction(0, null, null, Map.of("data", "cold"));
        LifecyclePolicyMetadata policyMetadata = getWarmColdPolicyMeta(shrinkAction, warmAllocateAction, coldAllocateAction);

        LifecycleExecutionState preMigrationExecutionState = LifecycleExecutionState.builder()
            .setPhase("cold")
            .setAction("allocate")
            .setStep("allocate")
            .setPhaseDefinition("{\n" +
                "        \"policy\" : \"" + lifecycleName + "\",\n" +
                "        \"phase_definition\" : {\n" +
                "          \"min_age\" : \"0m\",\n" +
                "          \"actions\" : {\n" +
                "            \"allocate\" : {\n" +
                "              \"number_of_replicas\" : \"0\",\n" +
                "              \"require\" : {\n" +
                "                \"data\": \"cold\"\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        \"version\" : 1,\n" +
                "        \"modified_date_in_millis\" : 1578521007076\n" +
                "      }")
            .build();

        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName).settings(getBaseIndexSettings())
            .putCustom(ILM_CUSTOM_METADATA_KEY, preMigrationExecutionState.asMap());

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder()
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(
                Collections.singletonMap(policyMetadata.getName(), policyMetadata), OperationMode.RUNNING))
            .put(indexMetadata).build())
            .build();

        Metadata.Builder newMetadata = Metadata.builder(state.metadata());
        List<String> migratedPolicies = migrateIlmPolicies(newMetadata, state, "data", REGISTRY, client);

        assertThat(migratedPolicies.get(0), is(lifecycleName));
        ClusterState newState = ClusterState.builder(state).metadata(newMetadata).build();
        LifecycleExecutionState newLifecycleState = LifecycleExecutionState.fromIndexMetadata(newState.metadata().index(indexName));

        // expecting the phase definition to be refreshed with the migrated phase representation
        // ie. allocate action does not contain any allocation rules
        String expectedRefreshedPhaseDefinition = "\"phase_definition\":{\"min_age\":\"0ms\"," +
            "\"actions\":{\"allocate\":{\"number_of_replicas\":0,\"include\":{},\"exclude\":{},\"require\":{}}}}";
        assertThat(newLifecycleState.getPhaseDefinition(), containsString(expectedRefreshedPhaseDefinition));
    }

    private Settings.Builder getBaseIndexSettings() {
        Settings.Builder settings = Settings.builder()
            .put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 10))
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 5))
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);
        return settings;
    }

    public void testAllocateActionDefinesRoutingRules() {
        assertThat(allocateActionDefinesRoutingRules("data", new AllocateAction(null, Map.of("data", "cold"), null, null)), is(true));
        assertThat(allocateActionDefinesRoutingRules("data", new AllocateAction(null, null, Map.of("data", "cold"), null)), is(true));
        assertThat(allocateActionDefinesRoutingRules("data", new AllocateAction(null, Map.of("another_attribute", "rack1"), null,
            Map.of("data", "cold"))), is(true));
        assertThat(allocateActionDefinesRoutingRules("data", new AllocateAction(null, null, null, Map.of("another_attribute", "cold"))),
            is(false));
        assertThat(allocateActionDefinesRoutingRules("data", null), is(false));
    }

    public void testConvertAttributeValueToTierPreference() {
        assertThat(convertAttributeValueToTierPreference("frozen"), is("data_frozen,data_cold,data_warm,data_hot"));
        assertThat(convertAttributeValueToTierPreference("cold"), is("data_cold,data_warm,data_hot"));
        assertThat(convertAttributeValueToTierPreference("warm"), is("data_warm,data_hot"));
        assertThat(convertAttributeValueToTierPreference("hot"), is("data_hot"));
        assertThat(convertAttributeValueToTierPreference("content"), nullValue());
        assertThat(convertAttributeValueToTierPreference("rack1"), nullValue());
    }

    public void testMigrateIndices() {
        {
            // index with `warm` data attribute is migrated to the equivalent _tier_preference routing
            IndexMetadata.Builder indexWitWarmDataAttribute =
                IndexMetadata.builder("indexWitWarmDataAttribute").settings(getBaseIndexSettings().put(DATA_ROUTING_REQUIRE_SETTING,
                    "warm"));
            ClusterState state =
                ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(indexWitWarmDataAttribute)).build();

            Metadata.Builder mb = Metadata.builder(state.metadata());

            List<String> migratedIndices = migrateIndices(mb, state, "data");
            assertThat(migratedIndices.size(), is(1));
            assertThat(migratedIndices.get(0), is("indexWitWarmDataAttribute"));

            ClusterState migratedState = ClusterState.builder(ClusterName.DEFAULT).metadata(mb).build();
            IndexMetadata migratedIndex = migratedState.metadata().index("indexWitWarmDataAttribute");
            assertThat(migratedIndex.getSettings().get(DATA_ROUTING_REQUIRE_SETTING), nullValue());
            assertThat(migratedIndex.getSettings().get(INDEX_ROUTING_PREFER), is("data_warm,data_hot"));
        }

        {
            // since the index has a _tier_preference configuration the migrated index should still contain it and have the `data`
            // attribute routing removed
            IndexMetadata.Builder indexWithTierPreferenceAndDataAttribute =
                IndexMetadata.builder("indexWithTierPreferenceAndDataAttribute").settings(getBaseIndexSettings()
                    .put(DATA_ROUTING_REQUIRE_SETTING, "cold")
                    .put(INDEX_ROUTING_PREFER, "data_warm,data_hot")
                );
            ClusterState state =
                ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(indexWithTierPreferenceAndDataAttribute)).build();

            Metadata.Builder mb = Metadata.builder(state.metadata());

            List<String> migratedIndices = migrateIndices(mb, state, "data");
            assertThat(migratedIndices.size(), is(1));
            assertThat(migratedIndices.get(0), is("indexWithTierPreferenceAndDataAttribute"));

            ClusterState migratedState = ClusterState.builder(ClusterName.DEFAULT).metadata(mb).build();
            IndexMetadata migratedIndex = migratedState.metadata().index("indexWithTierPreferenceAndDataAttribute");
            assertThat(migratedIndex.getSettings().get(DATA_ROUTING_REQUIRE_SETTING), nullValue());
            assertThat(migratedIndex.getSettings().get(INDEX_ROUTING_PREFER), is("data_warm,data_hot"));
        }

        {
            // index with an unknown `data` attribute routing value should **not** be migrated
            IndexMetadata.Builder indexWithUnknownDataAttribute =
                IndexMetadata.builder("indexWithUnknownDataAttribute").settings(getBaseIndexSettings().put(DATA_ROUTING_REQUIRE_SETTING,
                    "something_else"));
            ClusterState state =
                ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(indexWithUnknownDataAttribute)).build();

            Metadata.Builder mb = Metadata.builder(state.metadata());
            List<String> migratedIndices = migrateIndices(mb, state, "data");
            assertThat(migratedIndices.size(), is(0));

            ClusterState migratedState = ClusterState.builder(ClusterName.DEFAULT).metadata(mb).build();
            IndexMetadata migratedIndex = migratedState.metadata().index("indexWithUnknownDataAttribute");
            assertThat(migratedIndex.getSettings().get(DATA_ROUTING_REQUIRE_SETTING), is("something_else"));
        }

        {
            // index with data and another attribute should only see the data attribute removed and the corresponding tier_preference
            // configured
            IndexMetadata.Builder indexDataAndBoxAttribute =
                IndexMetadata.builder("indexWithDataAndBoxAttribute").settings(getBaseIndexSettings().put(DATA_ROUTING_REQUIRE_SETTING,
                    "warm").put(BOX_ROUTING_REQUIRE_SETTING, "box1"));

            ClusterState state =
                ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(indexDataAndBoxAttribute)).build();

            Metadata.Builder mb = Metadata.builder(state.metadata());
            List<String> migratedIndices = migrateIndices(mb, state, "data");
            assertThat(migratedIndices.size(), is(1));
            assertThat(migratedIndices.get(0), is("indexWithDataAndBoxAttribute"));

            ClusterState migratedState = ClusterState.builder(ClusterName.DEFAULT).metadata(mb).build();
            IndexMetadata migratedIndex = migratedState.metadata().index("indexWithDataAndBoxAttribute");
            assertThat(migratedIndex.getSettings().get(DATA_ROUTING_REQUIRE_SETTING), nullValue());
            assertThat(migratedIndex.getSettings().get(BOX_ROUTING_REQUIRE_SETTING), is("box1"));
            assertThat(migratedIndex.getSettings().get(INDEX_ROUTING_PREFER), is("data_warm,data_hot"));
        }

        {
            // index that doesn't have any data attribute routing but has another attribute should not see any change
            IndexMetadata.Builder indexBoxAttribute =
                IndexMetadata.builder("indexWithBoxAttribute").settings(getBaseIndexSettings().put(BOX_ROUTING_REQUIRE_SETTING, "warm"));

            ClusterState state =
                ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(indexBoxAttribute)).build();

            Metadata.Builder mb = Metadata.builder(state.metadata());
            List<String> migratedIndices = migrateIndices(mb, state, "data");
            assertThat(migratedIndices.size(), is(0));

            ClusterState migratedState = ClusterState.builder(ClusterName.DEFAULT).metadata(mb).build();
            IndexMetadata migratedIndex = migratedState.metadata().index("indexWithBoxAttribute");
            assertThat(migratedIndex.getSettings().get(DATA_ROUTING_REQUIRE_SETTING), nullValue());
            assertThat(migratedIndex.getSettings().get(BOX_ROUTING_REQUIRE_SETTING), is("warm"));
            assertThat(migratedIndex.getSettings().get(INDEX_ROUTING_PREFER), nullValue());
        }

        {
            IndexMetadata.Builder indexNoRoutingAttribute =
                IndexMetadata.builder("indexNoRoutingAttribute").settings(getBaseIndexSettings());

            ClusterState state =
                ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(indexNoRoutingAttribute)).build();

            Metadata.Builder mb = Metadata.builder(state.metadata());
            List<String> migratedIndices = migrateIndices(mb, state, "data");
            assertThat(migratedIndices.size(), is(0));

            ClusterState migratedState = ClusterState.builder(ClusterName.DEFAULT).metadata(mb).build();
            IndexMetadata migratedIndex = migratedState.metadata().index("indexNoRoutingAttribute");
            assertThat(migratedIndex.getSettings().get(DATA_ROUTING_REQUIRE_SETTING), nullValue());
            assertThat(migratedIndex.getSettings().get(BOX_ROUTING_REQUIRE_SETTING), nullValue());
            assertThat(migratedIndex.getSettings().get(INDEX_ROUTING_PREFER), nullValue());
        }
    }

    public void testMigrateToDataTiersRouting() {
        AllocateAction allocateActionWithDataAttribute = new AllocateAction(null, Map.of("data", "warm"), null, Map.of("rack", "rack1"));
        AllocateAction allocateActionWithOtherAttribute = new AllocateAction(0, null, null, Map.of("other", "cold"));

        LifecyclePolicy policyToMigrate = new LifecyclePolicy(lifecycleName,
            Map.of("warm",
                new Phase("warm", TimeValue.ZERO, Map.of(allocateActionWithDataAttribute.getWriteableName(),
                    allocateActionWithDataAttribute))));
        LifecyclePolicyMetadata policyWithDataAttribute = new LifecyclePolicyMetadata(policyToMigrate, Collections.emptyMap(),
            randomNonNegativeLong(), randomNonNegativeLong());

        LifecyclePolicy shouldntBeMigratedPolicy = new LifecyclePolicy("dont-migrate",
            Map.of("warm",
                new Phase("warm", TimeValue.ZERO, Map.of(allocateActionWithOtherAttribute.getWriteableName(),
                    allocateActionWithOtherAttribute))));
        LifecyclePolicyMetadata policyWithOtherAttribute = new LifecyclePolicyMetadata(shouldntBeMigratedPolicy, Collections.emptyMap(),
            randomNonNegativeLong(), randomNonNegativeLong());


        IndexMetadata.Builder indexWithUnknownDataAttribute =
            IndexMetadata.builder("indexWithUnknownDataAttribute").settings(getBaseIndexSettings().put(DATA_ROUTING_REQUIRE_SETTING,
                "something_else"));
        IndexMetadata.Builder indexWitWarmDataAttribute =
            IndexMetadata.builder("indexWitWarmDataAttribute").settings(getBaseIndexSettings().put(DATA_ROUTING_REQUIRE_SETTING, "warm"));

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder()
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(
                Map.of(policyToMigrate.getName(), policyWithDataAttribute, shouldntBeMigratedPolicy.getName(), policyWithOtherAttribute),
                OperationMode.RUNNING))
            .put(IndexTemplateMetadata.builder("catch-all").patterns(List.of("*"))
                .settings(Settings.builder().put(DATA_ROUTING_REQUIRE_SETTING, "hot"))
                .build())
            .put(IndexTemplateMetadata.builder("other-template").patterns(List.of("other-*"))
                .settings(Settings.builder().put(DATA_ROUTING_REQUIRE_SETTING, "hot"))
                .build())
            .put(indexWithUnknownDataAttribute).put(indexWitWarmDataAttribute))
            .build();

        {
            Tuple<ClusterState, MigratedEntities> migratedEntitiesTuple =
                migrateToDataTiersRouting(state, "data", "catch-all", REGISTRY, client);

            MigratedEntities migratedEntities = migratedEntitiesTuple.v2();
            assertThat(migratedEntities.removedIndexTemplateName, is("catch-all"));
            assertThat(migratedEntities.migratedPolicies.size(), is(1));
            assertThat(migratedEntities.migratedPolicies.get(0), is(lifecycleName));
            assertThat(migratedEntities.migratedIndices.size(), is(1));
            assertThat(migratedEntities.migratedIndices.get(0), is("indexWitWarmDataAttribute"));

            ClusterState newState = migratedEntitiesTuple.v1();
            assertThat(newState.metadata().getTemplates().size(), is(1));
            assertThat(newState.metadata().getTemplates().get("catch-all"), nullValue());
            assertThat(newState.metadata().getTemplates().get("other-template"), notNullValue());
        }

        {
            // let's test a null template name to make sure nothing is removed
            Tuple<ClusterState, MigratedEntities> migratedEntitiesTuple =
                migrateToDataTiersRouting(state, "data", null, REGISTRY, client);

            MigratedEntities migratedEntities = migratedEntitiesTuple.v2();
            assertThat(migratedEntities.removedIndexTemplateName, nullValue());
            assertThat(migratedEntities.migratedPolicies.size(), is(1));
            assertThat(migratedEntities.migratedPolicies.get(0), is(lifecycleName));
            assertThat(migratedEntities.migratedIndices.size(), is(1));
            assertThat(migratedEntities.migratedIndices.get(0), is("indexWitWarmDataAttribute"));

            ClusterState newState = migratedEntitiesTuple.v1();
            assertThat(newState.metadata().getTemplates().size(), is(2));
            assertThat(newState.metadata().getTemplates().get("catch-all"), notNullValue());
            assertThat(newState.metadata().getTemplates().get("other-template"), notNullValue());
        }

        {
            // let's test a null node attribute parameter defaults to "data"
            Tuple<ClusterState, MigratedEntities> migratedEntitiesTuple =
                migrateToDataTiersRouting(state, null, null, REGISTRY, client);

            MigratedEntities migratedEntities = migratedEntitiesTuple.v2();
            assertThat(migratedEntities.migratedPolicies.size(), is(1));
            assertThat(migratedEntities.migratedPolicies.get(0), is(lifecycleName));
            assertThat(migratedEntities.migratedIndices.size(), is(1));
            assertThat(migratedEntities.migratedIndices.get(0), is("indexWitWarmDataAttribute"));

            IndexMetadata migratedIndex = migratedEntitiesTuple.v1().metadata().index("indexWitWarmDataAttribute");
            assertThat(migratedIndex.getSettings().get(INDEX_ROUTING_PREFER), is("data_warm,data_hot"));
        }
    }

    private LifecyclePolicyMetadata getWarmColdPolicyMeta(ShrinkAction shrinkAction, AllocateAction warmAllocateAction,
                                                          AllocateAction coldAllocateAction) {
        LifecyclePolicy policy = new LifecyclePolicy(lifecycleName,
            Map.of("warm",
                new Phase("warm", TimeValue.ZERO, Map.of(shrinkAction.getWriteableName(), shrinkAction,
                    warmAllocateAction.getWriteableName(), warmAllocateAction)),
                "cold",
                new Phase("cold", TimeValue.ZERO, Map.of(coldAllocateAction.getWriteableName(), coldAllocateAction))
            ));
        return new LifecyclePolicyMetadata(policy, Collections.emptyMap(),
            randomNonNegativeLong(), randomNonNegativeLong());
    }
}
