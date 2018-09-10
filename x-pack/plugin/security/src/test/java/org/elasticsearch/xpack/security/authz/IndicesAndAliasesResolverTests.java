/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.protocol.xpack.graph.GraphExploreRequest;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.graph.action.GraphExploreAction;
import org.elasticsearch.xpack.core.security.authc.DefaultAuthenticationFailureHandler;
import org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authz.IndicesAndAliasesResolver.ResolvedIndices;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.test.SecurityTestUtils;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.security.support.SecurityIndexManager.SECURITY_INDEX_NAME;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndicesAndAliasesResolverTests extends ESTestCase {

    private User user;
    private User userDashIndices;
    private User userNoIndices;
    private CompositeRolesStore rolesStore;
    private MetaData metaData;
    private AuthorizationService authzService;
    private IndicesAndAliasesResolver defaultIndicesResolver;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private Map<String, RoleDescriptor> roleMap;

    @Before
    public void setup() {
        Settings settings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 2))
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 1))
                .put("cluster.remote.remote.seeds", "127.0.0.1:" + randomIntBetween(9301, 9350))
                .put("cluster.remote.other_remote.seeds", "127.0.0.1:" + randomIntBetween(9351, 9399))
                .build();

        indexNameExpressionResolver = new IndexNameExpressionResolver(Settings.EMPTY);

        final boolean withAlias = randomBoolean();
        final String securityIndexName = SECURITY_INDEX_NAME + (withAlias ? "-" + randomAlphaOfLength(5) : "");
        MetaData metaData = MetaData.builder()
                .put(indexBuilder("foo").putAlias(AliasMetaData.builder("foofoobar")).settings(settings))
                .put(indexBuilder("foobar").putAlias(AliasMetaData.builder("foofoobar")).settings(settings))
                .put(indexBuilder("closed").state(IndexMetaData.State.CLOSE)
                        .putAlias(AliasMetaData.builder("foofoobar")).settings(settings))
                .put(indexBuilder("foofoo-closed").state(IndexMetaData.State.CLOSE).settings(settings))
                .put(indexBuilder("foobar-closed").state(IndexMetaData.State.CLOSE).settings(settings))
                .put(indexBuilder("foofoo").putAlias(AliasMetaData.builder("barbaz")).settings(settings))
                .put(indexBuilder("bar").settings(settings))
                .put(indexBuilder("bar-closed").state(IndexMetaData.State.CLOSE).settings(settings))
                .put(indexBuilder("bar2").settings(settings))
                .put(indexBuilder(indexNameExpressionResolver.resolveDateMathExpression("<datetime-{now/M}>")).settings(settings))
                .put(indexBuilder("-index10").settings(settings))
                .put(indexBuilder("-index11").settings(settings))
                .put(indexBuilder("-index20").settings(settings))
                .put(indexBuilder("-index21").settings(settings))
                .put(indexBuilder(securityIndexName).settings(settings)).build();

        if (withAlias) {
            metaData = SecurityTestUtils.addAliasToMetaData(metaData, securityIndexName);
        }
        this.metaData = metaData;

        user = new User("user", "role");
        userDashIndices = new User("dash", "dash");
        userNoIndices = new User("test", "test");
        rolesStore = mock(CompositeRolesStore.class);
        String[] authorizedIndices = new String[] { "bar", "bar-closed", "foofoobar", "foofoo", "missing", "foofoo-closed"};
        String[] dashIndices = new String[]{"-index10", "-index11", "-index20", "-index21"};
        roleMap = new HashMap<>();
        roleMap.put("role", new RoleDescriptor("role", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices(authorizedIndices).privileges("all").build() }, null));
        roleMap.put("dash", new RoleDescriptor("dash", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices(dashIndices).privileges("all").build() }, null));
        roleMap.put("test", new RoleDescriptor("test", new String[] { "monitor" }, null, null));
        roleMap.put("alias_read_write", new RoleDescriptor("alias_read_write", null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("barbaz", "foofoobar").privileges("read", "write").build() },
            null));
        roleMap.put(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName(), ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        final FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        doAnswer((i) -> {
                ActionListener callback =
                        (ActionListener) i.getArguments()[2];
                Set<String> names = (Set<String>) i.getArguments()[0];
                assertNotNull(names);
                Set<RoleDescriptor> roleDescriptors = new HashSet<>();
                for (String name : names) {
                    RoleDescriptor descriptor = roleMap.get(name);
                    if (descriptor != null) {
                        roleDescriptors.add(descriptor);
                    }
                }

                if (roleDescriptors.isEmpty()) {
                    callback.onResponse(Role.EMPTY);
                } else {
                    CompositeRolesStore.buildRoleFromDescriptors(roleDescriptors, fieldPermissionsCache, null,
                            ActionListener.wrap(r -> callback.onResponse(r), callback::onFailure)
                    );
                }
                return Void.TYPE;
            }).when(rolesStore).roles(any(Set.class), any(FieldPermissionsCache.class), any(ActionListener.class));

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        authzService = new AuthorizationService(settings, rolesStore, clusterService,
                mock(AuditTrailService.class), new DefaultAuthenticationFailureHandler(), mock(ThreadPool.class),
                new AnonymousUser(settings));
        defaultIndicesResolver = new IndicesAndAliasesResolver(settings, clusterService);
    }

    public void testDashIndicesAreAllowedInShardLevelRequests() {
        //indices with names starting with '-' or '+' can be created up to version  2.x and can be around in 5.x
        //aliases with names starting with '-' or '+' can be created up to version 5.x and can be around in 6.x
        ShardSearchTransportRequest request = mock(ShardSearchTransportRequest.class);
        when(request.indices()).thenReturn(new String[]{"-index10", "-index20", "+index30"});
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME))
                .getLocal();
        String[] expectedIndices = new String[]{"-index10", "-index20", "+index30"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
    }

    public void testWildcardsAreNotAllowedInShardLevelRequests() {
        ShardSearchTransportRequest request = mock(ShardSearchTransportRequest.class);
        when(request.indices()).thenReturn(new String[]{"index*"});
        IllegalStateException illegalStateException = expectThrows(IllegalStateException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME))
                        .getLocal());
        assertEquals("There are no external requests known to support wildcards that don't support replacing their indices",
                illegalStateException.getMessage());
    }

    public void testAllIsNotAllowedInShardLevelRequests() {
        ShardSearchTransportRequest request = mock(ShardSearchTransportRequest.class);
        if (randomBoolean()) {
            when(request.indices()).thenReturn(new String[]{"_all"});
        } else {
            if (randomBoolean()) {
                when(request.indices()).thenReturn(Strings.EMPTY_ARRAY);
            } else {
                when(request.indices()).thenReturn(null);
            }
        }
        IllegalStateException illegalStateException = expectThrows(IllegalStateException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME))
                        .getLocal());
        assertEquals("There are no external requests known to support wildcards that don't support replacing their indices",
                illegalStateException.getMessage());
    }

    public void testExplicitDashIndices() {
        SearchRequest request = new SearchRequest("-index10", "-index20");
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME)).getLocal();
        String[] expectedIndices = new String[]{"-index10", "-index20"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(request.indices().length, equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(expectedIndices));
    }

    public void testWildcardDashIndices() {
        SearchRequest request;
        if (randomBoolean()) {
            request = new SearchRequest("-index*", "--index20");
        } else {
            request = new SearchRequest("*", "--index20");
        }
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME)).getLocal();
        String[] expectedIndices = new String[]{"-index10", "-index11", "-index21"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(request.indices().length, equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(expectedIndices));
    }

    public void testExplicitMixedWildcardDashIndices() {
        SearchRequest request = new SearchRequest("-index21", "-does_not_exist", "-index1*", "--index11");
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME)).getLocal();
        String[] expectedIndices = new String[]{"-index10", "-index21", "-does_not_exist"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(request.indices().length, equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(expectedIndices));
    }

    public void testDashIndicesNoExpandWildcard() {
        SearchRequest request = new SearchRequest("-index1*", "--index11");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), false, false));
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME)).getLocal();
        String[] expectedIndices = new String[]{"-index1*", "--index11"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(request.indices().length, equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(expectedIndices));
    }

    public void testDashIndicesMinus() {
        SearchRequest request = new SearchRequest("-index10", "-index11", "--index11", "-index20");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), randomBoolean(), randomBoolean()));
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME)).getLocal();
        String[] expectedIndices = new String[]{"-index10", "-index11", "--index11", "-index20"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(request.indices().length, equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(expectedIndices));
    }

    public void testDashIndicesPlus() {
        SearchRequest request = new SearchRequest("+bar");
        request.indicesOptions(IndicesOptions.fromOptions(true, false, randomBoolean(), randomBoolean()));
        expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME)));
    }

    public void testDashNotExistingIndex() {
        SearchRequest request = new SearchRequest("-does_not_exist");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), randomBoolean(), randomBoolean()));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(userDashIndices, SearchAction.NAME)).getLocal();
        String[] expectedIndices = new String[]{"-does_not_exist"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(request.indices().length, equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(expectedIndices));
    }

    public void testResolveEmptyIndicesExpandWilcardsOpenAndClosed() {
        SearchRequest request = new SearchRequest();
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), true, true));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foofoo", "foofoo-closed"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveEmptyIndicesExpandWilcardsOpen() {
        SearchRequest request = new SearchRequest();
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), true, false));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar", "foofoobar", "foofoo"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveAllExpandWilcardsOpenAndClosed() {
        SearchRequest request = new SearchRequest("_all");
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), true, true));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foofoo", "foofoo-closed"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveAllExpandWilcardsOpen() {
        SearchRequest request = new SearchRequest("_all");
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), true, false));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar", "foofoobar", "foofoo"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveWildcardsStrictExpand() {
        SearchRequest request = new SearchRequest("barbaz", "foofoo*");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), true, true));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"barbaz", "foofoobar", "foofoo", "foofoo-closed"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveWildcardsExpandOpenAndClosedIgnoreUnavailable() {
        SearchRequest request = new SearchRequest("barbaz", "foofoo*");
        request.indicesOptions(IndicesOptions.fromOptions(true, randomBoolean(), true, true));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"foofoobar", "foofoo", "foofoo-closed"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveWildcardsStrictExpandOpen() {
        SearchRequest request = new SearchRequest("barbaz", "foofoo*");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), true, false));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"barbaz", "foofoobar", "foofoo"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveWildcardsLenientExpandOpen() {
        SearchRequest request = new SearchRequest("barbaz", "foofoo*");
        request.indicesOptions(IndicesOptions.fromOptions(true, randomBoolean(), true, false));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"foofoobar", "foofoo"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveWildcardsMinusExpandWilcardsOpen() {
        SearchRequest request = new SearchRequest("*", "-foofoo*");
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), true, false));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveWildcardsMinusExpandWilcardsOpenAndClosed() {
        SearchRequest request = new SearchRequest("*", "-foofoo*");
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), true, true));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar", "bar-closed"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveWildcardsExclusionsExpandWilcardsOpenStrict() {
        SearchRequest request = new SearchRequest("*", "-foofoo*", "barbaz", "foob*");
        request.indicesOptions(IndicesOptions.fromOptions(false, true, true, false));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar", "barbaz"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveWildcardsPlusAndMinusExpandWilcardsOpenIgnoreUnavailable() {
        SearchRequest request = new SearchRequest("*", "-foofoo*", "+barbaz", "+foob*");
        request.indicesOptions(IndicesOptions.fromOptions(true, true, true, false));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveWildcardsExclusionExpandWilcardsOpenAndClosedStrict() {
        SearchRequest request = new SearchRequest("*", "-foofoo*", "barbaz");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), true, true));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar", "bar-closed", "barbaz"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveWildcardsExclusionExpandWilcardsOpenAndClosedIgnoreUnavailable() {
        SearchRequest request = new SearchRequest("*", "-foofoo*", "barbaz");
        request.indicesOptions(IndicesOptions.fromOptions(true, randomBoolean(), true, true));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar", "bar-closed"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveNonMatchingIndicesAllowNoIndices() {
        SearchRequest request = new SearchRequest("missing*");
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), true, true, randomBoolean()));
        assertNoIndices(request, resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)));
    }

    public void testResolveNonMatchingIndicesDisallowNoIndices() {
        SearchRequest request = new SearchRequest("missing*");
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), false, true, randomBoolean()));
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)));
        assertEquals("no such index", e.getMessage());
    }

    public void testResolveExplicitIndicesStrict() {
        SearchRequest request = new SearchRequest("missing", "bar", "barbaz");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), randomBoolean(), randomBoolean()));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"missing", "bar", "barbaz"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveExplicitIndicesIgnoreUnavailable() {
        SearchRequest request = new SearchRequest("missing", "bar", "barbaz");
        request.indicesOptions(IndicesOptions.fromOptions(true, randomBoolean(), randomBoolean(), randomBoolean()));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] replacedIndices = new String[]{"bar"};
        assertThat(indices.size(), equalTo(replacedIndices.length));
        assertThat(request.indices().length, equalTo(replacedIndices.length));
        assertThat(indices, hasItems(replacedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
    }

    public void testResolveNoAuthorizedIndicesAllowNoIndices() {
        SearchRequest request = new SearchRequest();
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), true, true, randomBoolean()));
        assertNoIndices(request, resolveIndices(request,
                buildAuthorizedIndices(userNoIndices, SearchAction.NAME)));
    }

    public void testResolveNoAuthorizedIndicesDisallowNoIndices() {
        SearchRequest request = new SearchRequest();
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), false, true, randomBoolean()));
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(userNoIndices, SearchAction.NAME)));
        assertEquals("no such index", e.getMessage());
    }

    public void testResolveMissingIndexStrict() {
        SearchRequest request = new SearchRequest("bar*", "missing");
        request.indicesOptions(IndicesOptions.fromOptions(false, true, true, false));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] expectedIndices = new String[]{"bar", "missing"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(request.indices().length, equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), equalTo(expectedIndices));
    }

    public void testResolveMissingIndexIgnoreUnavailable() {
        SearchRequest request = new SearchRequest("bar*", "missing");
        request.indicesOptions(IndicesOptions.fromOptions(true, randomBoolean(), true, false));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] expectedIndices = new String[]{"bar"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(request.indices().length, equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), equalTo(expectedIndices));
    }

    public void testResolveNonMatchingIndicesAndExplicit() {
        SearchRequest request = new SearchRequest("missing*", "bar");
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), true, true, randomBoolean()));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] expectedIndices = new String[]{"bar"};
        assertThat(indices.toArray(new String[indices.size()]), equalTo(expectedIndices));
        assertThat(request.indices(), equalTo(expectedIndices));
    }

    public void testResolveNoExpandStrict() {
        SearchRequest request = new SearchRequest("missing*");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), false, false));
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        String[] expectedIndices = new String[]{"missing*"};
        assertThat(indices.toArray(new String[indices.size()]), equalTo(expectedIndices));
        assertThat(request.indices(), equalTo(expectedIndices));
    }

    public void testResolveNoExpandIgnoreUnavailable() {
        SearchRequest request = new SearchRequest("missing*");
        request.indicesOptions(IndicesOptions.fromOptions(true, true, false, false));
        assertNoIndices(request, resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)));
    }

    public void testSearchWithRemoteIndex() {
        SearchRequest request = new SearchRequest("remote:indexName");
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        final ResolvedIndices resolved = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME));
        assertThat(resolved.getLocal(), emptyIterable());
        assertThat(resolved.getRemote(), containsInAnyOrder("remote:indexName"));
        assertThat(request.indices(), arrayContaining("remote:indexName"));
    }

    public void testSearchWithRemoteAndLocalIndices() {
        SearchRequest request = new SearchRequest("remote:indexName", "bar", "bar2");
        request.indicesOptions(IndicesOptions.fromOptions(true, randomBoolean(), randomBoolean(), randomBoolean()));
        final ResolvedIndices resolved = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME));
        assertThat(resolved.getLocal(), containsInAnyOrder("bar"));
        assertThat(resolved.getRemote(), containsInAnyOrder("remote:indexName"));
        assertThat(request.indices(), arrayContainingInAnyOrder("remote:indexName", "bar"));
    }

    public void testSearchWithRemoteAndLocalWildcards() {
        SearchRequest request = new SearchRequest("*:foo", "r*:bar*", "remote:baz*", "bar*", "foofoo");
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), true, false));
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, SearchAction.NAME);
        final ResolvedIndices resolved = resolveIndices(request, authorizedIndices);
        assertThat(resolved.getRemote(), containsInAnyOrder("remote:foo", "other_remote:foo", "remote:bar*", "remote:baz*"));
        assertThat(resolved.getLocal(), containsInAnyOrder("bar", "foofoo"));
        assertThat(request.indices(),
                arrayContainingInAnyOrder("remote:foo", "other_remote:foo", "remote:bar*", "remote:baz*", "bar", "foofoo"));
    }

    public void testResolveIndicesAliasesRequest() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.add().alias("alias1").indices("foo", "foofoo"));
        request.addAliasAction(AliasActions.add().alias("alias2").indices("foo", "foobar"));
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(user, IndicesAliasesAction.NAME)).getLocal();
        //the union of all indices and aliases gets returned
        String[] expectedIndices = new String[]{"alias1", "alias2", "foo", "foofoo", "foobar"};
        assertSameValues(indices, expectedIndices);
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("foo", "foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("alias1"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContainingInAnyOrder("foo", "foobar"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("alias2"));
    }

    public void testResolveIndicesAliasesRequestExistingAlias() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.add().alias("alias1").indices("foo", "foofoo"));
        request.addAliasAction(AliasActions.add().alias("foofoobar").indices("foo", "foobar"));
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(user, IndicesAliasesAction.NAME)).getLocal();
        //the union of all indices and aliases gets returned, foofoobar is an existing alias but that doesn't make any difference
        String[] expectedIndices = new String[]{"alias1", "foofoobar", "foo", "foofoo", "foobar"};
        assertSameValues(indices, expectedIndices);
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("foo", "foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("alias1"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContainingInAnyOrder("foo", "foobar"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("foofoobar"));
    }

    public void testResolveIndicesAliasesRequestMissingIndex() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.add().alias("alias1").indices("foo", "foofoo"));
        request.addAliasAction(AliasActions.add().alias("alias2").index("missing"));
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(user, IndicesAliasesAction.NAME)).getLocal();
        //the union of all indices and aliases gets returned, missing is not an existing index/alias but that doesn't make any difference
        String[] expectedIndices = new String[]{"alias1", "alias2", "foo", "foofoo", "missing"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("foo", "foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("alias1"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContainingInAnyOrder("missing"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("alias2"));
    }

    public void testResolveWildcardsIndicesAliasesRequest() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.add().alias("foo-alias").index("foo*"));
        request.addAliasAction(AliasActions.add().alias("alias2").index("bar*"));
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(user, IndicesAliasesAction.NAME)).getLocal();
        //the union of all resolved indices and aliases gets returned, based on indices and aliases that user is authorized for
        String[] expectedIndices = new String[]{"foo-alias", "alias2", "foofoo", "bar"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        //wildcards get replaced on each single action
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("foo-alias"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContainingInAnyOrder("bar"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("alias2"));
    }

    public void testResolveWildcardsIndicesAliasesRequestNoMatchingIndices() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.add().alias("alias1").index("foo*"));
        request.addAliasAction(AliasActions.add().alias("alias2").index("bar*"));
        request.addAliasAction(AliasActions.add().alias("alias3").index("non_matching_*"));
        //if a single operation contains wildcards and ends up being resolved to no indices, it makes the whole request fail
        expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(user, IndicesAliasesAction.NAME)));
    }

    public void testResolveAllIndicesAliasesRequest() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.add().alias("alias1").index("_all"));
        request.addAliasAction(AliasActions.add().alias("alias2").index("_all"));
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(user, IndicesAliasesAction.NAME)).getLocal();
        //the union of all resolved indices and aliases gets returned
        String[] expectedIndices = new String[]{"bar", "foofoo", "alias1", "alias2"};
        assertSameValues(indices, expectedIndices);
        String[] replacedIndices = new String[]{"bar", "foofoo"};
        //_all gets replaced with all indices that user is authorized for, on each single action
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder(replacedIndices));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("alias1"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContainingInAnyOrder(replacedIndices));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("alias2"));
    }

    public void testResolveAllIndicesAliasesRequestNoAuthorizedIndices() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.add().alias("alias1").index("_all"));
        //current user is not authorized for any index, _all resolves to no indices, the request fails
        expectThrows(IndexNotFoundException.class, () ->
                resolveIndices(request, buildAuthorizedIndices(userNoIndices, IndicesAliasesAction.NAME)));
    }

    public void testResolveWildcardsIndicesAliasesRequestNoAuthorizedIndices() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.add().alias("alias1").index("foo*"));
        //current user is not authorized for any index, foo* resolves to no indices, the request fails
        expectThrows(IndexNotFoundException.class, () -> resolveIndices(
                request, buildAuthorizedIndices(userNoIndices, IndicesAliasesAction.NAME)));
    }

    public void testResolveIndicesAliasesRequestDeleteActions() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.remove().index("foo").alias("foofoobar"));
        request.addAliasAction(AliasActions.remove().index("foofoo").alias("barbaz"));
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, IndicesAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all indices and aliases gets returned
        String[] expectedIndices = new String[]{"foo", "foofoobar", "foofoo", "barbaz"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("foo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("foofoobar"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContainingInAnyOrder("foofoo"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("barbaz"));
    }

    public void testResolveIndicesAliasesRequestDeleteActionsMissingIndex() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.remove().index("foo").alias("foofoobar"));
        request.addAliasAction(AliasActions.remove().index("missing_index").alias("missing_alias"));
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, IndicesAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all indices and aliases gets returned, doesn't matter is some of them don't exist
        String[] expectedIndices = new String[]{"foo", "foofoobar", "missing_index", "missing_alias"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("foo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("foofoobar"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContainingInAnyOrder("missing_index"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("missing_alias"));
    }

    public void testResolveWildcardsIndicesAliasesRequestDeleteActions() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.remove().index("foo*").alias("foofoobar"));
        request.addAliasAction(AliasActions.remove().index("bar*").alias("barbaz"));
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, IndicesAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //union of all resolved indices and aliases gets returned, based on what user is authorized for
        String[] expectedIndices = new String[]{"foofoobar", "foofoo", "bar", "barbaz"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        //wildcards get replaced within each single action
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("foofoobar"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContainingInAnyOrder("bar"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("barbaz"));
    }

    public void testResolveAliasesWildcardsIndicesAliasesRequestDeleteActions() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.remove().index("*").alias("foo*"));
        request.addAliasAction(AliasActions.remove().index("*bar").alias("foo*"));
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, IndicesAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //union of all resolved indices and aliases gets returned, based on what user is authorized for
        //note that the index side will end up containing matching aliases too, which is fine, as es core would do
        //the same and resolve those aliases to their corresponding concrete indices (which we let core do)
        String[] expectedIndices = new String[]{"bar", "foofoobar", "foofoo"};
        assertSameValues(indices, expectedIndices);
        //alias foofoobar on both sides, that's fine, es core would do the same, same as above
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("bar", "foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("foofoobar"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContainingInAnyOrder("bar"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("foofoobar"));
    }

    public void testResolveAllAliasesWildcardsIndicesAliasesRequestDeleteActions() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.remove().index("*").alias("_all"));
        request.addAliasAction(AliasActions.remove().index("_all").aliases("_all", "explicit"));
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, IndicesAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //union of all resolved indices and aliases gets returned, based on what user is authorized for
        //note that the index side will end up containing matching aliases too, which is fine, as es core would do
        //the same and resolve those aliases to their corresponding concrete indices (which we let core do)
        String[] expectedIndices = new String[]{"bar", "foofoobar", "foofoo", "explicit"};
        assertSameValues(indices, expectedIndices);
        //alias foofoobar on both sides, that's fine, es core would do the same, same as above
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("bar", "foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("foofoobar"));
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("bar", "foofoo"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("foofoobar", "explicit"));
    }

    public void testResolveAliasesWildcardsIndicesAliasesRequestDeleteActionsNoAuthorizedIndices() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.remove().index("foo*").alias("foo*"));
        //no authorized aliases match bar*, hence this action fails and makes the whole request fail
        request.addAliasAction(AliasActions.remove().index("*bar").alias("bar*"));
        expectThrows(IndexNotFoundException.class, () -> resolveIndices(
                request, buildAuthorizedIndices(user, IndicesAliasesAction.NAME)));
    }

    public void testResolveWildcardsIndicesAliasesRequestAddAndDeleteActions() {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(AliasActions.remove().index("foo*").alias("foofoobar"));
        request.addAliasAction(AliasActions.add().index("bar*").alias("foofoobar"));
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, IndicesAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //union of all resolved indices and aliases gets returned, based on what user is authorized for
        String[] expectedIndices = new String[]{"foofoobar", "foofoo", "bar"};
        assertSameValues(indices, expectedIndices);
        //every single action has its indices replaced with matching (authorized) ones
        assertThat(request.getAliasActions().get(0).indices(), arrayContainingInAnyOrder("foofoo"));
        assertThat(request.getAliasActions().get(0).aliases(), arrayContainingInAnyOrder("foofoobar"));
        assertThat(request.getAliasActions().get(1).indices(), arrayContainingInAnyOrder("bar"));
        assertThat(request.getAliasActions().get(1).aliases(), arrayContainingInAnyOrder("foofoobar"));
    }

    public void testResolveGetAliasesRequestStrict() {
        GetAliasesRequest request = new GetAliasesRequest("alias1").indices("foo", "foofoo");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), randomBoolean(), randomBoolean()));
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all indices and aliases gets returned
        String[] expectedIndices = new String[]{"alias1", "foo", "foofoo"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder("foo", "foofoo"));
        assertThat(request.aliases(), arrayContainingInAnyOrder("alias1"));
    }

    public void testResolveGetAliasesRequestIgnoreUnavailable() {
        GetAliasesRequest request = new GetAliasesRequest("alias1").indices("foo", "foofoo");
        request.indicesOptions(IndicesOptions.fromOptions(true, randomBoolean(), randomBoolean(), randomBoolean()));
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        String[] expectedIndices = new String[]{"alias1", "foofoo"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder("foofoo"));
        assertThat(request.aliases(), arrayContainingInAnyOrder("alias1"));
    }

    public void testResolveGetAliasesRequestMissingIndexStrict() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), true, randomBoolean()));
        request.indices("missing");
        request.aliases("alias2");
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all indices and aliases gets returned, missing is not an existing index/alias but that doesn't make any difference
        String[] expectedIndices = new String[]{"alias2", "missing"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder("missing"));
        assertThat(request.aliases(), arrayContainingInAnyOrder("alias2"));
    }

    public void testGetAliasesRequestMissingIndexIgnoreUnavailableDisallowNoIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(true, false, randomBoolean(), randomBoolean()));
        request.indices("missing");
        request.aliases("alias2");
        IndexNotFoundException exception = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(user, GetAliasesAction.NAME)).getLocal());
        assertEquals("no such index", exception.getMessage());
    }

    public void testGetAliasesRequestMissingIndexIgnoreUnavailableAllowNoIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(true, true, randomBoolean(), randomBoolean()));
        request.indices("missing");
        request.aliases("alias2");
        assertNoIndices(request, resolveIndices(request, buildAuthorizedIndices(user, GetAliasesAction.NAME)));
    }

    public void testGetAliasesRequestMissingIndexStrict() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), randomBoolean(), randomBoolean()));
        request.indices("missing");
        request.aliases("alias2");
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        String[] expectedIndices = new String[]{"alias2", "missing"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder("missing"));
        assertThat(request.aliases(), arrayContainingInAnyOrder("alias2"));
    }

    public void testResolveWildcardsGetAliasesRequestStrictExpand() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), true, true));
        request.aliases("alias1");
        request.indices("foo*");
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all resolved indices and aliases gets returned, based on indices and aliases that user is authorized for
        String[] expectedIndices = new String[]{"alias1", "foofoo", "foofoo-closed", "foofoobar"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        //wildcards get replaced on each single action
        assertThat(request.indices(), arrayContainingInAnyOrder("foofoobar", "foofoo", "foofoo-closed"));
        assertThat(request.aliases(), arrayContainingInAnyOrder("alias1"));
    }

    public void testResolveWildcardsGetAliasesRequestStrictExpandOpen() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), true, false));
        request.aliases("alias1");
        request.indices("foo*");
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all resolved indices and aliases gets returned, based on indices and aliases that user is authorized for
        String[] expectedIndices = new String[]{"alias1", "foofoo", "foofoobar"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        //wildcards get replaced on each single action
        assertThat(request.indices(), arrayContainingInAnyOrder("foofoobar", "foofoo"));
        assertThat(request.aliases(), arrayContainingInAnyOrder("alias1"));
    }

    public void testResolveWildcardsGetAliasesRequestLenientExpandOpen() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(true, randomBoolean(), true, false));
        request.aliases("alias1");
        request.indices("foo*", "bar", "missing");
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all resolved indices and aliases gets returned, based on indices and aliases that user is authorized for
        String[] expectedIndices = new String[]{"alias1", "foofoo", "foofoobar", "bar"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        //wildcards get replaced on each single action
        assertThat(request.indices(), arrayContainingInAnyOrder("foofoobar", "foofoo", "bar"));
        assertThat(request.aliases(), arrayContainingInAnyOrder("alias1"));
    }

    public void testWildcardsGetAliasesRequestNoMatchingIndicesDisallowNoIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), false, true, randomBoolean()));
        request.aliases("alias3");
        request.indices("non_matching_*");
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(user, GetAliasesAction.NAME)).getLocal());
        assertEquals("no such index", e.getMessage());
    }

    public void testWildcardsGetAliasesRequestNoMatchingIndicesAllowNoIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), true, true, randomBoolean()));
        request.aliases("alias3");
        request.indices("non_matching_*");
        assertNoIndices(request, resolveIndices(request, buildAuthorizedIndices(user, GetAliasesAction.NAME)));
    }

    public void testResolveAllGetAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest();
        //even if not set, empty means _all
        if (randomBoolean()) {
            request.indices("_all");
        }
        request.aliases("alias1");
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all resolved indices and aliases gets returned
        String[] expectedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foofoo", "foofoo-closed", "alias1"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        String[] replacedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foofoo", "foofoo-closed"};
        //_all gets replaced with all indices that user is authorized for
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
        assertThat(request.aliases(), arrayContainingInAnyOrder("alias1"));
    }

    public void testResolveAllGetAliasesRequestExpandWildcardsOpenOnly() {
        GetAliasesRequest request = new GetAliasesRequest();
        //set indices options to have wildcards resolved to open indices only (default is open and closed)
        request.indicesOptions(IndicesOptions.fromOptions(true, false, true, false));
        //even if not set, empty means _all
        if (randomBoolean()) {
            request.indices("_all");
        }
        request.aliases("alias1");
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all resolved indices and aliases gets returned
        String[] expectedIndices = new String[]{"bar", "foofoobar", "foofoo", "alias1"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        String[] replacedIndices = new String[]{"bar", "foofoobar", "foofoo"};
        //_all gets replaced with all indices that user is authorized for
        assertThat(request.indices(), arrayContainingInAnyOrder(replacedIndices));
        assertThat(request.aliases(), arrayContainingInAnyOrder("alias1"));
    }

    public void testAllGetAliasesRequestNoAuthorizedIndicesAllowNoIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), true, true, randomBoolean()));
        request.aliases("alias1");
        request.indices("_all");
        assertNoIndices(request, resolveIndices(request,
                buildAuthorizedIndices(userNoIndices, GetAliasesAction.NAME)));
    }

    public void testAllGetAliasesRequestNoAuthorizedIndicesDisallowNoIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), false, true, randomBoolean()));
        request.aliases("alias1");
        request.indices("_all");
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(userNoIndices, GetAliasesAction.NAME)));
        assertEquals("no such index", e.getMessage());
    }

    public void testWildcardsGetAliasesRequestNoAuthorizedIndicesAllowNoIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.aliases("alias1");
        request.indices("foo*");
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), true, true, randomBoolean()));
        assertNoIndices(request, resolveIndices(request,
                buildAuthorizedIndices(userNoIndices, GetAliasesAction.NAME)));
    }

    public void testWildcardsGetAliasesRequestNoAuthorizedIndicesDisallowNoIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), false, true, randomBoolean()));
        request.aliases("alias1");
        request.indices("foo*");
        //current user is not authorized for any index, foo* resolves to no indices, the request fails
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(userNoIndices, GetAliasesAction.NAME)));
        assertEquals("no such index", e.getMessage());
    }

    public void testResolveAllAliasesGetAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest();
        if (randomBoolean()) {
            request.aliases("_all");
        }
        if (randomBoolean()) {
            request.indices("_all");
        }
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all resolved indices and aliases gets returned
        String[] expectedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foofoo", "foofoo-closed"};
        assertSameValues(indices, expectedIndices);
        //_all gets replaced with all indices that user is authorized for
        assertThat(request.indices(), arrayContainingInAnyOrder(expectedIndices));
        assertThat(request.aliases(), arrayContainingInAnyOrder("foofoobar"));
    }

    public void testResolveAllAndExplicitAliasesGetAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest(new String[]{"_all", "explicit"});
        if (randomBoolean()) {
            request.indices("_all");
        }
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all resolved indices and aliases gets returned
        String[] expectedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foofoo", "foofoo-closed", "explicit"};
        assertSameValues(indices, expectedIndices);
        //_all gets replaced with all indices that user is authorized for
        assertThat(request.indices(), arrayContainingInAnyOrder("bar", "bar-closed", "foofoobar", "foofoo", "foofoo-closed"));
        assertThat(request.aliases(), arrayContainingInAnyOrder("foofoobar", "explicit"));
    }

    public void testResolveAllAndWildcardsAliasesGetAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest(new String[]{"_all", "foo*", "non_matching_*"});
        if (randomBoolean()) {
            request.indices("_all");
        }
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //the union of all resolved indices and aliases gets returned
        String[] expectedIndices = new String[]{"bar", "bar-closed", "foofoobar", "foofoo", "foofoo-closed"};
        assertSameValues(indices, expectedIndices);
        //_all gets replaced with all indices that user is authorized for
        assertThat(request.indices(), arrayContainingInAnyOrder(expectedIndices));
        assertThat(request.aliases(), arrayContainingInAnyOrder("foofoobar", "foofoobar"));
    }

    public void testResolveAliasesWildcardsGetAliasesRequest() {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indices("*bar");
        request.aliases("foo*");
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, GetAliasesAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        //union of all resolved indices and aliases gets returned, based on what user is authorized for
        //note that the index side will end up containing matching aliases too, which is fine, as es core would do
        //the same and resolve those aliases to their corresponding concrete indices (which we let core do)
        String[] expectedIndices = new String[]{"bar", "foofoobar"};
        assertSameValues(indices, expectedIndices);
        //alias foofoobar on both sides, that's fine, es core would do the same, same as above
        assertThat(request.indices(), arrayContainingInAnyOrder("bar", "foofoobar"));
        assertThat(request.aliases(), arrayContainingInAnyOrder("foofoobar"));
    }

    public void testResolveAliasesWildcardsGetAliasesRequestNoAuthorizedIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        //no authorized aliases match bar*, hence the request fails
        request.aliases("bar*");
        request.indices("*bar");
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(user, GetAliasesAction.NAME)));
        assertEquals("no such index", e.getMessage());
    }

    public void testResolveAliasesAllGetAliasesRequestNoAuthorizedIndices() {
        GetAliasesRequest request = new GetAliasesRequest();
        if (randomBoolean()) {
            request.aliases("_all");
        }
        request.indices("non_existing");
        //current user is not authorized for any index, foo* resolves to no indices, the request fails
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(userNoIndices, GetAliasesAction.NAME)));
        assertEquals("no such index", e.getMessage());
    }

    /**
     * Tests that all the request types that are known to support remote indices successfully pass them through
     *  the resolver
     */
    public void testRemotableRequestsAllowRemoteIndices() {
        IndicesOptions options = IndicesOptions.fromOptions(true, false, false, false);
        Tuple<TransportRequest, String> tuple = randomFrom(
                new Tuple<>(new SearchRequest("remote:foo").indicesOptions(options), SearchAction.NAME),
                new Tuple<>(new FieldCapabilitiesRequest().indices("remote:foo").indicesOptions(options), FieldCapabilitiesAction.NAME),
                new Tuple<>(new GraphExploreRequest().indices("remote:foo").indicesOptions(options), GraphExploreAction.NAME)
        );
        final TransportRequest request = tuple.v1();
        ResolvedIndices resolved = resolveIndices(request, buildAuthorizedIndices(user, tuple.v2()));
        assertThat(resolved.getRemote(), containsInAnyOrder("remote:foo"));
        assertThat(resolved.getLocal(), emptyIterable());
        assertThat(((IndicesRequest) request).indices(), arrayContaining("remote:foo"));
    }

    /**
     * Tests that request types that do not support remote indices will be resolved as if all index names are local.
     */
    public void testNonRemotableRequestDoesNotAllowRemoteIndices() {
        IndicesOptions options = IndicesOptions.fromOptions(true, false, false, false);
        Tuple<TransportRequest, String> tuple = randomFrom(
                new Tuple<>(new CloseIndexRequest("remote:foo").indicesOptions(options), CloseIndexAction.NAME),
                new Tuple<>(new DeleteIndexRequest("remote:foo").indicesOptions(options), DeleteIndexAction.NAME),
                new Tuple<>(new PutMappingRequest("remote:foo").indicesOptions(options), PutMappingAction.NAME)
        );
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(tuple.v1(), buildAuthorizedIndices(user, tuple.v2())).getLocal());
        assertEquals("no such index", e.getMessage());
    }

    public void testNonRemotableRequestDoesNotAllowRemoteWildcardIndices() {
        IndicesOptions options = IndicesOptions.fromOptions(randomBoolean(), true, true, true);
        Tuple<TransportRequest, String> tuple = randomFrom(
                new Tuple<>(new CloseIndexRequest("*:*").indicesOptions(options), CloseIndexAction.NAME),
                new Tuple<>(new DeleteIndexRequest("*:*").indicesOptions(options), DeleteIndexAction.NAME),
                new Tuple<>(new PutMappingRequest("*:*").indicesOptions(options), PutMappingAction.NAME)
        );
        final ResolvedIndices resolved = resolveIndices(tuple.v1(), buildAuthorizedIndices(user, tuple.v2()));
        assertNoIndices((IndicesRequest.Replaceable) tuple.v1(), resolved);
    }

    public void testCompositeIndicesRequestIsNotSupported() {
        TransportRequest request = randomFrom(new MultiSearchRequest(), new MultiGetRequest(),
                new MultiTermVectorsRequest(), new BulkRequest());
        expectThrows(IllegalStateException.class, () -> resolveIndices(request,
                buildAuthorizedIndices(user, MultiSearchAction.NAME)));
    }

    public void testResolveAdminAction() {
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, DeleteIndexAction.NAME);
        {
            RefreshRequest request = new RefreshRequest("*");
            List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
            String[] expectedIndices = new String[]{"bar", "foofoobar", "foofoo"};
            assertThat(indices.size(), equalTo(expectedIndices.length));
            assertThat(indices, hasItems(expectedIndices));
            assertThat(request.indices(), arrayContainingInAnyOrder(expectedIndices));
        }
        {
            DeleteIndexRequest request = new DeleteIndexRequest("*");
            List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
            String[] expectedIndices = new String[]{"bar", "bar-closed", "foofoo", "foofoo-closed"};
            assertThat(indices.size(), equalTo(expectedIndices.length));
            assertThat(indices, hasItems(expectedIndices));
            assertThat(request.indices(), arrayContainingInAnyOrder(expectedIndices));
        }
    }

    public void testIndicesExists() {
        //verify that the ignore_unavailable and allow_no_indices get replaced like es core does, to make sure that
        //indices exists api never throws exception due to missing indices, but only returns false instead.
        {
            IndicesExistsRequest request = new IndicesExistsRequest();
            assertNoIndices(request, resolveIndices(request,
                    buildAuthorizedIndices(userNoIndices, IndicesExistsAction.NAME)));
        }

        {
            IndicesExistsRequest request = new IndicesExistsRequest("does_not_exist");

            assertNoIndices(request, resolveIndices(request,
                    buildAuthorizedIndices(user, IndicesExistsAction.NAME)));
        }
        {
            IndicesExistsRequest request = new IndicesExistsRequest("does_not_exist_*");
            assertNoIndices(request, resolveIndices(request,
                    buildAuthorizedIndices(user, IndicesExistsAction.NAME)));
        }
    }

    public void testXPackSecurityUserHasAccessToSecurityIndex() {
        SearchRequest request = new SearchRequest();
        {
            final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(XPackSecurityUser.INSTANCE, SearchAction.NAME);
            List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
            assertThat(indices, hasItem(SecurityIndexManager.SECURITY_INDEX_NAME));
        }
        {
            IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest();
            aliasesRequest.addAliasAction(AliasActions.add().alias("security_alias").index(SECURITY_INDEX_NAME));
            final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(XPackSecurityUser.INSTANCE, IndicesAliasesAction.NAME);
            List<String> indices = resolveIndices(aliasesRequest, authorizedIndices).getLocal();
            assertThat(indices, hasItem(SecurityIndexManager.SECURITY_INDEX_NAME));
        }
    }

    public void testXPackUserDoesNotHaveAccessToSecurityIndex() {
        SearchRequest request = new SearchRequest();
        final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(XPackUser.INSTANCE, SearchAction.NAME);
        List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
        assertThat(indices, not(hasItem(SecurityIndexManager.SECURITY_INDEX_NAME)));
    }

    public void testNonXPackUserAccessingSecurityIndex() {
        User allAccessUser = new User("all_access", "all_access");
        roleMap.put("all_access", new RoleDescriptor("all_access", new String[] { "all" },
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("all").build() }, null));

        {
            SearchRequest request = new SearchRequest();
            final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(allAccessUser, SearchAction.NAME);
            List<String> indices = resolveIndices(request, authorizedIndices).getLocal();
            assertThat(indices, not(hasItem(SecurityIndexManager.SECURITY_INDEX_NAME)));
        }

        {
            IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest();
            aliasesRequest.addAliasAction(AliasActions.add().alias("security_alias1").index("*"));
            final AuthorizedIndices authorizedIndices = buildAuthorizedIndices(allAccessUser, IndicesAliasesAction.NAME);
            List<String> indices = resolveIndices(aliasesRequest, authorizedIndices).getLocal();
            assertThat(indices, not(hasItem(SecurityIndexManager.SECURITY_INDEX_NAME)));
        }
    }

    public void testUnauthorizedDateMathExpressionIgnoreUnavailable() {
        SearchRequest request = new SearchRequest("<datetime-{now/M}>");
        request.indicesOptions(IndicesOptions.fromOptions(true, true, randomBoolean(), randomBoolean()));
        assertNoIndices(request, resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)));
    }

    public void testUnauthorizedDateMathExpressionIgnoreUnavailableDisallowNoIndices() {
        SearchRequest request = new SearchRequest("<datetime-{now/M}>");
        request.indicesOptions(IndicesOptions.fromOptions(true, false, randomBoolean(), randomBoolean()));
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)));
        assertEquals("no such index" , e.getMessage());
    }

    public void testUnauthorizedDateMathExpressionStrict() {
        SearchRequest request = new SearchRequest("<datetime-{now/M}>");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), randomBoolean(), randomBoolean()));
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)));
        assertEquals("no such index" , e.getMessage());
    }

    public void testResolveDateMathExpression() {
        // make the user authorized
        final String pattern = randomBoolean() ? "<datetime-{now/M}>" : "<datetime-{now/M}*>";
        String dateTimeIndex = indexNameExpressionResolver.resolveDateMathExpression("<datetime-{now/M}>");
        String[] authorizedIndices = new String[] { "bar", "bar-closed", "foofoobar", "foofoo", "missing", "foofoo-closed", dateTimeIndex};
        roleMap.put("role", new RoleDescriptor("role", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices(authorizedIndices).privileges("all").build() }, null));

        SearchRequest request = new SearchRequest(pattern);
        if (randomBoolean()) {
            final boolean expandIndicesOpen = Regex.isSimpleMatchPattern(pattern) ? true : randomBoolean();
            request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), expandIndicesOpen, randomBoolean()));
        }
        List<String> indices = resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)).getLocal();
        assertThat(indices.size(), equalTo(1));
        assertThat(request.indices()[0], equalTo(dateTimeIndex));
    }

    public void testMissingDateMathExpressionIgnoreUnavailable() {
        SearchRequest request = new SearchRequest("<foobar-{now/M}>");
        request.indicesOptions(IndicesOptions.fromOptions(true, true, randomBoolean(), randomBoolean()));
        assertNoIndices(request, resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)));
    }

    public void testMissingDateMathExpressionIgnoreUnavailableDisallowNoIndices() {
        SearchRequest request = new SearchRequest("<foobar-{now/M}>");
        request.indicesOptions(IndicesOptions.fromOptions(true, false, randomBoolean(), randomBoolean()));
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)));
        assertEquals("no such index" , e.getMessage());
    }

    public void testMissingDateMathExpressionStrict() {
        SearchRequest request = new SearchRequest("<foobar-{now/M}>");
        request.indicesOptions(IndicesOptions.fromOptions(false, randomBoolean(), randomBoolean(), randomBoolean()));
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> resolveIndices(request, buildAuthorizedIndices(user, SearchAction.NAME)));
        assertEquals("no such index" , e.getMessage());
    }

    public void testAliasDateMathExpressionNotSupported() {
        // make the user authorized
        String[] authorizedIndices = new String[] { "bar", "bar-closed", "foofoobar", "foofoo", "missing", "foofoo-closed",
                indexNameExpressionResolver.resolveDateMathExpression("<datetime-{now/M}>")};
        roleMap.put("role", new RoleDescriptor("role", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices(authorizedIndices).privileges("all").build() }, null));
        GetAliasesRequest request = new GetAliasesRequest("<datetime-{now/M}>").indices("foo", "foofoo");
        List<String> indices =
                resolveIndices(request, buildAuthorizedIndices(user, GetAliasesAction.NAME)).getLocal();
        //the union of all indices and aliases gets returned
        String[] expectedIndices = new String[]{"<datetime-{now/M}>", "foo", "foofoo"};
        assertThat(indices.size(), equalTo(expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
        assertThat(request.indices(), arrayContainingInAnyOrder("foo", "foofoo"));
        assertThat(request.aliases(), arrayContainingInAnyOrder("<datetime-{now/M}>"));
    }

    public void testDynamicPutMappingRequestFromAlias() {
        PutMappingRequest request = new PutMappingRequest(Strings.EMPTY_ARRAY).setConcreteIndex(new Index("foofoo", UUIDs.base64UUID()));
        User user = new User("alias-writer", "alias_read_write");
        AuthorizedIndices authorizedIndices = buildAuthorizedIndices(user, PutMappingAction.NAME);

        String putMappingIndexOrAlias = IndicesAndAliasesResolver.getPutMappingIndexOrAlias(request, authorizedIndices, metaData);
        assertEquals("barbaz", putMappingIndexOrAlias);

        // multiple indices map to an alias so we can only return the concrete index
        final String index = randomFrom("foo", "foobar");
        request = new PutMappingRequest(Strings.EMPTY_ARRAY).setConcreteIndex(new Index(index, UUIDs.base64UUID()));
        putMappingIndexOrAlias = IndicesAndAliasesResolver.getPutMappingIndexOrAlias(request, authorizedIndices, metaData);
        assertEquals(index, putMappingIndexOrAlias);
    }

    // TODO with the removal of DeleteByQuery is there another way to test resolving a write action?


    private AuthorizedIndices buildAuthorizedIndices(User user, String action) {
        PlainActionFuture<Role> rolesListener = new PlainActionFuture<>();
        authzService.roles(user, rolesListener);
        return new AuthorizedIndices(user, rolesListener.actionGet(), action, metaData);
    }

    public static IndexMetaData.Builder indexBuilder(String index) {
        return IndexMetaData.builder(index).settings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
    }

    private ResolvedIndices resolveIndices(TransportRequest request, AuthorizedIndices authorizedIndices) {
        return defaultIndicesResolver.resolve(request, this.metaData, authorizedIndices);
    }

    private static void assertNoIndices(IndicesRequest.Replaceable request, ResolvedIndices resolvedIndices) {
        final List<String> localIndices = resolvedIndices.getLocal();
        assertEquals(1, localIndices.size());
        assertEquals(IndicesAndAliasesResolverField.NO_INDEX_PLACEHOLDER, localIndices.iterator().next());
        assertEquals(IndicesAndAliasesResolver.NO_INDICES_LIST, Arrays.asList(request.indices()));
        assertEquals(0, resolvedIndices.getRemote().size());
    }

    private void assertSameValues(List<String> indices, String[] expectedIndices) {
        assertThat(indices.stream().distinct().count(), equalTo((long)expectedIndices.length));
        assertThat(indices, hasItems(expectedIndices));
    }
}
