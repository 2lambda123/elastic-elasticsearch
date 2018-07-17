/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.support;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.Strings.collectionToDelimitedString;

/**
 * Utility class for supporting "delegated authorization" (aka "authorizing_realms", aka "lookup realms").
 * A {@link Realm} may support delegating authorization to another realm. It does this by registering a
 * setting for {@link DelegatedAuthorizationSettings#AUTHZ_REALMS}, and constructing an instance of this
 * class. Then, after the realm has performed any authentication steps, if {@link #hasDelegation()} is
 * {@code true}, it delegates the construction of the {@link User} object and {@link AuthenticationResult}
 * to {@link #resolve(String, ActionListener)}.
 */
public class DelegatedAuthorizationSupport {

    private final RealmUserLookup lookup;
    private final Logger logger;
    private final XPackLicenseState licenseState;

    /**
     * Resolves the {@link DelegatedAuthorizationSettings#AUTHZ_REALMS} setting from {@code config} and calls
     * {@link #DelegatedAuthorizationSupport(Iterable, List, ThreadContext, XPackLicenseState)}
     */
    public DelegatedAuthorizationSupport(Iterable<? extends Realm> allRealms, RealmConfig config, XPackLicenseState licenseState) {
        this(allRealms, DelegatedAuthorizationSettings.AUTHZ_REALMS.get(config.settings()), config.threadContext(), licenseState);
    }

    /**
     * Constructs a new object that delegates to the named realms ({@code lookupRealms}), which must exist within
     * {@code allRealms}.
     * @throws IllegalArgumentException if one of the specified realms does not exist
     */
    protected DelegatedAuthorizationSupport(Iterable<? extends Realm> allRealms, List<String> lookupRealms, ThreadContext threadContext,
                                            XPackLicenseState licenseState) {
       this.lookup = new RealmUserLookup(resolveRealms(allRealms, lookupRealms), threadContext);
       this.logger = Loggers.getLogger(getClass());
       this.licenseState = licenseState;
    }

    /**
     * Are there any realms configured for delegated lookup
     */
    public boolean hasDelegation() {
        return this.lookup.hasRealms();
    }

    /**
     * Attempts to find the user specified by {@code username} in one of the delegated realms.
     * The realms are searched in the order specified during construction.
     * Returns a {@link AuthenticationResult#success(User) successful result} if a {@link User}
     * was found, otherwise returns an
     * {@link AuthenticationResult#unsuccessful(String, Exception) unsuccessful result}
     * with a meaningful diagnostic message.
     */
    public void resolve(String username, ActionListener<AuthenticationResult> resultListener) {
        if (licenseState.isAuthorizingRealmAllowed() == false) {
            resultListener.onResponse(AuthenticationResult.unsuccessful(
                DelegatedAuthorizationSettings.AUTHZ_REALMS.getKey() + " are not permitted",
                LicenseUtils.newComplianceException(DelegatedAuthorizationSettings.AUTHZ_REALMS.getKey())
            ));
            return;
        }
        if (hasDelegation() == false) {
            resultListener.onResponse(AuthenticationResult.unsuccessful(
                "No [" + DelegatedAuthorizationSettings.AUTHZ_REALMS.getKey() + "] have been configured", null));
            return;
        }
        ActionListener<Tuple<User, Realm>> userListener = ActionListener.wrap(tuple -> {
            if (tuple != null) {
                logger.trace("Found user " + tuple.v1() + " in realm " + tuple.v2());
                resultListener.onResponse(AuthenticationResult.success(tuple.v1()));
            } else {
                resultListener.onResponse(AuthenticationResult.unsuccessful("the principal [" + username
                    + "] was authenticated, but no user could be found in realms [" + collectionToDelimitedString(lookup.getRealms(), ",")
                    + "]", null));
            }
        }, resultListener::onFailure);
        lookup.lookup(username, userListener);
    }

    private List<Realm> resolveRealms(Iterable<? extends Realm> allRealms, List<String> lookupRealms) {
        final List<Realm> result = new ArrayList<>(lookupRealms.size());
        for (String name : lookupRealms) {
            result.add(findRealm(name, allRealms));
        }
        assert result.size() == lookupRealms.size();
        return result;
    }

    private Realm findRealm(String name, Iterable<? extends Realm> allRealms) {
        for (Realm realm : allRealms) {
            if (name.equals(realm.name())) {
                return realm;
            }
        }
        throw new IllegalArgumentException("configured authorizing realm [" + name + "] does not exist (or is not enabled)");
    }

}
