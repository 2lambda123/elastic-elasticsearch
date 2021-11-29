/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.user.InternalUserSerializationHelper;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

// TODO(hub-cap) Clean this up after moving User over - This class can re-inherit its field AUTHENTICATION_KEY in AuthenticationField.
// That interface can be removed
public class Authentication implements ToXContentObject {

    public static final Version VERSION_API_KEY_ROLES_AS_BYTES = Version.V_7_9_0;

    private final User user;
    private final RealmRef authenticatedBy;
    private final RealmRef lookedUpBy;
    private final Version version;
    private final AuthenticationType type;
    private final Map<String, Object> metadata; // authentication contains metadata, includes api_key details (including api_key metadata)

    public Authentication(User user, RealmRef authenticatedBy, RealmRef lookedUpBy) {
        this(user, authenticatedBy, lookedUpBy, Version.CURRENT);
    }

    public Authentication(User user, RealmRef authenticatedBy, RealmRef lookedUpBy, Version version) {
        this(user, authenticatedBy, lookedUpBy, version, AuthenticationType.REALM, Collections.emptyMap());
    }

    public Authentication(
        User user,
        RealmRef authenticatedBy,
        RealmRef lookedUpBy,
        Version version,
        AuthenticationType type,
        Map<String, Object> metadata
    ) {
        this.user = Objects.requireNonNull(user);
        this.authenticatedBy = Objects.requireNonNull(authenticatedBy);
        this.lookedUpBy = lookedUpBy;
        this.version = version;
        this.type = type;
        this.metadata = metadata;
        this.assertApiKeyMetadata();
    }

    public Authentication(StreamInput in) throws IOException {
        this.user = InternalUserSerializationHelper.readFrom(in);
        this.authenticatedBy = new RealmRef(in);
        if (in.readBoolean()) {
            this.lookedUpBy = new RealmRef(in);
        } else {
            this.lookedUpBy = null;
        }
        this.version = in.getVersion();
        type = AuthenticationType.values()[in.readVInt()];
        metadata = in.readMap();
        this.assertApiKeyMetadata();
    }

    public User getUser() {
        return user;
    }

    public RealmRef getAuthenticatedBy() {
        return authenticatedBy;
    }

    public RealmRef getLookedUpBy() {
        return lookedUpBy;
    }

    /**
     * Get the realm where the effective user comes from.
     * The effective user is the es-security-runas-user if present or the authenticated user.
     */
    public RealmRef getSourceRealm() {
        return lookedUpBy == null ? authenticatedBy : lookedUpBy;
    }

    public Version getVersion() {
        return version;
    }

    public AuthenticationType getAuthenticationType() {
        return type;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public boolean isAuthenticatedWithServiceAccount() {
        return ServiceAccountSettings.REALM_TYPE.equals(getAuthenticatedBy().getType());
    }

    public boolean isAuthenticatedWithApiKey() {
        return AuthenticationType.API_KEY.equals(getAuthenticationType());
    }

    /**
     * Writes the authentication to the context. There must not be an existing authentication in the context and if there is an
     * {@link IllegalStateException} will be thrown
     */
    public void writeToContext(ThreadContext ctx) throws IOException, IllegalArgumentException {
        new AuthenticationContextSerializer().writeToContext(this, ctx);
    }

    public String encode() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(version);
        Version.writeVersion(version, output);
        writeTo(output);
        return Base64.getEncoder().encodeToString(BytesReference.toBytes(output.bytes()));
    }

    public void writeTo(StreamOutput out) throws IOException {
        InternalUserSerializationHelper.writeTo(user, out);
        authenticatedBy.writeTo(out);
        if (lookedUpBy != null) {
            out.writeBoolean(true);
            lookedUpBy.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeVInt(type.ordinal());
        out.writeMap(metadata);
    }

    /**
     * Checks whether the user or API key of the passed in authentication can access the resources owned by the user
     * or API key of this authentication. The rules are as follows:
     *   * True if the authentications are for the same API key (same API key ID)
     *   * True if they are the same username from the same realm
     *      - For file and native realm, same realm means the same realm type
     *      - For all other realms, same realm means same realm type plus same realm name
     *   * An user and its API key cannot access each other's resources
     *   * An user and its token can access each other's resources
     *   * Two API keys are never able to access each other's resources regardless of their ownership.
     *
     *  This check is a best effort and it does not account for certain static and external changes.
     *  See also <a href="https://www.elastic.co/guide/en/elasticsearch/reference/master/security-limitations.html">
     *      security limitations</a>
     */
    public boolean canAccessResourcesOf(Authentication other) {
        if (AuthenticationType.API_KEY == getAuthenticationType() && AuthenticationType.API_KEY == other.getAuthenticationType()) {
            final boolean sameKeyId = getMetadata().get(AuthenticationField.API_KEY_ID_KEY)
                .equals(other.getMetadata().get(AuthenticationField.API_KEY_ID_KEY));
            if (sameKeyId) {
                assert getUser().principal().equals(other.getUser().principal())
                    : "The same API key ID cannot be attributed to two different usernames";
            }
            return sameKeyId;
        }

        if (getAuthenticationType().equals(other.getAuthenticationType())
            || (AuthenticationType.REALM == getAuthenticationType() && AuthenticationType.TOKEN == other.getAuthenticationType())
            || (AuthenticationType.TOKEN == getAuthenticationType() && AuthenticationType.REALM == other.getAuthenticationType())) {
            if (false == getUser().principal().equals(other.getUser().principal())) {
                return false;
            }
            final RealmRef thisRealm = getSourceRealm();
            final RealmRef otherRealm = other.getSourceRealm();
            // Domain only works when both realms have it explicitly configured
            if (thisRealm.getDomain() != null && thisRealm.getDomain().equals(otherRealm.getDomain())) {
                return true;
            } else {
                if (FileRealmSettings.TYPE.equals(thisRealm.getType()) || NativeRealmSettings.TYPE.equals(thisRealm.getType())) {
                    return thisRealm.getType().equals(otherRealm.getType());
                }
                return thisRealm.getName().equals(otherRealm.getName()) && thisRealm.getType().equals(otherRealm.getType());
            }
        } else {
            assert EnumSet.of(
                AuthenticationType.REALM,
                AuthenticationType.API_KEY,
                AuthenticationType.TOKEN,
                AuthenticationType.ANONYMOUS,
                AuthenticationType.INTERNAL
            ).containsAll(EnumSet.of(getAuthenticationType(), other.getAuthenticationType()))
                : "cross AuthenticationType comparison for canAccessResourcesOf is not applicable for: "
                    + EnumSet.of(getAuthenticationType(), other.getAuthenticationType());
            return false;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Authentication that = (Authentication) o;
        return user.equals(that.user)
            && authenticatedBy.equals(that.authenticatedBy)
            && Objects.equals(lookedUpBy, that.lookedUpBy)
            && version.equals(that.version)
            && type == that.type
            && metadata.equals(that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, authenticatedBy, lookedUpBy, version, type, metadata);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragment(builder);
        return builder.endObject();
    }

    /**
     * Generates XContent without the start/end object.
     */
    public void toXContentFragment(XContentBuilder builder) throws IOException {
        builder.field(User.Fields.USERNAME.getPreferredName(), user.principal());
        builder.array(User.Fields.ROLES.getPreferredName(), user.roles());
        builder.field(User.Fields.FULL_NAME.getPreferredName(), user.fullName());
        builder.field(User.Fields.EMAIL.getPreferredName(), user.email());
        if (isAuthenticatedWithServiceAccount()) {
            final String tokenName = (String) getMetadata().get(ServiceAccountSettings.TOKEN_NAME_FIELD);
            assert tokenName != null : "token name cannot be null";
            final String tokenSource = (String) getMetadata().get(ServiceAccountSettings.TOKEN_SOURCE_FIELD);
            assert tokenSource != null : "token source cannot be null";
            builder.field(
                User.Fields.TOKEN.getPreferredName(),
                Map.of("name", tokenName, "type", ServiceAccountSettings.REALM_TYPE + "_" + tokenSource)
            );
        }
        builder.field(User.Fields.METADATA.getPreferredName(), user.metadata());
        builder.field(User.Fields.ENABLED.getPreferredName(), user.enabled());
        builder.startObject(User.Fields.AUTHENTICATION_REALM.getPreferredName());
        builder.field(User.Fields.REALM_NAME.getPreferredName(), getAuthenticatedBy().getName());
        builder.field(User.Fields.REALM_TYPE.getPreferredName(), getAuthenticatedBy().getType());
        if (getAuthenticatedBy().getDomain() != null) {
            builder.field(User.Fields.REALM_DOMAIN.getPreferredName(), getAuthenticatedBy().getDomain());
        }
        builder.endObject();
        builder.startObject(User.Fields.LOOKUP_REALM.getPreferredName());
        if (getLookedUpBy() != null) {
            builder.field(User.Fields.REALM_NAME.getPreferredName(), getLookedUpBy().getName());
            builder.field(User.Fields.REALM_TYPE.getPreferredName(), getLookedUpBy().getType());
            if (getLookedUpBy().getDomain() != null) {
                builder.field(User.Fields.REALM_DOMAIN.getPreferredName(), getLookedUpBy().getDomain());
            }
        } else {
            builder.field(User.Fields.REALM_NAME.getPreferredName(), getAuthenticatedBy().getName());
            builder.field(User.Fields.REALM_TYPE.getPreferredName(), getAuthenticatedBy().getType());
            if (getAuthenticatedBy().getDomain() != null) {
                builder.field(User.Fields.REALM_DOMAIN.getPreferredName(), getAuthenticatedBy().getDomain());
            }
        }
        builder.endObject();
        builder.field(User.Fields.AUTHENTICATION_TYPE.getPreferredName(), getAuthenticationType().name().toLowerCase(Locale.ROOT));
        if (isAuthenticatedWithApiKey()) {
            this.assertApiKeyMetadata();
            final String apiKeyId = (String) this.metadata.get(AuthenticationField.API_KEY_ID_KEY);
            final String apiKeyName = (String) this.metadata.get(AuthenticationField.API_KEY_NAME_KEY);
            if (apiKeyName == null) {
                builder.field("api_key", Map.of("id", apiKeyId));
            } else {
                builder.field("api_key", Map.of("id", apiKeyId, "name", apiKeyName));
            }
        }
    }

    private void assertApiKeyMetadata() {
        assert (AuthenticationType.API_KEY.equals(this.type) == false) || (this.metadata.get(AuthenticationField.API_KEY_ID_KEY) != null)
            : "API KEY authentication requires metadata to contain API KEY id, and the value must be non-null.";
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("Authentication[").append(user)
            .append(",type=")
            .append(type)
            .append(",by=")
            .append(authenticatedBy);
        if (lookedUpBy != null) {
            builder.append(",lookup=").append(lookedUpBy);
        }
        builder.append("]");
        return builder.toString();
    }

    public static class RealmRef {

        private final String nodeName;
        private final String name;
        private final String type;
        private final String domain;

        public RealmRef(String name, String type, String nodeName, String domain) {
            this.nodeName = nodeName;
            this.name = name;
            this.type = type;
            this.domain = domain;
        }

        public RealmRef(String name, String type, String nodeName) {
            this(name, type, nodeName, null);
        }

        public RealmRef(StreamInput in) throws IOException {
            this.nodeName = in.readString();
            this.name = in.readString();
            this.type = in.readString();
            if (in.getVersion().onOrBefore(Version.V_8_0_0)) {
                this.domain = null;
            } else {
                this.domain = in.readOptionalString();
            }
        }

        void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeName);
            out.writeString(name);
            out.writeString(type);
            if (out.getVersion().onOrBefore(Version.V_8_0_0)) {
                if (domain != null) {
                    throw new IllegalArgumentException(
                        "realm domain is not supported by node of version [" + Version.V_8_0_0 + "] and earlier"
                    );
                }
            } else {
                out.writeOptionalString(domain);
            }
        }

        public String getNodeName() {
            return nodeName;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        @Nullable
        public String getDomain() {
            return domain;
        }

        public String getEffectiveDomain() {
            return domain != null ? domain : name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RealmRef realmRef = (RealmRef) o;

            if (nodeName.equals(realmRef.nodeName) == false) return false;
            if (name.equals(realmRef.name) == false) return false;
            if (type.equals(realmRef.type) == false) return false;
            return getEffectiveDomain().equals(realmRef.getEffectiveDomain());
        }

        @Override
        public int hashCode() {
            int result = nodeName.hashCode();
            result = 31 * result + name.hashCode();
            result = 31 * result + type.hashCode();
            result = 31 * result + getEffectiveDomain().hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "{Realm[" + type + "." + name + (domain != null ? ("." + domain) : "") + "] on Node[" + nodeName + "]}";
        }
    }

    public enum AuthenticationType {
        REALM,
        API_KEY,
        TOKEN,
        ANONYMOUS,
        INTERNAL
    }
}
