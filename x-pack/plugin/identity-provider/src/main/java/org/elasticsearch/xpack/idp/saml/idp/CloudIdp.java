package org.elasticsearch.xpack.idp.saml.idp;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.X509KeyPairSettings;
import org.opensaml.security.x509.X509Credential;
import org.opensaml.security.x509.impl.X509KeyManagerX509CredentialAdapter;

import javax.net.ssl.X509KeyManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_ENTITY_ID;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_SIGNING_KEY_ALIAS;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_SLO_POST_ENDPOINT;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_SLO_REDIRECT_ENDPOINT;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_SSO_POST_ENDPOINT;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_SSO_REDIRECT_ENDPOINT;

public class CloudIdp implements SamlIdentityProvider {

    private final String entityId;
    private final HashMap<String, String> ssoEndpoints = new HashMap<>();
    private final HashMap<String, String> sloEndpoints = new HashMap<>();
    private final X509Credential signingCredential;

    public CloudIdp(Environment env, Settings settings) {
        this.entityId = require(settings, IDP_ENTITY_ID);
        this.ssoEndpoints.put("redirect", require(settings, IDP_SSO_REDIRECT_ENDPOINT));
        this.ssoEndpoints.computeIfAbsent("post", v -> settings.get(IDP_SSO_POST_ENDPOINT.getKey()));
        this.sloEndpoints.computeIfAbsent("post", v -> settings.get(IDP_SLO_POST_ENDPOINT.getKey()));
        this.sloEndpoints.computeIfAbsent("redirect", v -> settings.get(IDP_SLO_REDIRECT_ENDPOINT.getKey()));
        this.signingCredential = buildSigningCredential(env, settings);
    }

    @Override
    public String getEntityId() {
        return entityId;
    }

    @Override
    public String getSingleSignOnEndpoint(String binding) {
        return ssoEndpoints.get(binding);
    }

    @Override
    public String getSingleLogoutEndpoint(String binding) {
        return sloEndpoints.get(binding);
    }

    @Override
    public X509Credential getSigningCredential() {
        return signingCredential;
    }

    private static String require(Settings settings, Setting<String> setting) {
        if (settings.hasValue(setting.getKey())) {
            return setting.get(settings);
        } else {
            throw new SettingsException("The configuration setting [" + setting.getKey() + "] is required");
        }
    }

    static X509Credential buildSigningCredential(Environment env, Settings settings) {
        final X509KeyPairSettings keyPairSettings = X509KeyPairSettings.withPrefix("xpack.idp.signing.", false);
        final X509KeyManager keyManager = CertParsingUtils.getKeyManager(keyPairSettings, settings, null, env);
        if (keyManager == null) {
            return null;
        }
        final Set<String> aliases = new HashSet<>();
        final String configAlias = IDP_SIGNING_KEY_ALIAS.get(settings);
        if (Strings.isNullOrEmpty(configAlias)) {
            final String[] rsaAliases = keyManager.getServerAliases("RSA", null);
            if (null != rsaAliases) {
                aliases.addAll(Arrays.asList(rsaAliases));
            }
            final String[] ecAliases = keyManager.getServerAliases("EC", null);
            if (null != ecAliases) {
                aliases.addAll(Arrays.asList(ecAliases));
            }
            if (aliases.isEmpty()) {
                throw new IllegalArgumentException("The configured keystore for xpack.idp.signing.keystore does not contain any RSA or EC" +
                    " key pairs");
            }
            if (aliases.size() > 1) {
                throw new IllegalArgumentException("The configured keystore for xpack.idp.signing.keystore contains multiple key pairs" +
                    " but no alias has been configured with [" + IDP_SIGNING_KEY_ALIAS.getKey() + "]");
            }
        } else {
            aliases.add(configAlias);
        }
        String alias = new ArrayList<>(aliases).get(0);
        if (keyManager.getPrivateKey(alias) == null) {
            throw new IllegalArgumentException("The configured keystore for xpack.idp.signing.keystore does not have a private key" +
                " associated with alias [" + alias + "]");
        }

        final String keyType = keyManager.getPrivateKey(alias).getAlgorithm();
        if (keyType.equals("RSA") == false && keyType.equals("EC") == false) {
            throw new IllegalArgumentException("The key associated with alias [" + alias + "] " + "that has been configured with ["
                + IDP_SIGNING_KEY_ALIAS.getKey() + "] uses unsupported key algorithm type [" + keyType
                + "], only RSA and EC are supported");
        }
        return new X509KeyManagerX509CredentialAdapter(keyManager, alias);
    }


}
