/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.saml.authn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.internal.io.Streams;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.xpack.idp.action.SamlValidateAuthnRequestResponse;
import org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProvider;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProvider;
import org.elasticsearch.xpack.idp.saml.support.SamlFactory;
import org.elasticsearch.xpack.idp.saml.support.SamlInit;
import org.opensaml.saml.saml2.core.AuthnRequest;
import org.opensaml.saml.saml2.core.Issuer;
import org.opensaml.saml.saml2.core.NameIDPolicy;
import org.opensaml.security.x509.X509Credential;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import static org.opensaml.saml.common.xml.SAMLConstants.SAML2_REDIRECT_BINDING_URI;
import static org.opensaml.saml.saml2.core.NameIDType.UNSPECIFIED;

/*
 * Processes a SAML AuthnRequest, validates it and extracts necessary information
 */
public class SamlAuthnRequestValidator {

    private final SamlFactory samlFactory;
    private final SamlIdentityProvider idp;
    private final Logger logger = LogManager.getLogger(SamlAuthnRequestValidator.class);
    private static final String[] XSD_FILES = new String[]{"/org/elasticsearch/xpack/idp/saml/support/saml-schema-protocol-2.0.xsd",
        "/org/elasticsearch/xpack/idp/saml/support/saml-schema-assertion-2.0.xsd",
        "/org/elasticsearch/xpack/idp/saml/support/xenc-schema.xsd",
        "/org/elasticsearch/xpack/idp/saml/support/xmldsig-core-schema.xsd"};

    private static final ThreadLocal<DocumentBuilder> THREAD_LOCAL_DOCUMENT_BUILDER = ThreadLocal.withInitial(() -> {
        try {
            return SamlFactory.getHardenedBuilder(XSD_FILES);
        } catch (Exception e) {
            throw new ElasticsearchSecurityException("Could not load XSD schema file", e);
        }
    });

    public SamlAuthnRequestValidator(SamlFactory samlFactory, SamlIdentityProvider idp) {
        SamlInit.initialize();
        this.samlFactory = samlFactory;
        this.idp = idp;
    }

    public void processQueryString(String queryString, ActionListener<SamlValidateAuthnRequestResponse> listener) {

        final ParsedQueryString parsedQueryString;
        try {
            parsedQueryString = parseQueryString(queryString);
        } catch (ElasticsearchSecurityException e) {
            logger.debug("Failed to parse query string for SAML AuthnRequest", e);
            listener.onFailure(e);
            return;
        }

        try {
            // We consciously parse the AuthnRequest before we validate its signature as we need to get the Issuer, in order to
            // verify if we know of this SP and get its credentials for signature verification
            final Element root = parseSamlMessage(inflate(decodeBase64(parsedQueryString.samlRequest)));
            if (samlFactory.elementNameMatches(root, "urn:oasis:names:tc:SAML:2.0:protocol", "AuthnRequest") == false) {
                logAndRespond(new ParameterizedMessage("SAML message [{}] is not an AuthnRequest", samlFactory.text(root, 128)), listener);
                return;
            }
            final AuthnRequest authnRequest = samlFactory.buildXmlObject(root, AuthnRequest.class);
            getSpFromIssuer(authnRequest.getIssuer(), ActionListener.wrap(
                sp -> {
                    try {
                        validateAuthnRequest(authnRequest, sp, parsedQueryString, listener);
                    } catch (ElasticsearchSecurityException e) {
                        logger.debug("Could not validate AuthnRequest", e);
                        listener.onFailure(e);
                    } catch (Exception e) {
                        logAndRespond("Could not validate AuthnRequest", e, listener);
                    }
                },
                listener::onFailure
            ));
        } catch (ElasticsearchSecurityException e) {
            logger.debug("Could not process AuthnRequest", e);
            listener.onFailure(e);
        } catch (Exception e) {
            logAndRespond("Could not process AuthnRequest", e, listener);
        }
    }

    private ParsedQueryString parseQueryString(String queryString) throws ElasticsearchSecurityException {

        final Map<String, String> parameters = new HashMap<>();
        RestUtils.decodeQueryString(queryString, 0, parameters);
        if (parameters.isEmpty()) {
            throw new ElasticsearchSecurityException("Invalid Authentication Request query string (zero parameters)");
        }
        logger.trace(new ParameterizedMessage("Parsed the following parameters from the query string: {}", parameters));
        final String samlRequest = parameters.get("SAMLRequest");
        if (null == samlRequest) {
            throw new ElasticsearchSecurityException("Query string [{}] does not contain a SAMLRequest parameter", queryString);
        }
        return new ParsedQueryString(
            queryString,
            samlRequest,
            parameters.get("RelayState"),
            parameters.get("SigAlg"),
            parameters.get("Signature"));
    }

    private void validateAuthnRequest(AuthnRequest authnRequest, SamlServiceProvider sp, ParsedQueryString parsedQueryString,
                                         ActionListener<SamlValidateAuthnRequestResponse> listener) {
        // If the Service Provider should not sign requests, do not try to handle signatures even if they are added to the request
        if (sp.shouldSignAuthnRequests()) {
            if (Strings.hasText(parsedQueryString.signature)) {
                if (Strings.hasText(parsedQueryString.sigAlg) == false) {
                    logAndRespond(new ParameterizedMessage("Query string [{}] contains a Signature but SigAlg parameter is missing",
                        parsedQueryString.queryString), listener);
                    return;
                }
                final Set<X509Credential> spSigningCredentials = sp.getSpSigningCredentials();
                if (spSigningCredentials == null || spSigningCredentials.isEmpty()) {
                    logAndRespond(
                        "Unable to validate signature of authentication request, " +
                            "Service Provider hasn't registered signing credentials",
                        listener);
                    return;
                }
                if (validateSignature(parsedQueryString, spSigningCredentials) == false) {
                    logAndRespond(
                        new ParameterizedMessage("Unable to validate signature of authentication request [{}] using credentials [{}]",
                            parsedQueryString.queryString,
                            samlFactory.describeCredentials(spSigningCredentials)), listener);
                    return;
                }
            } else if (Strings.hasText(parsedQueryString.sigAlg)) {
                logAndRespond(new ParameterizedMessage("Query string [{}] contains a SigAlg parameter but Signature is missing",
                    parsedQueryString.queryString), listener);
                return;
            } else {
                logAndRespond("The Service Provider must sign authentication requests but no signature was found", listener);
                return;
            }
        }

        checkDestination(authnRequest);
        checkAcs(authnRequest, sp);

        Map<String, Object> authnState = buildAuthnState(authnRequest, sp);
        final SamlValidateAuthnRequestResponse response = new SamlValidateAuthnRequestResponse(sp.getEntityId(),
            authnRequest.isForceAuthn(), authnState);
        logger.trace(new ParameterizedMessage("Validated AuthnResponse from queryString [{}] and extracted [{}]",
            parsedQueryString.queryString, response));
        listener.onResponse(response);
    }

    private Map<String, Object> buildAuthnState(AuthnRequest request, SamlServiceProvider sp) {
        Map<String, Object> authnState = new HashMap<>();
        final NameIDPolicy nameIDPolicy = request.getNameIDPolicy();
        if (null != nameIDPolicy) {
            final String requestedFormat = request.getNameIDPolicy().getFormat();
            if (Strings.hasText(requestedFormat)) {
                authnState.put("nameid_format", requestedFormat);
                // we should not throw an error. Pass this as additional data so that the /saml/init API can
                // return a SAML response with the appropriate status (3.4.1.1 in the core spec)
                if (requestedFormat.equals(UNSPECIFIED) == false && sp.getAllowedNameIdFormats().contains(requestedFormat) == false) {
                    logger.warn(() ->
                        new ParameterizedMessage("The requested NameID format [{}] doesn't match the allowed NameID formats" +
                            "for this Service Provider are {}", requestedFormat, sp.getAllowedNameIdFormats()));
                    authnState.put("error", "invalid_nameid_policy");
                }
            }
        }
        return authnState;
    }

    private boolean validateSignature(ParsedQueryString queryString, Collection<X509Credential> credentials) {
        final String javaSigAlgorithm = samlFactory.getJavaAlorithmNameFromUri(queryString.sigAlg);
        final byte[] contentBytes = queryString.reconstructQueryParameters().getBytes(StandardCharsets.UTF_8);
        final byte[] signatureBytes = Base64.getDecoder().decode(queryString.signature);
        return credentials.stream().anyMatch(credential -> {
            try {
                Signature sig = Signature.getInstance(javaSigAlgorithm);
                sig.initVerify(credential.getEntityCertificate().getPublicKey());
                sig.update(contentBytes);
                return sig.verify(signatureBytes);
            } catch (NoSuchAlgorithmException e) {
                throw new ElasticsearchSecurityException("Java signature algorithm [{}] is not available for SAML/XML-Sig algorithm [{}]",
                    e, javaSigAlgorithm, queryString.sigAlg);
            } catch (InvalidKeyException | SignatureException e) {
                logger.warn(new ParameterizedMessage("Signature verification failed for credential [{}]",
                    samlFactory.describeCredentials(Set.of(credential))), e);
                return false;
            }
        });
    }

    private void getSpFromIssuer(Issuer issuer, ActionListener<SamlServiceProvider> listener) {
        if (issuer == null || issuer.getValue() == null) {
            throw new ElasticsearchSecurityException("SAML authentication request has no issuer");
        }
        final String issuerString = issuer.getValue();
        idp.getRegisteredServiceProvider(issuerString, false, ActionListener.wrap(
            serviceProvider -> {
                if (null == serviceProvider) {
                    throw new ElasticsearchSecurityException(
                        "Service Provider with Entity ID [{}] is not registered with this Identity Provider", issuerString);
                }
                listener.onResponse(serviceProvider);
            },
            listener::onFailure
        ));
    }

    private void checkDestination(AuthnRequest request) {
        final String url = idp.getSingleSignOnEndpoint(SAML2_REDIRECT_BINDING_URI).toString();
        if (url.equals(request.getDestination()) == false) {
            throw new ElasticsearchSecurityException(
                "SAML authentication request [{}] is for destination [{}] but the SSO endpoint of this Identity Provider is [{}]",
                request.getID(), request.getDestination(), url);
        }
    }

    private void checkAcs(AuthnRequest request, SamlServiceProvider sp) {
        final String acs = request.getAssertionConsumerServiceURL();
        if (Strings.hasText(acs) == false) {
            final String message = request.getAssertionConsumerServiceIndex() == null ?
                "SAML authentication does not contain an AssertionConsumerService URL" :
                "SAML authentication does not contain an AssertionConsumerService URL. It contains an Assertion Consumer Service Index " +
                    "but this IDP doesn't support multiple AssertionConsumerService URLs.";
            throw new ElasticsearchSecurityException(message);
        }
        if (acs.equals(sp.getAssertionConsumerService().toString()) == false) {
            throw new ElasticsearchSecurityException("The registered ACS URL for this Service Provider is [{}] but the authentication " +
                "request contained [{}]", sp.getAssertionConsumerService(), acs);
        }
    }

    protected Element parseSamlMessage(byte[] content) {
        final Element root;
        try (ByteArrayInputStream input = new ByteArrayInputStream(content)) {
            // This will parse and validate the input against the schemas
            final Document doc = THREAD_LOCAL_DOCUMENT_BUILDER.get().parse(input);
            root = doc.getDocumentElement();
            if (logger.isTraceEnabled()) {
                logger.trace("Received SAML Message: {} \n", samlFactory.toString(root, true));
            }
        } catch (SAXException | IOException e) {
            throw new ElasticsearchSecurityException("Failed to parse SAML message", e);
        }
        return root;
    }

    private byte[] decodeBase64(String content) {
        try {
            return Base64.getDecoder().decode(content.replaceAll("\\s+", ""));
        } catch (IllegalArgumentException e) {
            logger.info("Failed to decode base64 string [{}] - {}", content, e);
            throw new ElasticsearchSecurityException("SAML message cannot be Base64 decoded", e);
        }
    }

    private byte[] inflate(byte[] bytes) {
        Inflater inflater = new Inflater(true);
        try (ByteArrayInputStream in = new ByteArrayInputStream(bytes);
             InflaterInputStream inflate = new InflaterInputStream(in, inflater);
             ByteArrayOutputStream out = new ByteArrayOutputStream(bytes.length * 3 / 2)) {
            Streams.copy(inflate, out);
            return out.toByteArray();
        } catch (IOException e) {
            throw new ElasticsearchSecurityException("SAML message cannot be inflated", e);
        }
    }

    private String urlEncode(String param) throws UnsupportedEncodingException {
        return URLEncoder.encode(param, StandardCharsets.UTF_8.name());
    }

    private void logAndRespond(String message, ActionListener listener) {
        logger.debug(message);
        listener.onFailure(new ElasticsearchSecurityException(message));
    }

    private void logAndRespond(String message, Throwable e, ActionListener listener) {
        logger.debug(message);
        listener.onFailure(new ElasticsearchSecurityException(message, e));
    }

    private void logAndRespond(ParameterizedMessage message, ActionListener listener) {
        logger.debug(message.getFormattedMessage());
        listener.onFailure(new ElasticsearchSecurityException(message.getFormattedMessage()));
    }

    private class ParsedQueryString {
        private final String queryString;
        private final String samlRequest;
        @Nullable
        private final String relayState;
        @Nullable
        private final String sigAlg;
        @Nullable
        private final String signature;

        private ParsedQueryString(String queryString, String samlRequest, String relayState, String sigAlg, String signature) {
            this.queryString = Objects.requireNonNull(queryString, "Query string may not be null");
            this.samlRequest = Objects.requireNonNull(samlRequest, "SAML request parameter may not be null");
            this.relayState = relayState;
            this.sigAlg = sigAlg;
            this.signature = signature;
        }

        public String reconstructQueryParameters() throws ElasticsearchSecurityException {
            try {
                return relayState == null ?
                    "SAMLRequest=" + urlEncode(samlRequest) + "&SigAlg=" + urlEncode(sigAlg) :
                    "SAMLRequest=" + urlEncode(samlRequest) + "&RelayState=" + urlEncode(relayState) + "&SigAlg=" + urlEncode(sigAlg);
            } catch (UnsupportedEncodingException e) {
                throw new ElasticsearchSecurityException("Cannot reconstruct query for signature verification", e);
            }
        }
    }
}
