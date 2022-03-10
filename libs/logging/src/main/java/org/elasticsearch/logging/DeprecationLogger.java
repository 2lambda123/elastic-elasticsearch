/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

import org.elasticsearch.logging.internal.DeprecatedMessage;
import org.elasticsearch.logging.internal.ESLogMessage;
import org.elasticsearch.logging.internal.HeaderWarningAppender;
import org.elasticsearch.logging.internal.RateLimitingFilter;
import org.elasticsearch.logging.internal.ServerSupportImpl;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.List;

/**
 * A logger that logs deprecation notices. Logger should be initialized with a class or name which will be used
 * for deprecation logger. For instance <code>DeprecationLogger.getLogger("org.elasticsearch.test.SomeClass")</code> will
 * result in a deprecation logger with name <code>org.elasticsearch.deprecation.test.SomeClass</code>. This allows to use a
 * <code>deprecation</code> logger defined in log4j2.properties.
 * <p>
 * Logs are emitted at the custom {@link #CRITICAL} level, and routed wherever they need to go using log4j. For example,
 * to disk using a rolling file appender, or added as a response header using {@link HeaderWarningAppender}.
 * <p>
 * Deprecation messages include a <code>key</code>, which is used for rate-limiting purposes. The log4j configuration
 * uses {@link RateLimitingFilter} to prevent the same message being logged repeatedly in a short span of time. This
 * key is combined with the <code>X-Opaque-Id</code> request header value, if supplied, which allows for per-client
 * message limiting.
 */
// TODO: PG i wonder if we coudl expose an interface and inject this implementation? the same we would do for a regular Logger interface
public final class DeprecationLogger {
    /**
     * Deprecation messages are logged at this level.
     * More serious that WARN by 1, but less serious than ERROR
     */
    public static Level CRITICAL = Level.of("CRITICAL", Level.WARN.getSeverity() - 1);

    private static volatile List<String> skipTheseDeprecations = Collections.emptyList();

    private final Logger logger;

    /**
     * Creates a new deprecation logger for the supplied class. Internally, it delegates to
     * {@link #getLogger(String)}, passing the full class name.
     */
    public static DeprecationLogger getLogger(Class<?> aClass) {
        return getLogger(toLoggerName(aClass));
    }

    /**
     * Creates a new deprecation logger based on the parent logger. Automatically
     * prefixes the logger name with "deprecation", if it starts with "org.elasticsearch.",
     * it replaces "org.elasticsearch" with "org.elasticsearch.deprecation" to maintain
     * the "org.elasticsearch" namespace.
     */
    public static DeprecationLogger getLogger(String name) {
        return new DeprecationLogger(name);
    }

    /**
     * The DeprecationLogger uses the "deprecation.skip_deprecated_settings" setting to decide whether to log a deprecation for a setting.
     * This is a node setting. This method initializes the DeprecationLogger class with the node settings for the node in order to read the
     * "deprecation.skip_deprecated_settings" setting. This only needs to be called once per JVM. If it is not called, the default behavior
     * is to assume that the "deprecation.skip_deprecated_settings" setting is not set.
     * @param nodeSkipDeprecatedSetting The settings for this node  // TODO: typy this up
     */
    public static void initialize(List<String> nodeSkipDeprecatedSetting) {
        skipTheseDeprecations = nodeSkipDeprecatedSetting == null ? Collections.emptyList() : nodeSkipDeprecatedSetting;
        // nodeSettings.getAsList("deprecation.skip_deprecated_settings");
    }

    private DeprecationLogger(String parentLoggerName) {
        this.logger = LogManager.getLogger(getLoggerName(parentLoggerName));
    }

    private static String getLoggerName(String name) {
        if (name.startsWith("org.elasticsearch")) {
            name = name.replace("org.elasticsearch.", "org.elasticsearch.deprecation.");
        } else {
            name = "deprecation." + name;
        }
        return name;
    }

    private static String toLoggerName(final Class<?> cls) {
        String canonicalName = cls.getCanonicalName();
        return canonicalName != null ? canonicalName : cls.getName();
    }

    /**
     * Logs a message at the {@link DeprecationLogger#CRITICAL} level.
     * This log will indicate that a change will break in next version.
     * The message is also sent to the header warning logger,
     * so that it can be returned to the client.
     */
    public DeprecationLogger critical(final DeprecationCategory category, final String key, final String msg, final Object... params) {
        return logDeprecation(CRITICAL, category, key, msg, params);
    }

    /**
     * Logs a message at the {@link Level#WARN} level for less critical deprecations
     * that won't break in next version.
     * The message is also sent to the header warning logger,
     * so that it can be returned to the client.
     */
    public DeprecationLogger warn(final DeprecationCategory category, final String key, final String msg, final Object... params) {
        return logDeprecation(Level.WARN, category, key, msg, params);
    }

    private DeprecationLogger logDeprecation(Level level, DeprecationCategory category, String key, String msg, Object[] params) {

        return this;
    }

    private void doPrivilegedLog(Level level, ESLogMessage deprecationMessage) {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            logger.log(level, deprecationMessage);
            return null;
        });
    }

    /**
     * Used for handling previous version RestApiCompatible logic.
     * Logs a message at the {@link DeprecationLogger#CRITICAL} level
     * that have been broken in previous version.
     * The message is also sent to the header warning logger,
     * so that it can be returned to the client.
     */
    public DeprecationLogger compatibleCritical(final String key, final String msg, final Object... params) {
        return compatible(CRITICAL, key, msg, params);
    }

    /**
     * Used for handling previous version RestApiCompatible logic.
     * Logs a message at the given level
     * that has been broken in previous version.
     * The message is also sent to the header warning logger,
     * so that it can be returned to the client.
     */

    public DeprecationLogger compatible(final Level level, final String key, final String msg, final Object... params) {
        String opaqueId = ServerSupportImpl.INSTANCE.getXOpaqueIdHeader();
        String productOrigin = ServerSupportImpl.INSTANCE.getProductOriginHeader();
        ESLogMessage deprecationMessage = DeprecatedMessage.compatibleDeprecationMessage(key, opaqueId, productOrigin, msg, params);
        logger.log(level, deprecationMessage);
        return this;
    }
}
