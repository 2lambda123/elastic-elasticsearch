/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe;

import java.text.MessageFormat;
import java.util.Locale;

public class DataFrameMessages {

    public static final String REST_STOP_JOB_WAIT_FOR_COMPLETION_TIMEOUT =
            "Timed out after [{0}] while waiting for data frame job [{1}] to stop";
    public static final String REST_STOP_JOB_WAIT_FOR_COMPLETION_INTERRUPT = "Interrupted while waiting for data frame job [{0}] to stop";

    private DataFrameMessages() {
    }

    /**
     * Returns the message parameter
     *
     * @param message Should be one of the statics defined in this class
     */
    public static String getMessage(String message) {
        return message;
    }

    /**
     * Format the message with the supplied arguments
     *
     * @param message Should be one of the statics defined in this class
     * @param args MessageFormat arguments. See {@linkplain MessageFormat#format(Object)}]
     */
    public static String getMessage(String message, Object... args) {
        return new MessageFormat(message, Locale.ROOT).format(args);
    }
}
