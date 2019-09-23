/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;

import java.util.regex.Pattern;

import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.LIFECYCLE_ORIGINATION_DATE;
import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.LIFECYCLE_PARSE_ORIGINATION_DATE;

public class IndexLifecycleOriginationDateParser {

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("yyyy.MM.dd");
    private static final String INDEX_NAME_REGEX = "^.*-(.+)(-[\\d]+)?$";
    private static final Pattern INDEX_NAME_PATTERN = Pattern.compile(INDEX_NAME_REGEX);

    /**
     * Determines if the origination date needs to be parsed from the index name.
     */
    public static boolean shouldParseIndexName(Settings indexSettings) {
        return indexSettings.getAsLong(LIFECYCLE_ORIGINATION_DATE, -1L) == -1L &&
            indexSettings.getAsBoolean(LIFECYCLE_PARSE_ORIGINATION_DATE, false);
    }

    /**
     * Parses the index according to the supported format and extracts the origination date. If the index does not match the expected
     * format or the date in the index name doesn't match the `yyyy.MM.dd` format it throws an {@link IllegalArgumentException}
     */
    public static long parseIndexNameAndExtractDate(String indexName) {
        if (INDEX_NAME_PATTERN.matcher(indexName).matches()) {
            int firstDash = indexName.indexOf("-");
            assert firstDash != -1 : "no separator '-' found";
            int dateEndIndex;
            int lastDash = indexName.lastIndexOf("-");
            if (lastDash == firstDash) {
                // only one dash found in the index name so we are assuming the indexName-{date} format
                dateEndIndex = indexName.length();
            } else {
                dateEndIndex = lastDash;
            }

            String dateAsString = indexName.substring(firstDash + 1, dateEndIndex);
            try {
                return DATE_FORMATTER.parseMillis(dateAsString);
            } catch (ElasticsearchParseException | IllegalArgumentException e) {
                throw new IllegalArgumentException("index name [" + indexName + "] does not match pattern '" + INDEX_NAME_REGEX + "'");
            }
        }

        throw new IllegalArgumentException("index name [" + indexName + "] does not match pattern '" + INDEX_NAME_REGEX + "'");
    }
}
