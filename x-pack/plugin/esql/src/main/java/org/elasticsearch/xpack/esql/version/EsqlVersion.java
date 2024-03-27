/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.version;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.VersionId;

public enum EsqlVersion implements VersionId<EsqlVersion> {
    // Breaking changes go here until the next version is released.
    NIGHTLY(Integer.MAX_VALUE, Integer.MAX_VALUE, "\uD83D\uDE34"),
    PARTY_POPPER(2024, 4, "\uD83C\uDF89");

    EsqlVersion(int year, int month, String emoji) {
        this.year = year;
        this.month = month;
        this.emoji = emoji;
    }

    private int year;
    private int month;
    private String emoji;

    /**
     * Version prefix that we accept when parsing. If a version string starts with the given prefix, we consider the version string valid.
     * E.g. "2024.04.🎉" will be interpreted as {@link EsqlVersion#PARTY_POPPER}, but so will "2024.04" and "2024.04foobar".
     */
    public String versionStringNoEmoji() {
        return this == NIGHTLY ? "nightly" : String.format("%d.%02d", year, month);
    }

    public static EsqlVersion parse(String versionString) {
        EsqlVersion parsed = null;
        if (Strings.hasText(versionString)) {
            versionString = versionString.toLowerCase();
            for (EsqlVersion version : EsqlVersion.values()) {
                if (versionString.startsWith(version.versionStringNoEmoji())) {
                    return version;
                }
            }
        }
        return parsed;
    }

    @Override
    public String toString() {
        return this == NIGHTLY ? "nightly." + emoji : String.format("%d.%02d.%s", year, month, emoji);
    }

    @Override
    public int id() {
        return this == NIGHTLY ? Integer.MAX_VALUE : (100 * year + month);
    }
}
