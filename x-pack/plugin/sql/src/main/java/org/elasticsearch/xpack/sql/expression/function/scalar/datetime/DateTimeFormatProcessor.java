/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.xpack.sql.util.DateUtils.asTimeAtZone;

public class DateTimeFormatProcessor extends BinaryDateTimeProcessor {

    public static final String NAME = "dtformat";
    private static final String[][] JAVA_TIME_FORMAT_REPLACEMENTS = {
        {"tt", "a"},
        {"t", "a"},
        {"dddd", "eeee"},
        {"ddd", "eee"},
        {"K", "v"},
        {"g", "G"},
        {"f", "S"},
        {"F", "S"},
        {"z", "X"}
    };

    private static final String[][] JAVA_TIME_FORMAT_REPLACEMENTS_FOR_MYSQL = {
        {"%a", "EEE"},
        {"%b", "MMM"},
        {"%c", "MM"},
        {"%d", "dd"},
        {"%e", "d"},
        {"%f", "SSSSSS"},
        {"%H", "HH"},
        {"%h", "hh"},
        {"%I", "hh"},
        {"%i", "mm"},
        {"%j", "DDD"},
        {"%k", "H"},
        {"%l", "h"},
        {"%M", "MMMM"},
        {"%m", "MM"},
        {"%p", "a"},
        {"%r", "hh:mm:ss a"},
        {"%S", "ss"},
        {"%s", "ss"},
        {"%T", "HH:mm:ss"},
        {"%U", "w"},
        {"%u", "w"},
        {"%V", "w"},
        {"%v", "w"},
        {"%W", "EEEE"},
        {"%w", "e"},
        {"%X", "Y"},
        {"%x", "Y"},
        {"%Y", "yyyy"},
        {"%y", "yy"},
        {"%%", "%"}
    };

    private final Formatter formatter;


    public enum Formatter {
        FORMAT,
        DATE_TIME_FORMAT,
        DATE_FORMAT;

        private String getJavaPattern(String pattern) {
            if (this == FORMAT) {
                pattern = replacePattern(pattern, JAVA_TIME_FORMAT_REPLACEMENTS);
            } else if (this == DATE_FORMAT) {
                pattern = replacePatternForMySql(pattern);
            }
            return pattern;
        }

        private String replacePatternForMySql(String pattern) {
            // escape any chars that are not starts with '%' to avoid chars being interpreted by DateTimeFormatter
            pattern = pattern.replaceAll(
                "(%\\w)",
                "{$0}");
            pattern = pattern.replaceAll(
                "([^(\\{%\\w\\})])?(\\w+)([^(\\{%\\w\\})])?",
                "$1'$2'$3"
            );
            pattern = pattern.replaceAll(
                "\\{%'(\\w)'\\}",
                "%$1"
            );

            // replace %x with x, for any "x" not used in mysql date format specifiers
            pattern = pattern.replaceAll(
                "%([^abcDdefHhIijklMmprSsTUuVvWwXxYy])",
                "'$1'"
            );
            pattern = pattern.replace("''", "");

            return replacePattern(pattern, JAVA_TIME_FORMAT_REPLACEMENTS_FOR_MYSQL);
        }

        private String replacePattern(String pattern, String[][] javaTimeFormatReplacements) {
            for (String[] replacement : javaTimeFormatReplacements) {
                pattern = pattern.replace(replacement[0], replacement[1]);
            }
            return pattern;
        }

        public Object format(Object timestamp, Object pattern, ZoneId zoneId) {
            if (timestamp == null || pattern == null) {
                return null;
            }
            String patternString;
            if (pattern instanceof String) {
                patternString = (String) pattern;
            } else {
                throw new SqlIllegalArgumentException("A string is required; received [{}]", pattern);
            }
            if (patternString.isEmpty()) {
                return null;
            }

            if (timestamp instanceof ZonedDateTime == false && timestamp instanceof OffsetTime == false) {
                throw new SqlIllegalArgumentException("A date/datetime/time is required; received [{}]", timestamp);
            }

            TemporalAccessor ta;
            if (timestamp instanceof ZonedDateTime) {
                ta = ((ZonedDateTime) timestamp).withZoneSameInstant(zoneId);
            } else {
                ta = asTimeAtZone((OffsetTime) timestamp, zoneId);
            }
            try {
                String javaPattern = getJavaPattern(patternString);
                if (this == DATE_FORMAT) {
                    javaPattern = interpretAndEscapeDayOfMonthWithOrdinalIndicator(ta, javaPattern);
                }
                return DateTimeFormatter.ofPattern(javaPattern, Locale.ROOT).format(ta);
            } catch (IllegalArgumentException | DateTimeException e) {
                throw new SqlIllegalArgumentException(
                    "Invalid pattern [{}] is received for formatting date/time [{}]; {}",
                    pattern,
                    timestamp,
                    e.getMessage()
                );
            }
        }

        private String interpretAndEscapeDayOfMonthWithOrdinalIndicator(TemporalAccessor ta, String javaPattern) {
            if (javaPattern.contains("%D")) {
                String dayOfMonth = DateTimeFormatter.ofPattern("d", Locale.ROOT).format(ta);
                if (dayOfMonth.endsWith("1")) {
                    dayOfMonth = "'" + dayOfMonth + "st'";
                } else if (dayOfMonth.endsWith("2")) {
                    dayOfMonth = "'" + dayOfMonth + "nd'";
                } else if (dayOfMonth.endsWith("3")) {
                    dayOfMonth = "'" + dayOfMonth + "rd'";
                } else {
                    dayOfMonth = "'" +dayOfMonth+ "th'";
                }
                javaPattern = javaPattern.replace("%D", dayOfMonth);
            }
            return javaPattern;
        }
    }

    public DateTimeFormatProcessor(Processor source1, Processor source2, ZoneId zoneId, Formatter formatter) {
        super(source1, source2, zoneId);
        this.formatter = formatter;
    }

    public DateTimeFormatProcessor(StreamInput in) throws IOException {
        super(in);
        this.formatter = in.readEnum(Formatter.class);
    }

    @Override
    protected void doWrite(StreamOutput out) throws IOException {
        out.writeEnum(formatter);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Object doProcess(Object timestamp, Object pattern) {
        return this.formatter.format(timestamp, pattern, zoneId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), formatter);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        DateTimeFormatProcessor other = (DateTimeFormatProcessor) obj;
        return super.equals(other) && Objects.equals(formatter, other.formatter);
    }

    public Formatter formatter() {
        return formatter;
    }
}
