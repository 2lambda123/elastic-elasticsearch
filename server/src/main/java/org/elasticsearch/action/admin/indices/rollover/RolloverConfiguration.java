/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.action.admin.indices.rollover.RolloverConditions.MAX_AGE_FIELD;
import static org.elasticsearch.action.admin.indices.rollover.RolloverConditions.MAX_DOCS_FIELD;
import static org.elasticsearch.action.admin.indices.rollover.RolloverConditions.MAX_PRIMARY_SHARD_DOCS_FIELD;
import static org.elasticsearch.action.admin.indices.rollover.RolloverConditions.MAX_PRIMARY_SHARD_SIZE_FIELD;
import static org.elasticsearch.action.admin.indices.rollover.RolloverConditions.MAX_SIZE_FIELD;
import static org.elasticsearch.action.admin.indices.rollover.RolloverConditions.MIN_AGE_FIELD;
import static org.elasticsearch.action.admin.indices.rollover.RolloverConditions.MIN_DOCS_FIELD;
import static org.elasticsearch.action.admin.indices.rollover.RolloverConditions.MIN_PRIMARY_SHARD_DOCS_FIELD;
import static org.elasticsearch.action.admin.indices.rollover.RolloverConditions.MIN_PRIMARY_SHARD_SIZE_FIELD;
import static org.elasticsearch.action.admin.indices.rollover.RolloverConditions.MIN_SIZE_FIELD;

/**
 * This class holds the configuration of the rollover conditions as they are defined in DLM lifecycle. Currently, it can handle automatic
 * configuration for the max index age condition.
 * TODO: extend this for ILM too, when the design is settled
 */
public class RolloverConfiguration implements Writeable, ToXContentObject {

    public static final ObjectParser<ValueParser, Void> PARSER = new ObjectParser<>("rollover_conditions");

    static {
        PARSER.declareString((valueParser, s) -> valueParser.addMaxIndexAgeCondition(s, MaxAgeCondition.NAME), MAX_AGE_FIELD);
        PARSER.declareLong(ValueParser::addMaxIndexDocsCondition, MAX_DOCS_FIELD);
        PARSER.declareString((valueParser, s) -> valueParser.addMaxIndexSizeCondition(s, MaxSizeCondition.NAME), MAX_SIZE_FIELD);
        PARSER.declareString(
            (valueParser, s) -> valueParser.addMaxPrimaryShardSizeCondition(s, MaxPrimaryShardSizeCondition.NAME),
            MAX_PRIMARY_SHARD_SIZE_FIELD
        );
        PARSER.declareLong(ValueParser::addMaxPrimaryShardDocsCondition, MAX_PRIMARY_SHARD_DOCS_FIELD);
        PARSER.declareString((valueParser, s) -> valueParser.addMinIndexAgeCondition(s, MinAgeCondition.NAME), MIN_AGE_FIELD);
        PARSER.declareLong(ValueParser::addMinIndexDocsCondition, MIN_DOCS_FIELD);
        PARSER.declareString((valueParser, s) -> valueParser.addMinIndexSizeCondition(s, MinSizeCondition.NAME), MIN_SIZE_FIELD);
        PARSER.declareString(
            (valueParser, s) -> valueParser.addMinPrimaryShardSizeCondition(s, MinPrimaryShardSizeCondition.NAME),
            MIN_PRIMARY_SHARD_SIZE_FIELD
        );
        PARSER.declareLong(ValueParser::addMinPrimaryShardDocsCondition, MIN_PRIMARY_SHARD_DOCS_FIELD);
    }

    private final RolloverConditions concreteConditions;
    private final Set<String> automaticConditions;

    public RolloverConfiguration(RolloverConditions concreteConditions, Set<String> automaticConditions) {
        this.concreteConditions = concreteConditions;
        this.automaticConditions = automaticConditions;
    }

    public RolloverConfiguration(StreamInput in) throws IOException {
        concreteConditions = new RolloverConditions(in);
        automaticConditions = in.readSet(StreamInput::readString);
    }

    public static RolloverConfiguration fromXContent(XContentParser parser) throws IOException {
        ValueParser valueParser = new ValueParser();
        PARSER.parse(parser, valueParser, null);
        return valueParser.getRolloverConfiguration();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeWriteable(concreteConditions);
        out.writeCollection(automaticConditions, StreamOutput::writeString);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        concreteConditions.toXContentFragment(builder, params);
        for (String automaticCondition : automaticConditions) {
            builder.field(automaticCondition, "auto");
        }
        builder.endObject();
        return builder;
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params, TimeValue retention) throws IOException {
        builder.startObject();
        concreteConditions.toXContentFragment(builder, params);
        for (String automaticCondition : automaticConditions) {
            builder.field(automaticCondition, evaluateMaxAgeCondition(retention) + " [automatic]");
        }
        builder.endObject();
        return builder;
    }

    /**
     * Evaluates all the automatic conditions, currently only max age based on the input data
     */
    public RolloverConditions resolveRolloverConditions(TimeValue dataRetention) {
        if (automaticConditions.isEmpty()) {
            return concreteConditions;
        }
        RolloverConditions.Builder builder = RolloverConditions.newBuilder(concreteConditions);
        if (automaticConditions.contains(MaxAgeCondition.NAME)) {
            builder.addMaxIndexAgeCondition(evaluateMaxAgeCondition(dataRetention));
        }
        return builder.build();
    }

    // Visible for testing
    RolloverConditions getConcreteConditions() {
        return concreteConditions;
    }

    // Visible for testing
    Set<String> getAutomaticConditions() {
        return automaticConditions;
    }

    /**
     * Parses a cluster setting configuration, it expects it to have the following format: "condition1=value1,condition2=value2"
     * @throws SettingsException if the input is invalid, if there are unknown conditions or invalid format values.
     * @throws IllegalArgumentException if the input is null or blank.
     */
    public static RolloverConfiguration parseSetting(String input, String setting) {
        if (Strings.isNullOrBlank(input)) {
            throw new IllegalArgumentException("The rollover conditions cannot be null or blank");
        }
        String[] sConditions = input.split(",");
        RolloverConfiguration.ValueParser valueParser = new RolloverConfiguration.ValueParser();
        for (String sCondition : sConditions) {
            String[] keyValue = sCondition.split("=");
            if (keyValue.length != 2) {
                throw new SettingsException("Invalid condition: '{}', format must be 'condition=value'", sCondition);
            }
            var condition = keyValue[0];
            var value = keyValue[1];
            if (MaxSizeCondition.NAME.equals(condition)) {
                valueParser.addMaxIndexSizeCondition(value, setting);
            } else if (MaxPrimaryShardSizeCondition.NAME.equals(condition)) {
                valueParser.addMaxPrimaryShardSizeCondition(value, setting);
            } else if (MaxAgeCondition.NAME.equals(condition)) {
                valueParser.addMaxIndexAgeCondition(value, setting);
            } else if (MaxDocsCondition.NAME.equals(condition)) {
                valueParser.addMaxIndexDocsCondition(parseLong(value, setting));
            } else if (MaxPrimaryShardDocsCondition.NAME.equals(condition)) {
                valueParser.addMaxPrimaryShardDocsCondition(parseLong(value, setting));
            } else if (MinSizeCondition.NAME.equals(condition)) {
                valueParser.addMinIndexSizeCondition(value, setting);
            } else if (MinPrimaryShardSizeCondition.NAME.equals(condition)) {
                valueParser.addMinPrimaryShardSizeCondition(value, setting);
            } else if (MinAgeCondition.NAME.equals(condition)) {
                valueParser.addMinIndexAgeCondition(value, setting);
            } else if (MinDocsCondition.NAME.equals(condition)) {
                valueParser.addMinIndexDocsCondition(parseLong(value, setting));
            } else if (MinPrimaryShardDocsCondition.NAME.equals(condition)) {
                valueParser.addMinPrimaryShardDocsCondition(parseLong(value, condition));
            } else {
                throw new SettingsException("Unknown condition: '{}'", condition);
            }
        }
        return valueParser.getRolloverConfiguration();
    }

    private static Long parseLong(String sValue, String settingName) {
        try {
            return Long.parseLong(sValue);
        } catch (NumberFormatException e) {
            throw new SettingsException(
                "Invalid value '{}' in setting '{}', the value is expected to be of type long",
                sValue,
                settingName,
                e.getMessage()
            );
        }
    }

    /**
     * When max_age is auto we’ll use the following retention dependent heuristics to compute the value
     * we’ll use for the rollover operation:
     * - If retention is null aka infinite (default) max_age will be 30 days
     * - If retention is configured to anything lower than 3 months max_age will be 7 days
     * - If retention is configured to anything greater than 3 months max_age will be 30 days
     */
    private TimeValue evaluateMaxAgeCondition(@Nullable TimeValue retention) {
        if (retention == null) {
            return TimeValue.timeValueDays(30);
        }
        if (retention.compareTo(TimeValue.timeValueDays(90)) <= 0) {
            return TimeValue.timeValueDays(7);
        }
        return TimeValue.timeValueDays(30);
    }

    /**
     * Simple builder class that helps keeping track of the values during parsing
     */
    public static class ValueParser {
        private final RolloverConditions.Builder concreteConditions = RolloverConditions.newBuilder();
        private final Set<String> automatic = new HashSet<>();
        Set<String> encounteredConditions = new HashSet<>();

        /**
         * Parses and adds max index age condition (can be automatic)
         */
        public ValueParser addMaxIndexAgeCondition(String value, String setting) {
            if (value != null) {
                String condition = MaxAgeCondition.NAME;
                checkForDuplicatesAndAdd(condition, () -> {
                    if (value.equals("auto")) {
                        automatic.add(condition);
                    } else {
                        concreteConditions.addMaxIndexAgeCondition(TimeValue.parseTimeValue(value, setting));
                    }
                });
            }
            return this;
        }

        /**
         * Adds max index docs condition
         */
        public ValueParser addMaxIndexDocsCondition(Long maxDocs) {
            if (maxDocs != null) {
                String condition = MaxDocsCondition.NAME;
                checkForDuplicatesAndAdd(condition, () -> concreteConditions.addMaxIndexDocsCondition(maxDocs));
            }
            return this;
        }

        /**
         * Parses and adds max index size
         */
        public ValueParser addMaxIndexSizeCondition(String value, String setting) {
            if (value != null) {
                String condition = MaxSizeCondition.NAME;
                checkForDuplicatesAndAdd(
                    condition,
                    () -> concreteConditions.addMaxIndexSizeCondition(ByteSizeValue.parseBytesSizeValue(value, setting))
                );
            }
            return this;
        }

        /**
         * Parses and adds max index primary shard size
         */
        public ValueParser addMaxPrimaryShardSizeCondition(String value, String setting) {
            if (value != null) {
                String condition = MaxPrimaryShardSizeCondition.NAME;
                checkForDuplicatesAndAdd(
                    condition,
                    () -> concreteConditions.addMaxPrimaryShardSizeCondition(ByteSizeValue.parseBytesSizeValue(value, setting))
                );
            }
            return this;
        }

        /**
         * Adds max primary shard doc count
         */
        public ValueParser addMaxPrimaryShardDocsCondition(Long maxDocs) {
            if (maxDocs != null) {
                String condition = MaxPrimaryShardDocsCondition.NAME;
                checkForDuplicatesAndAdd(condition, () -> concreteConditions.addMaxPrimaryShardDocsCondition(maxDocs));
            }
            return this;
        }

        /**
         * Parses and adds min index age condition
         */
        public ValueParser addMinIndexAgeCondition(String value, String setting) {
            if (value != null) {
                String condition = MinAgeCondition.NAME;
                checkForDuplicatesAndAdd(
                    condition,
                    () -> concreteConditions.addMinIndexAgeCondition(TimeValue.parseTimeValue(value, setting))
                );
            }
            return this;
        }

        /**
         * Adds the min index docs count condition
         */
        public ValueParser addMinIndexDocsCondition(Long minDocs) {
            if (minDocs != null) {
                String condition = MinDocsCondition.NAME;
                checkForDuplicatesAndAdd(condition, () -> concreteConditions.addMinIndexDocsCondition(minDocs));
            }
            return this;
        }

        /**
         * Parses and adds min index size
         */
        public ValueParser addMinIndexSizeCondition(String value, String setting) {
            if (value != null) {
                String condition = MinSizeCondition.NAME;
                checkForDuplicatesAndAdd(
                    condition,
                    () -> concreteConditions.addMinIndexSizeCondition(ByteSizeValue.parseBytesSizeValue(value, setting))
                );
            }
            return this;
        }

        /**
         * Parses and adds min index primary shard size
         */
        public ValueParser addMinPrimaryShardSizeCondition(String value, String setting) {
            if (value != null) {
                String condition = MinPrimaryShardSizeCondition.NAME;
                checkForDuplicatesAndAdd(
                    condition,
                    () -> concreteConditions.addMinPrimaryShardSizeCondition(ByteSizeValue.parseBytesSizeValue(value, setting))
                );
            }
            return this;
        }

        /**
         * Adds the max primary shard doc count
         */
        public ValueParser addMinPrimaryShardDocsCondition(Long minDocs) {
            if (minDocs != null) {
                String condition = MinPrimaryShardDocsCondition.NAME;
                checkForDuplicatesAndAdd(condition, () -> concreteConditions.addMinPrimaryShardDocsCondition(minDocs));
            }
            return this;
        }

        public void checkForDuplicatesAndAdd(String condition, Runnable parseAndAdd) {
            if (encounteredConditions.contains(condition)) {
                throw new IllegalArgumentException(condition + " condition is already set");
            }
            parseAndAdd.run();
            encounteredConditions.add(condition);
        }

        RolloverConfiguration getRolloverConfiguration() {
            return new RolloverConfiguration(concreteConditions.build(), Collections.unmodifiableSet(automatic));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RolloverConfiguration that = (RolloverConfiguration) o;
        return Objects.equals(concreteConditions, that.concreteConditions) && Objects.equals(automaticConditions, that.automaticConditions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(concreteConditions, automaticConditions);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
