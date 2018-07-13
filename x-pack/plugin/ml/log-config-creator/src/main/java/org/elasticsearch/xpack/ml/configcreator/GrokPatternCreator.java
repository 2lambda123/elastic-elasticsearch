/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.Terminal.Verbosity;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.xpack.ml.configcreator.TimestampFormatFinder.TimestampMatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Creates Grok patterns that will match all provided sample messages.
 *
 * The choice of field names is quite primitive.  The intention is that a human will edit these.
 */
public final class GrokPatternCreator {

    private static final String PUNCTUATION_OR_SPACE = "\"'`‘’“”#@%=\\/|~:;,<>()[]{}«»^$*¿?¡!§¶ \t\n";
    private static final String NEEDS_ESCAPING = "\\|()[]{}^$*?";

    private static final String PREFACE = "preface";
    private static final String VALUE = "value";
    private static final String EPILOGUE = "epilogue";

    /**
     * Grok patterns that are designed to match the whole message, not just a part of it.
     */
    private static final List<FullMatchGrokPatternCandidate> FULL_MATCH_GROK_PATTERNS = Arrays.asList(
        new FullMatchGrokPatternCandidate("BACULA_LOGLINE", "bts"),
        new FullMatchGrokPatternCandidate("CATALINALOG", "timestamp"),
        new FullMatchGrokPatternCandidate("COMBINEDAPACHELOG", "timestamp"),
        new FullMatchGrokPatternCandidate("COMMONAPACHELOG", "timestamp"),
        new FullMatchGrokPatternCandidate("ELB_ACCESS_LOG", "timestamp"),
        new FullMatchGrokPatternCandidate("HAPROXYHTTP", "syslog_timestamp"),
        new FullMatchGrokPatternCandidate("HAPROXYTCP", "syslog_timestamp"),
        new FullMatchGrokPatternCandidate("HTTPD20_ERRORLOG", "timestamp"),
        new FullMatchGrokPatternCandidate("HTTPD24_ERRORLOG", "timestamp"),
        new FullMatchGrokPatternCandidate("NAGIOSLOGLINE", "nagios_epoch"),
        new FullMatchGrokPatternCandidate("NETSCREENSESSIONLOG", "date"),
        new FullMatchGrokPatternCandidate("RAILS3", "timestamp"),
        new FullMatchGrokPatternCandidate("RUBY_LOGGER", "timestamp"),
        new FullMatchGrokPatternCandidate("SHOREWALL", "timestamp"),
        new FullMatchGrokPatternCandidate("TOMCATLOG", "timestamp")
    );

    /**
     * The first match in this list will be chosen, so it needs to be ordered
     * such that more generic patterns come after more specific patterns.
     */
    private static final List<GrokPatternCandidate> ORDERED_CANDIDATE_GROK_PATTERNS = Arrays.asList(
        new ValueOnlyGrokPatternCandidate("TOMCAT_DATESTAMP", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("TIMESTAMP_ISO8601", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("DATESTAMP_RFC822", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("DATESTAMP_RFC2822", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("DATESTAMP_OTHER", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("DATESTAMP_EVENTLOG", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("SYSLOGTIMESTAMP", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("HTTPDATE", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("CATALINA_DATESTAMP", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("CISCOTIMESTAMP", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("LOGLEVEL", "keyword", "loglevel"),
        new ValueOnlyGrokPatternCandidate("URI", "keyword", "uri"),
        new ValueOnlyGrokPatternCandidate("UUID", "keyword", "uuid"),
        new ValueOnlyGrokPatternCandidate("MAC", "keyword", "macaddress"),
        // Can't use \b as the breaks, because slashes are not "word" characters
        new ValueOnlyGrokPatternCandidate("PATH", "keyword", "path", "(?<!\\w)", "(?!\\w)"),
        new ValueOnlyGrokPatternCandidate("EMAILADDRESS", "keyword", "email"),
        // TODO: would be nice to have IPORHOST here, but HOST matches almost all words
        new ValueOnlyGrokPatternCandidate("IP", "ip", "ipaddress"),
        new ValueOnlyGrokPatternCandidate("DATE", "date", "date"),
        new ValueOnlyGrokPatternCandidate("TIME", "date", "time"),
        // This already includes pre/post break conditions
        new ValueOnlyGrokPatternCandidate("QUOTEDSTRING", "keyword", "field", "", ""),
        // Disallow +, - and . before numbers, as well as "word" characters, otherwise we'll pick
        // up numeric suffices too eagerly
        new ValueOnlyGrokPatternCandidate("INT", "long", "field", "(?<![\\w.+-])", "(?![\\w+-]|\\.\\d)"),
        new ValueOnlyGrokPatternCandidate("NUMBER", "double", "field", "(?<![\\w.+-])", "(?![\\w+-]|\\.\\d)"),
        new ValueOnlyGrokPatternCandidate("BASE16NUM", "keyword", "field", "(?<![\\w.+-])", "(?![\\w+-]|\\.\\w)")
        // TODO: also unfortunately can't have USERNAME in the list as it matches too broadly
        // Fixing these problems with overly broad matches would require some extra intelligence
        // to be added to remove inappropriate matches.  One idea would be to use a dictionary,
        // but that doesn't necessarily help as "jay" could be a username but is also a dictionary
        // word (plus there's the international headache with relying on dictionaries).  Similarly,
        // hostnames could also be dictionary words - I've worked on machines called "hippo" and
        // "scarf" in the past.  Another idea would be to look at the adjacent characters and
        // apply some heuristic based on those.
    );

    private GrokPatternCreator() {
    }

    /**
     * This method attempts to find a Grok pattern that will match all of the sample messages in their entirety.
     * @param terminal Used for verbose status messages.
     * @param sampleMessages Sample messages that any non-<code>null</code> return will match.
     * @param mappings Will be updated with mappings appropriate for the returned pattern, if non-<code>null</code>.
     * @return A tuple of (time field name, Grok string), or <code>null</code> if no suitable Grok pattern was found.
     */
    public static Tuple<String, String> findFullLineGrokPattern(Terminal terminal, Collection<String> sampleMessages,
                                                                Map<String, String> mappings) {

        for (FullMatchGrokPatternCandidate candidate : FULL_MATCH_GROK_PATTERNS) {
            if (candidate.matchesAll(sampleMessages)) {
                return candidate.processMatch(terminal, sampleMessages, mappings);
            }
        }

        return null;
    }

    /**
     * Build a Grok pattern that will match all of the sample messages in their entirety.
     * @param terminal Used for verbose status messages.
     * @param sampleMessages Sample messages that the returned Grok pattern will match.
     * @param seedPatternName A pattern that has already been determined to match some portion of every sample message.
     * @param seedFieldName The field name to be used for the portion of every sample message that the seed pattern matches.
     * @param mappings Will be updated with mappings appropriate for the returned pattern, excluding the seed field name.
     * @return The built Grok pattern.
     */
    public static String createGrokPatternFromExamples(Terminal terminal, Collection<String> sampleMessages, String seedPatternName,
                                                       String seedFieldName, Map<String, String> mappings) {

        GrokPatternCandidate seedCandidate = new NoMappingGrokPatternCandidate(seedPatternName, seedFieldName);

        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        StringBuilder overallGrokPatternBuilder = new StringBuilder();

        processCandidateAndSplit(terminal, fieldNameCountStore, overallGrokPatternBuilder, seedCandidate, true, sampleMessages, mappings,
            false, 0, false, 0);

        return overallGrokPatternBuilder.toString().replace("\t", "\\t").replace("\n", "\\n");
    }

    /**
     * Given a chosen Grok pattern and a collection of message snippets, split the snippets into the
     * matched section and the pieces before and after it.  Recurse to find more matches in the pieces
     * before and after and update the supplied string builder.
     */
    private static void processCandidateAndSplit(Terminal terminal, Map<String, Integer> fieldNameCountStore,
                                                 StringBuilder overallGrokPatternBuilder, GrokPatternCandidate chosenPattern,
                                                 boolean isLast, Collection<String> snippets, Map<String, String> mappings,
                                                 boolean ignoreKeyValueCandidateLeft, int ignoreValueOnlyCandidatesLeft,
                                                 boolean ignoreKeyValueCandidateRight, int ignoreValueOnlyCandidatesRight) {

        Collection<String> prefaces = new ArrayList<>();
        Collection<String> epilogues = new ArrayList<>();
        String patternBuilderContent = chosenPattern.processCaptures(fieldNameCountStore, snippets, prefaces, epilogues, mappings);
        appendBestGrokMatchForStrings(terminal, fieldNameCountStore, overallGrokPatternBuilder, false, prefaces, mappings,
            ignoreKeyValueCandidateLeft, ignoreValueOnlyCandidatesLeft);
        overallGrokPatternBuilder.append(patternBuilderContent);
        appendBestGrokMatchForStrings(terminal, fieldNameCountStore, overallGrokPatternBuilder, isLast, epilogues, mappings,
            ignoreKeyValueCandidateRight, ignoreValueOnlyCandidatesRight);
    }

    /**
     * Given a collection of message snippets, work out which (if any) of the Grok patterns we're allowed
     * to use matches it best.  Then append the appropriate Grok language to represent that finding onto
     * the supplied string builder.
     */
    static void appendBestGrokMatchForStrings(Terminal terminal, Map<String, Integer> fieldNameCountStore,
                                              StringBuilder overallGrokPatternBuilder, boolean isLast, Collection<String> snippets,
                                              Map<String, String> mappings, boolean ignoreKeyValueCandidate,
                                              int ignoreValueOnlyCandidates) {

        snippets = adjustForPunctuation(snippets, overallGrokPatternBuilder);

        GrokPatternCandidate bestCandidate = null;
        if (snippets.isEmpty() == false) {
            GrokPatternCandidate kvCandidate = new KeyValueGrokPatternCandidate(terminal);
            if (ignoreKeyValueCandidate == false && kvCandidate.matchesAll(snippets)) {
                bestCandidate = kvCandidate;
            } else {
                ignoreKeyValueCandidate = true;
                for (GrokPatternCandidate candidate :
                    ORDERED_CANDIDATE_GROK_PATTERNS.subList(ignoreValueOnlyCandidates, ORDERED_CANDIDATE_GROK_PATTERNS.size())) {
                    if (candidate.matchesAll(snippets)) {
                        bestCandidate = candidate;
                        break;
                    }
                    ++ignoreValueOnlyCandidates;
                }
            }
        }

        if (bestCandidate == null) {
            if (isLast) {
                finalizeGrokPattern(overallGrokPatternBuilder, snippets);
            } else {
                addIntermediateRegex(overallGrokPatternBuilder, snippets);
            }
        } else {
            processCandidateAndSplit(terminal, fieldNameCountStore, overallGrokPatternBuilder, bestCandidate, isLast, snippets, mappings,
                true, ignoreValueOnlyCandidates + (ignoreKeyValueCandidate ? 1 : 0), ignoreKeyValueCandidate, ignoreValueOnlyCandidates);
        }
    }

    /**
     * If the snippets supplied begin with more than 1 character of common punctuation or whitespace
     * then add all but the last of these characters to the overall pattern and remove them from the
     * snippets.
     * @param snippets Input snippets - not modified.
     * @param overallPatternBuilder The string builder in which a regex is being built to which common
     *                              punctuation characters will be appended (with appropriate escaping
     *                              if necessary).
     * @return Output snippets, which will be a copy of the input snippets but with whatever characters
     *         were added to <code>overallPatternBuilder</code> removed from the beginning.
     */
    static Collection<String> adjustForPunctuation(Collection<String> snippets, StringBuilder overallPatternBuilder) {

        assert snippets.isEmpty() == false;

        StringBuilder commonInitialPunctuation = new StringBuilder();

        for (String snippet : snippets) {

            if (commonInitialPunctuation.length() == 0) {
                for (int index = 0; index < snippet.length(); ++index) {
                    char ch = snippet.charAt(index);
                    if (PUNCTUATION_OR_SPACE.indexOf(ch) >= 0) {
                        commonInitialPunctuation.append(ch);
                    } else {
                        break;
                    }
                }
            } else {
                if (commonInitialPunctuation.length() > snippet.length()) {
                    commonInitialPunctuation.delete(snippet.length(), commonInitialPunctuation.length());
                }
                for (int index = 0; index < commonInitialPunctuation.length(); ++index) {
                    char ch = snippet.charAt(index);
                    if (ch != commonInitialPunctuation.charAt(index)) {
                        commonInitialPunctuation.delete(index, commonInitialPunctuation.length());
                        break;
                    }
                }
            }

            if (commonInitialPunctuation.length() <= 1) {
                return snippets;
            }
        }

        int numLiteralCharacters = commonInitialPunctuation.length() - 1;

        for (int index = 0; index < numLiteralCharacters; ++index) {
            char ch = commonInitialPunctuation.charAt(index);
            if (NEEDS_ESCAPING.indexOf(ch) >= 0) {
                overallPatternBuilder.append('\\');
            }
            overallPatternBuilder.append(ch);
        }

        return snippets.stream().map(snippet -> snippet.substring(numLiteralCharacters)).collect(Collectors.toList());
    }

    /**
     * The first time a particular field name is passed, simply return it.
     * The second time return it with "2" appended.
     * The third time return it with "3" appended.
     * Etc.
     */
    static String buildFieldName(Map<String, Integer> fieldNameCountStore, String fieldName) {
        Integer numberSeen = fieldNameCountStore.compute(fieldName, (k, v) -> 1 + ((v == null) ? 0 : v));
        return (numberSeen > 1) ? fieldName + numberSeen : fieldName;
    }

    public static void addIntermediateRegex(StringBuilder overallPatternBuilder, Collection<String> snippets) {
        if (snippets.isEmpty()) {
            return;
        }

        List<String> others = new ArrayList<>(snippets);
        String driver = others.remove(others.size() - 1);

        boolean wildcardRequired = true;
        for (int i = 0; i < driver.length(); ++i) {
            char ch = driver.charAt(i);
            if (PUNCTUATION_OR_SPACE.indexOf(ch) >= 0 && others.stream().allMatch(other -> other.indexOf(ch) >= 0)) {
                if (wildcardRequired && others.stream().anyMatch(other -> other.indexOf(ch) > 0)) {
                    overallPatternBuilder.append(".*?");
                }
                if (NEEDS_ESCAPING.indexOf(ch) >= 0) {
                    overallPatternBuilder.append('\\');
                }
                overallPatternBuilder.append(ch);
                wildcardRequired = true;
                others = others.stream().map(other -> other.substring(other.indexOf(ch) + 1)).collect(Collectors.toList());
            } else if (wildcardRequired) {
                overallPatternBuilder.append(".*?");
                wildcardRequired = false;
            }
        }

        if (wildcardRequired && others.stream().allMatch(String::isEmpty) == false) {
            overallPatternBuilder.append(".*?");
        }
    }

    private static void finalizeGrokPattern(StringBuilder overallPatternBuilder, Collection<String> snippets) {
        if (snippets.stream().allMatch(String::isEmpty)) {
            return;
        }

        List<String> others = new ArrayList<>(snippets);
        String driver = others.remove(others.size() - 1);

        for (int i = 0; i < driver.length(); ++i) {
            char ch = driver.charAt(i);
            int driverIndex = i;
            if (PUNCTUATION_OR_SPACE.indexOf(ch) >= 0 &&
                others.stream().allMatch(other -> other.length() > driverIndex && other.charAt(driverIndex) == ch)) {
                if (NEEDS_ESCAPING.indexOf(ch) >= 0) {
                    overallPatternBuilder.append('\\');
                }
                overallPatternBuilder.append(ch);
                if (i == driver.length() - 1 && others.stream().allMatch(driver::equals)) {
                    return;
                }
            } else {
                break;
            }
        }

        overallPatternBuilder.append(".*");
    }

    interface GrokPatternCandidate {

        /**
         * @return Does this Grok pattern candidate match all the snippets?
         */
        boolean matchesAll(Collection<String> snippets);

        /**
         * After it has been determined that this Grok pattern candidate matches a collection of strings,
         * return collections of the bits that come before (prefaces) and after (epilogues) the bit
         * that matches.  Also update mappings with the most appropriate field name and type.
         * @return The string that needs to be incorporated into the overall Grok pattern for the line.
         */
        String processCaptures(Map<String, Integer> fieldNameCountStore, Collection<String> snippets, Collection<String> prefaces,
                               Collection<String> epilogues, Map<String, String> mappings);
    }

    /**
     * A Grok pattern candidate that will match a single named Grok pattern.
     */
    static class ValueOnlyGrokPatternCandidate implements GrokPatternCandidate {

        private final String grokPatternName;
        private final String mappingType;
        private final String fieldName;
        private final Grok grok;

        /**
         * Pre/post breaks default to \b, but this may not be appropriate for Grok patterns that start or
         * end with a non "word" character (i.e. letter, number or underscore).  For such patterns use one
         * of the other constructors.
         * <p>
         * In cases where the Grok pattern defined by Logstash already includes conditions on what must
         * come before and after the match, use one of the other constructors and specify an empty string
         * for the pre and/or post breaks.
         *
         * @param grokPatternName Name of the Grok pattern to try to match - must match one defined in Logstash.
         * @param fieldName       Name of the field to extract from the match.
         */
        ValueOnlyGrokPatternCandidate(String grokPatternName, String mappingType, String fieldName) {
            this(grokPatternName, mappingType, fieldName, "\\b", "\\b");
        }

        /**
         * @param grokPatternName Name of the Grok pattern to try to match - must match one defined in Logstash.
         * @param mappingType     Data type for field in Elasticsearch mappings.
         * @param fieldName       Name of the field to extract from the match.
         * @param preBreak        Only consider the match if it's broken from the previous text by this.
         * @param postBreak       Only consider the match if it's broken from the following text by this.
         */
        ValueOnlyGrokPatternCandidate(String grokPatternName, String mappingType, String fieldName, String preBreak, String postBreak) {
            this.grokPatternName = grokPatternName;
            this.mappingType = mappingType;
            this.fieldName = fieldName;
            // The (?m) here has the Ruby meaning, which is equivalent to (?s) in Java
            grok = new Grok(Grok.getBuiltinPatterns(), "(?m)%{DATA:" + PREFACE + "}" + preBreak +
                "%{" + grokPatternName + ":" + VALUE + "}" + postBreak + "%{GREEDYDATA:" + EPILOGUE + "}");
        }

        @Override
        public boolean matchesAll(Collection<String> snippets) {
            return snippets.stream().allMatch(grok::match);
        }

        /**
         * Given a collection of strings, and a Grok pattern that matches some part of them all,
         * return collections of the bits that come before (prefaces) and after (epilogues) the
         * bit that matches.
         */
        @Override
        public String processCaptures(Map<String, Integer> fieldNameCountStore, Collection<String> snippets, Collection<String> prefaces,
                                      Collection<String> epilogues, Map<String, String> mappings) {
            String sampleValue = null;
            for (String snippet : snippets) {
                Map<String, Object> captures = grok.captures(snippet);
                // If the pattern doesn't match then captures will be null
                if (captures == null) {
                    throw new IllegalStateException("[%{" + grokPatternName + "}] does not match snippet [" + snippet + "]");
                }
                prefaces.add(captures.getOrDefault(PREFACE, "").toString());
                if (sampleValue == null) {
                    sampleValue = captures.get(VALUE).toString();
                }
                epilogues.add(captures.getOrDefault(EPILOGUE, "").toString());
            }
            String adjustedFieldName = buildFieldName(fieldNameCountStore, fieldName);
            if (mappings != null) {
                String fullMappingType = mappingType;
                if ("date".equals(mappingType)) {
                    TimestampMatch timestampMatch = TimestampFormatFinder.findFirstFullMatch(sampleValue);
                    if (timestampMatch != null) {
                        fullMappingType = timestampMatch.getEsDateMappingTypeWithFormat();
                    }
                }
                mappings.put(adjustedFieldName, fullMappingType);
            }
            return "%{" + grokPatternName + ":" + adjustedFieldName + "}";
        }
    }

    /**
     * Unlike the {@link ValueOnlyGrokPatternCandidate} an object of this class is not immutable and not thread safe.
     * When a given object matches a set of strings it chooses a field name.  Then that same field name is used when
     * processing captures from the pattern.  Hence only a single thread may use any particular instance of this
     * class.
     */
    static class KeyValueGrokPatternCandidate implements GrokPatternCandidate {

        private static final Pattern kvFinder = Pattern.compile("\\b(\\w+)=[\\w.-]+");
        private final Terminal terminal;
        private String fieldName;

        KeyValueGrokPatternCandidate(Terminal terminal) {
            this.terminal = terminal;
        }

        @Override
        public boolean matchesAll(Collection<String> snippets) {
            Set<String> candidateNames = new LinkedHashSet<>();
            boolean isFirst = true;
            for (String snippet : snippets) {
                if (isFirst) {
                    Matcher matcher = kvFinder.matcher(snippet);
                    while (matcher.find()) {
                        candidateNames.add(matcher.group(1));
                    }
                    isFirst = false;
                } else {
                    candidateNames.removeIf(candidateName ->
                        Pattern.compile("\\b" + candidateName + "=[\\w.-]+").matcher(snippet).find() == false);
                }
                if (candidateNames.isEmpty()) {
                    break;
                }
            }
            return (fieldName = candidateNames.stream().findFirst().orElse(null)) != null;
        }

        @Override
        public String processCaptures(Map<String, Integer> fieldNameCountStore, Collection<String> snippets, Collection<String> prefaces,
                                      Collection<String> epilogues, Map<String, String> mappings) {
            if (fieldName == null) {
                throw new IllegalStateException("Cannot process KV matches until a field name has been determined");
            }
            Grok grok = new Grok(Grok.getBuiltinPatterns(), "(?m)%{DATA:" + PREFACE + "}\\b" +
                fieldName + "=%{USER:" + VALUE + "}%{GREEDYDATA:" + EPILOGUE + "}");
            Collection<String> values = new ArrayList<>();
            for (String snippet : snippets) {
                Map<String, Object> captures = grok.captures(snippet);
                // If the pattern doesn't match then captures will be null
                if (captures == null) {
                    throw new IllegalStateException("[\\b" + fieldName + "=%{USER}] does not match snippet [" + snippet + "]");
                }
                prefaces.add(captures.getOrDefault(PREFACE, "").toString());
                values.add(captures.getOrDefault(VALUE, "").toString());
                epilogues.add(captures.getOrDefault(EPILOGUE, "").toString());
            }
            String adjustedFieldName = buildFieldName(fieldNameCountStore, fieldName);
            if (mappings != null) {
                mappings.put(adjustedFieldName, AbstractLogFileStructure.guessScalarMapping(terminal, adjustedFieldName, values));
            }
            return "\\b" + fieldName + "=%{USER:" + adjustedFieldName + "}";
        }
    }

    /**
     * A Grok pattern candidate that matches a single named Grok pattern but will not update mappings.
     */
    static class NoMappingGrokPatternCandidate extends ValueOnlyGrokPatternCandidate {

        NoMappingGrokPatternCandidate(String grokPatternName, String fieldName) {
            super(grokPatternName, null, fieldName);
        }

        @Override
        public String processCaptures(Map<String, Integer> fieldNameCountStore, Collection<String> snippets, Collection<String> prefaces,
                                      Collection<String> epilogues, Map<String, String> mappings) {
            return super.processCaptures(fieldNameCountStore, snippets, prefaces, epilogues, null);
        }
    }

    /**
     * Used to check whether a single Grok pattern matches every sample message in its entirety.
     */
    static class FullMatchGrokPatternCandidate {

        private final String grokString;
        private final String timeField;
        private final Grok grok;

        FullMatchGrokPatternCandidate(String grokPatternName, String timeField) {
            grokString = "%{" + grokPatternName + "}";
            this.timeField = timeField;
            grok = new Grok(Grok.getBuiltinPatterns(), grokString);
        }

        public boolean matchesAll(Collection<String> sampleMessages) {
            return sampleMessages.stream().allMatch(grok::match);
        }

        /**
         * This must only be called if {@link #matchesAll} returns <code>true</code>.
         * @return A tuple of (time field name, Grok string).
         */
        public Tuple<String, String> processMatch(Terminal terminal, Collection<String> sampleMessages, Map<String, String> mappings) {

            terminal.println(Verbosity.VERBOSE, "A full message Grok pattern [" + grokString.substring(2, grokString.length() - 1) +
                "] looks appropriate");

            if (mappings != null) {
                Map<String, Collection<String>> valuesPerField = new HashMap<>();

                for (String sampleMessage : sampleMessages) {
                    Map<String, Object> captures = grok.captures(sampleMessage);
                    // If the pattern doesn't match then captures will be null
                    if (captures == null) {
                        throw new IllegalStateException("[" + grokString + "] does not match snippet [" + sampleMessage + "]");
                    }
                    for (Map.Entry<String, Object> capture : captures.entrySet()) {

                        String fieldName = capture.getKey();
                        String fieldValue = capture.getValue().toString();

                        // Exclude the time field because that will be dropped and replaced with @timestamp
                        if (fieldName.equals(timeField) == false) {
                            valuesPerField.compute(fieldName, (k, v) -> {
                                if (v == null) {
                                    return new ArrayList<>(Collections.singletonList(fieldValue));
                                } else {
                                    v.add(fieldValue);
                                    return v;
                                }
                            });
                        }
                    }
                }

                for (Map.Entry<String, Collection<String>> valuesForField : valuesPerField.entrySet()) {
                    String fieldName = valuesForField.getKey();
                    mappings.put(fieldName, AbstractLogFileStructure.guessScalarMapping(terminal, fieldName, valuesForField.getValue()));
                }
            }

            return new Tuple<>(timeField, grokString);
        }
    }
}
