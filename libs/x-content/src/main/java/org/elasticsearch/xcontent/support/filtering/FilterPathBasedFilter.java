/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.support.filtering;

import com.fasterxml.jackson.core.filter.TokenFilter;

import org.elasticsearch.core.Glob;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FilterPathBasedFilter extends TokenFilter {

    /**
     * Marker value that should be used to indicate that a property name
     * or value matches one of the filter paths.
     */
    private static final TokenFilter MATCHING = new TokenFilter() {
        @Override
        public String toString() {
            return "MATCHING";
        }
    };

    /**
     * Marker value that should be used to indicate that none of the
     * property names/values matches one of the filter paths.
     */
    private static final TokenFilter NO_MATCHING = new TokenFilter() {
        @Override
        public String toString() {
            return "NO_MATCHING";
        }
    };

    private final FilterNode[] filters;

    private final boolean inclusive;

    public FilterPathBasedFilter(FilterNode[] filters, boolean inclusive) {
        if (filters == null || filters.length == 0) {
            throw new IllegalArgumentException("filters cannot be null or empty");
        }
        this.inclusive = inclusive;
        this.filters = filters;
    }

    public FilterPathBasedFilter(Set<String> filters, boolean inclusive) {
        this(FilterNode.compile(filters), inclusive);
    }

    /**
     * Evaluates if a property name matches one of the given filter paths.
     */
    private TokenFilter evaluate(String name, FilterNode[] filterNodes) {
        if (filterNodes != null) {
            List<FilterNode> nextFilters = new ArrayList<>();
            for (FilterNode filter : filterNodes) {
                if (filter.isDoubleWildcard()) {
                    nextFilters.add(filter);
                }

                FilterNode termNode = filter.getTermFilter(name);
                if (termNode != null) {
                    if (termNode.isEnd()) {
                        return MATCHING;
                    } else {
                        nextFilters.add(termNode);
                    }
                }

                for (Map.Entry<String, FilterNode> entry : filter.getWildcardFilters().entrySet()) {
                    String wildcardPattern = entry.getKey();
                    FilterNode wildcardNode = entry.getValue();
                    if (Glob.globMatch(wildcardPattern, name)) {
                        if (wildcardNode.isEnd()) {
                            return MATCHING;
                        } else {
                            nextFilters.add(wildcardNode);
                        }
                    }
                }
            }

            if (nextFilters.isEmpty() == false) {
                return new FilterPathBasedFilter(nextFilters.toArray(new FilterNode[nextFilters.size()]), inclusive);
            }
        }
        return NO_MATCHING;
    }

    @Override
    public TokenFilter includeProperty(String name) {
        TokenFilter filter = evaluate(name, filters);
        if (filter == MATCHING) {
            return inclusive ? TokenFilter.INCLUDE_ALL : null;
        }
        if (filter == NO_MATCHING) {
            return inclusive ? null : TokenFilter.INCLUDE_ALL;
        }
        return filter;
    }

    @Override
    protected boolean _includeScalar() {
        return inclusive == false;
    }
}
