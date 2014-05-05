package org.elasticsearch.common.lucene.search.profile;


import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.elasticsearch.common.lucene.search.*;

import java.lang.reflect.Field;
import java.util.ArrayList;

/**
 * This class walks a query and wraps all applicable queries in ProfileQuery queries
 */
public class ProfileQueryVisitor extends Visitor<Object, ProfileComponent> {

    public ProfileQueryVisitor() {
        super(ProfileQueryVisitor.class, Object.class, ProfileComponent.class);
    }

    public ProfileQuery visit(BooleanQuery booleanQuery) {

        // TODO replace this later with in-place updates
        BooleanQuery newQuery = new BooleanQuery(booleanQuery.isCoordDisabled());

        for (BooleanClause clause : booleanQuery.clauses()) {
            ProfileQuery pQuery = (ProfileQuery) apply(clause.getQuery());
            newQuery.add(pQuery, clause.getOccur());
        }

        return new ProfileQuery(newQuery);
    }

    public ProfileQuery visit(XFilteredQuery query) {
        ProfileQuery pQuery = (ProfileQuery) apply(query.getQuery());
        ProfileFilter pFilter = (ProfileFilter) apply(query.getFilter());

        XFilteredQuery newQuery = new XFilteredQuery(pQuery, pFilter);
        return new ProfileQuery(newQuery);
    }

    public ProfileQuery visit(XConstantScoreQuery query) {
        ProfileFilter pFilter = (ProfileFilter) apply(query.getFilter());

        return new ProfileQuery(new XConstantScoreQuery(pFilter));
    }

    public ProfileFilter visit(XBooleanFilter boolFilter) {
        XBooleanFilter newFilter = new XBooleanFilter();

        for (FilterClause clause : boolFilter.clauses()) {
            ProfileFilter pFilter = (ProfileFilter) apply(clause.getFilter());
            newFilter.add(pFilter, clause.getOccur());
        }

        return new ProfileFilter(newFilter);
    }

    public ProfileFilter visit(AndFilter filter) {
        ArrayList<ProfileFilter> pFilters = new ArrayList<ProfileFilter>(filter.filters().size());
        for (Filter f : filter.filters()) {
            pFilters.add((ProfileFilter)apply(f));
        }
        return new ProfileFilter(new AndFilter(pFilters));
    }

    public ProfileFilter visit(OrFilter filter) {
        ArrayList<ProfileFilter> pFilters = new ArrayList<ProfileFilter>(filter.filters().size());
        for (Filter f : filter.filters()) {
            pFilters.add((ProfileFilter)apply(f));
        }
        return new ProfileFilter(new OrFilter(pFilters));
    }

    public ProfileQuery visit(ToParentBlockJoinQuery query) throws NoSuchFieldException, IllegalAccessException {
        Field origChildQueryField = query.getClass().getDeclaredField("origChildQuery");
        origChildQueryField.setAccessible(true);

        Field parentsFilterField = query.getClass().getDeclaredField("parentsFilter");
        parentsFilterField.setAccessible(true);

        Field scoreModeField = query.getClass().getDeclaredField("scoreMode");
        scoreModeField.setAccessible(true);

        ProfileQuery innerQuery = (ProfileQuery) apply(origChildQueryField.get(query));
        ProfileFilter parentsFilter = (ProfileFilter) apply(parentsFilterField.get(query));
        ScoreMode scoreMode = (ScoreMode) scoreModeField.get(query);

        return new ProfileQuery(new ToParentBlockJoinQuery(innerQuery, parentsFilter, scoreMode));
    }

    public ProfileFilter visit(NotFilter filter) {
        return new ProfileFilter((ProfileFilter)apply(filter));
    }

    public ProfileQuery visit(Query query) {
        return new ProfileQuery(query);
    }

    public ProfileFilter visit(Filter filter) {
        return new ProfileFilter(filter);
    }
}
