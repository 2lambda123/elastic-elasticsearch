/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class LambdaMatcher {

    private static class PredicateMatcher<T> extends TypeSafeDiagnosingMatcher<T> {
        private final Predicate<? super T> predicate;
        private final Consumer<Description> description;

        private PredicateMatcher(Predicate<? super T> predicate, Consumer<Description> description) {
            this.predicate = predicate;
            this.description = description;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("matches predicate ");
            this.description.accept(description);
        }

        @Override
        protected boolean matchesSafely(T item, Description mismatchDescription) {
            boolean result = predicate.test(item);
            if (result == false) {
                mismatchDescription.appendValue(item).appendText(" did not match predicate ");
                this.description.accept(mismatchDescription);
            }
            return result;
        }
    }

    public static <T> Matcher<T> matches(Predicate<? super T> predicate) {
        return new PredicateMatcher<>(predicate, d -> d.appendValue(predicate));
    }

    public static <T> Matcher<T> matches(Predicate<? super T> predicate, String description) {
        return new PredicateMatcher<>(predicate, d -> d.appendText(description));
    }

    public static <T> Matcher<T> matches(Predicate<? super T> predicate, Supplier<String> description) {
        return new PredicateMatcher<>(predicate, d -> d.appendText(description.get()));
    }

    private static class EveryArrayItem<T> extends TypeSafeDiagnosingMatcher<T[]> {
        private final Matcher<T> itemMatcher;

        private EveryArrayItem(Matcher<T> itemMatcher) {
            this.itemMatcher = itemMatcher;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("every item is ").appendDescriptionOf(itemMatcher);
        }

        @Override
        protected boolean matchesSafely(T[] array, Description mismatchDescription) {
            for (T t : array) {
                if (itemMatcher.matches(t) == false) {
                    mismatchDescription.appendText("an item ");
                    itemMatcher.describeMismatch(t, mismatchDescription);
                    return false;
                }
            }
            return true;
        }
    }

    public static <T> Matcher<T[]> everyItemMatches(Predicate<? super T> predicate) {
        return new EveryArrayItem<>(matches(predicate));
    }

    public static <T> Matcher<T[]> everyItemMatches(Predicate<? super T> predicate, String description) {
        return new EveryArrayItem<>(matches(predicate, description));
    }

    public static <T> Matcher<T[]> everyItemMatches(Predicate<? super T> predicate, Supplier<String> description) {
        return new EveryArrayItem<>(matches(predicate, description));
    }
}
