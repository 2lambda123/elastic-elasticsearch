/*
 * Licensed to Elasticsearch under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elasticsearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.test.rest.section;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * Represents an is_false assert section:
 *
 *   - is_false:  get.fields.bar
 *
 */
public class IsFalseAssertion extends Assertion {

    private static final ESLogger logger = Loggers.getLogger(IsFalseAssertion.class);

    public IsFalseAssertion(String field) {
        super(field, false);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doAssert(Object actualValue, Object expectedValue) {
        logger.trace("assert that [{}] doesn't have a true value", actualValue);

        if (actualValue == null) {
            return;
        }

        String actualString = actualValue.toString();
        assertThat(errorMessage(), actualString, anyOf(
                equalTo(""),
                equalToIgnoringCase(Boolean.FALSE.toString()),
                equalTo("0")
        ));
    }

    private String errorMessage() {
        return "field [" + getField() + "] has a true value but it shouldn't";
    }
}
