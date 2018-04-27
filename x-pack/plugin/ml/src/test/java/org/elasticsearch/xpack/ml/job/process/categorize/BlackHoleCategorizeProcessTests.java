/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.categorize;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.FlushJobParams;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;

import java.util.Iterator;

public class BlackHoleCategorizeProcessTests extends ESTestCase {

    public void testFlushJob_writesAck() {
        try (BlackHoleCategorizeProcess process = new BlackHoleCategorizeProcess()) {
            String flushId = process.flushJob(FlushJobParams.builder().build());
            Iterator<AutodetectResult> iterator = process.readResults();
            iterator.hasNext();
            AutodetectResult result = iterator.next();
            FlushAcknowledgement ack = result.getFlushAcknowledgement();
            assertEquals(flushId, ack.getId());
        }
    }
}
