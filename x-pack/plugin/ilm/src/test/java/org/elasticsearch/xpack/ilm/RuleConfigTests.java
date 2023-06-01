/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.mockito.Mockito.mock;

public class RuleConfigTests extends ESTestCase {
    public void testActionRuleConfig() {
        var actionName = randomAlphaOfLength(30);
        assertTrue(
            new IlmHealthIndicatorService.ActionRule(actionName, null).test(
                randomNonNegativeLong(),
                metadataAction(actionName, randomNonNegativeLong())
            )
        );
        assertFalse(
            new IlmHealthIndicatorService.ActionRule(actionName, null).test(
                randomNonNegativeLong(),
                metadataAction(randomAlphaOfLength(30), randomNonNegativeLong())
            )
        );

        var maxTimeOn = TimeValue.parseTimeValue(randomTimeValue(), "");
        var rule = new IlmHealthIndicatorService.ActionRule(actionName, maxTimeOn);
        var now = System.currentTimeMillis();

        assertFalse(rule.test(now, metadataAction(randomAlphaOfLength(30), now)));
        assertFalse(rule.test(now, metadataAction(actionName, now)));
        assertTrue(rule.test(now, metadataAction(actionName, now - maxTimeOn.millis() - randomIntBetween(1000, 100000))));
    }

    public void testStepRuleConfig() {
        var stepName = randomAlphaOfLength(30);
        var maxTimeOn = TimeValue.parseTimeValue(randomTimeValue(), "");
        var maxRetries = randomIntBetween(11, 100);
        var rule = new IlmHealthIndicatorService.StepRule(stepName, maxTimeOn, maxRetries);
        var now = System.currentTimeMillis();

        // rule is not for this step
        assertFalse(rule.test(now, metadataStep(randomAlphaOfLength(30), now, maxRetries + 1)));

        // step still has time to run && can continue retrying
        assertFalse(rule.test(now, metadataStep(stepName, now, maxRetries - randomIntBetween(0, 10))));

        // step still has run longer than expected
        assertTrue(
            rule.test(
                now,
                metadataStep(stepName, now - maxTimeOn.millis() - randomIntBetween(1000, 100000), maxRetries - randomIntBetween(0, 10))
            )
        );

        // step still has time to run but have retried more than expected
        assertTrue(rule.test(now, metadataStep(stepName, now, maxRetries + randomIntBetween(1, 10))));
    }

    public void testRuleChaining() {
        var mockedMd = mock(IndexMetadata.class);
        var someLong = randomLong();

        assertTrue(ruleAlwaysReturn(true).test(someLong, mockedMd));
        assertFalse(ruleAlwaysReturn(false).test(someLong, mockedMd));

        // and
        assertTrue(ruleAlwaysReturn(true).and(ruleAlwaysReturn(true)).test(someLong, mockedMd));
        assertFalse(ruleAlwaysReturn(true).and(ruleAlwaysReturn(false)).test(someLong, mockedMd));
        assertFalse(ruleAlwaysReturn(false).and(ruleAlwaysReturn(false)).test(someLong, mockedMd));
        assertFalse(ruleAlwaysReturn(false).and(ruleAlwaysReturn(true)).test(someLong, mockedMd));

        // or
        assertTrue(ruleAlwaysReturn(true).or(ruleAlwaysReturn(true)).test(someLong, mockedMd));
        assertTrue(ruleAlwaysReturn(true).or(ruleAlwaysReturn(false)).test(someLong, mockedMd));
        assertFalse(ruleAlwaysReturn(false).or(ruleAlwaysReturn(false)).test(someLong, mockedMd));
        assertTrue(ruleAlwaysReturn(false).or(ruleAlwaysReturn(true)).test(someLong, mockedMd));
    }

    public void testGetElapsedTime() {
        var a = randomLongBetween(1000, 2000);
        var b = a - randomLongBetween(0, 500);
        assertEquals(IlmHealthIndicatorService.RuleConfig.getElapsedTime(a, b), TimeValue.timeValueMillis(a - b));
        assertEquals(IlmHealthIndicatorService.RuleConfig.getElapsedTime(a, null), TimeValue.ZERO);
    }

    private IlmHealthIndicatorService.RuleConfig ruleAlwaysReturn(boolean shouldReturn) {
        return (now, indexMetadata) -> shouldReturn;
    }

    private IndexMetadata metadataStep(String currentStep, long stepTime, long stepRetries) {
        return IndexMetadata.builder("some-index")
            .settings(settings(Version.CURRENT))
            .putCustom(
                ILM_CUSTOM_METADATA_KEY,
                Map.of("step", currentStep, "step_time", String.valueOf(stepTime), "failed_step_retry_count", String.valueOf(stepRetries))
            )
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
    }

    private IndexMetadata metadataAction(String currentAction, long actionTime) {
        return IndexMetadata.builder("some-index")
            .settings(settings(Version.CURRENT))
            .putCustom(ILM_CUSTOM_METADATA_KEY, Map.of("action", currentAction, "action_time", String.valueOf(actionTime)))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
    }
}
