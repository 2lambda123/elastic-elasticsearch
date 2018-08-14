/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.throttler;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.protocol.xpack.watcher.status.ActionAckStatus;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.actions.throttler.AckThrottler;
import org.elasticsearch.xpack.core.watcher.actions.throttler.Throttler;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.joda.time.DateTime;

import java.time.Clock;

import static org.elasticsearch.protocol.xpack.watcher.status.WatcherDateTimeUtils.formatDate;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.mockExecutionContext;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.joda.time.DateTimeZone.UTC;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AckThrottlerTests extends ESTestCase {
    public void testWhenAcked() throws Exception {
        DateTime timestamp = new DateTime(Clock.systemUTC().millis(), UTC);
        WatchExecutionContext ctx = mockExecutionContext("_watch", Payload.EMPTY);
        Watch watch = ctx.watch();
        ActionStatus actionStatus = mock(ActionStatus.class);
        when(actionStatus.ackStatus()).thenReturn(new ActionAckStatus(timestamp, ActionAckStatus.State.ACKED));
        WatchStatus watchStatus = mock(WatchStatus.class);
        when(watchStatus.actionStatus("_action")).thenReturn(actionStatus);
        when(watch.status()).thenReturn(watchStatus);
        AckThrottler throttler = new AckThrottler();
        Throttler.Result result = throttler.throttle("_action", ctx);
        assertThat(result.throttle(), is(true));
        assertThat(result.reason(), is("action [_action] was acked at [" + formatDate(timestamp) + "]"));
        assertThat(result.type(), is(Throttler.Type.ACK));
    }

    public void testThrottleWhenAwaitsSuccessfulExecution() throws Exception {
        DateTime timestamp = new DateTime(Clock.systemUTC().millis(), UTC);
        WatchExecutionContext ctx = mockExecutionContext("_watch", Payload.EMPTY);
        Watch watch = ctx.watch();
        ActionStatus actionStatus = mock(ActionStatus.class);
        when(actionStatus.ackStatus()).thenReturn(new ActionAckStatus(timestamp,
                ActionAckStatus.State.AWAITS_SUCCESSFUL_EXECUTION));
        WatchStatus watchStatus = mock(WatchStatus.class);
        when(watchStatus.actionStatus("_action")).thenReturn(actionStatus);
        when(watch.status()).thenReturn(watchStatus);
        AckThrottler throttler = new AckThrottler();
        Throttler.Result result = throttler.throttle("_action", ctx);
        assertThat(result.throttle(), is(false));
        assertThat(result.reason(), nullValue());
    }

    public void testThrottleWhenAckable() throws Exception {
        DateTime timestamp = new DateTime(Clock.systemUTC().millis(), UTC);
        WatchExecutionContext ctx = mockExecutionContext("_watch", Payload.EMPTY);
        Watch watch = ctx.watch();
        ActionStatus actionStatus = mock(ActionStatus.class);
        when(actionStatus.ackStatus()).thenReturn(new ActionAckStatus(timestamp, ActionAckStatus.State.ACKABLE));
        WatchStatus watchStatus = mock(WatchStatus.class);
        when(watchStatus.actionStatus("_action")).thenReturn(actionStatus);
        when(watch.status()).thenReturn(watchStatus);
        AckThrottler throttler = new AckThrottler();
        Throttler.Result result = throttler.throttle("_action", ctx);
        assertThat(result.throttle(), is(false));
        assertThat(result.reason(), nullValue());
    }
}
