/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.transport;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.ScheduledFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TcpTransportKeepAliveTests extends ESTestCase {

    private final ConnectionProfile defaultProfile = ConnectionProfile.buildDefaultConnectionProfile(Settings.EMPTY);
    private TcpTransportKeepAlive.PingSender pingSender;
    private TcpTransportKeepAlive keepAlive;
    private CapturingThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        pingSender = mock(TcpTransportKeepAlive.PingSender.class);
        threadPool = new CapturingThreadPool();
        keepAlive = new TcpTransportKeepAlive(threadPool, pingSender);
    }

    @Override
    public void tearDown() throws Exception {
        threadPool.shutdown();
        super.tearDown();
    }

    public void testRegisterNodeConnectionSchedulesKeepAlive() {
        TimeValue pingInterval = TimeValue.timeValueSeconds(randomLongBetween(1, 60));
        ConnectionProfile connectionProfile = new ConnectionProfile.Builder(defaultProfile)
            .setPingInterval(pingInterval)
            .build();

        assertEquals(0, threadPool.scheduledTasks.size());

        TcpChannel channel1 = new FakeTcpChannel();
        TcpChannel channel2 = new FakeTcpChannel();
        keepAlive.registerNodeConnection(Arrays.asList(channel1, channel2), connectionProfile);

        assertEquals(1, threadPool.scheduledTasks.size());
        Tuple<TimeValue, Runnable> taskTuple = threadPool.scheduledTasks.poll();
        assertEquals(pingInterval, taskTuple.v1());
        Runnable keepAliveTask = taskTuple.v2();
        assertEquals(0, threadPool.scheduledTasks.size());
        keepAliveTask.run();

        verify(pingSender, times(1)).send(same(channel1), any(BytesReference.class), any());
        verify(pingSender, times(1)).send(same(channel2), any(BytesReference.class), any());

        // Test that the task has rescheduled itself
        assertEquals(1, threadPool.scheduledTasks.size());
        Tuple<TimeValue, Runnable> rescheduledTask = threadPool.scheduledTasks.poll();
        assertEquals(pingInterval, rescheduledTask.v1());
    }

    public void testRegisterMultipleKeepAliveIntervals() {
        TimeValue pingInterval1 = TimeValue.timeValueSeconds(randomLongBetween(1, 30));
        ConnectionProfile connectionProfile1 = new ConnectionProfile.Builder(defaultProfile)
            .setPingInterval(pingInterval1)
            .build();

        TimeValue pingInterval2 = TimeValue.timeValueSeconds(randomLongBetween(31, 60));
        ConnectionProfile connectionProfile2 = new ConnectionProfile.Builder(defaultProfile)
            .setPingInterval(pingInterval2)
            .build();

        assertEquals(0, threadPool.scheduledTasks.size());

        TcpChannel channel1 = new FakeTcpChannel();
        TcpChannel channel2 = new FakeTcpChannel();
        keepAlive.registerNodeConnection(Arrays.asList(channel1), connectionProfile1);
        keepAlive.registerNodeConnection(Arrays.asList(channel2), connectionProfile2);

        assertEquals(2, threadPool.scheduledTasks.size());
        Tuple<TimeValue, Runnable> taskTuple1 = threadPool.scheduledTasks.poll();
        Tuple<TimeValue, Runnable> taskTuple2 = threadPool.scheduledTasks.poll();
        assertEquals(pingInterval1, taskTuple1.v1());
        assertEquals(pingInterval2, taskTuple2.v1());
        Runnable keepAliveTask1 = taskTuple1.v2();
        Runnable keepAliveTask2 = taskTuple1.v2();

        assertEquals(0, threadPool.scheduledTasks.size());
        keepAliveTask1.run();
        assertEquals(1, threadPool.scheduledTasks.size());
        keepAliveTask2.run();
        assertEquals(2, threadPool.scheduledTasks.size());

    }

    private class CapturingThreadPool extends TestThreadPool {

        private final Deque<Tuple<TimeValue, Runnable>> scheduledTasks = new ArrayDeque<>();

        private CapturingThreadPool() {
            super(getTestName());
        }

        @Override
        public ScheduledFuture<?> schedule(TimeValue delay, String executor, Runnable task) {
            scheduledTasks.add(new Tuple<>(delay, task));
            return null;
        }
    }
}
