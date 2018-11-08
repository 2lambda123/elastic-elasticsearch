/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.process;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MlMemoryTrackerTests extends ESTestCase {

    private ClusterService clusterService;
    private ThreadPool threadPool;
    private JobManager jobManager;
    private JobResultsProvider jobResultsProvider;
    private MlMemoryTracker memoryTracker;

    @Before
    public void setup() {

        clusterService = mock(ClusterService.class);
        threadPool = mock(ThreadPool.class);
        ExecutorService executorService = mock(ExecutorService.class);
        when(threadPool.executor(anyString())).thenReturn(executorService);
        jobManager = mock(JobManager.class);
        jobResultsProvider = mock(JobResultsProvider.class);
        memoryTracker = new MlMemoryTracker(clusterService, threadPool, jobManager, jobResultsProvider);
    }

    public void testRefreshAll() {

        boolean isMaster = randomBoolean();
        if (isMaster) {
            memoryTracker.onMaster();
        } else {
            memoryTracker.offMaster();
        }

        int numMlJobTasks = randomIntBetween(2, 5);
        Map<String, PersistentTasksCustomMetaData.PersistentTask<?>> tasks = new HashMap<>();
        for (int i = 1; i <= numMlJobTasks; ++i) {
            String jobId = "job" + i;
            PersistentTasksCustomMetaData.PersistentTask<?> task = makeTestTask(jobId);
            tasks.put(task.getId(), task);
        }
        PersistentTasksCustomMetaData persistentTasks = new PersistentTasksCustomMetaData(numMlJobTasks, tasks);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Consumer<Long> listener = (Consumer<Long>) invocation.getArguments()[3];
            listener.accept(randomLongBetween(1000, 1000000));
            return null;
        }).when(jobResultsProvider).getEstablishedMemoryUsage(any(), any(), any(), any(), any());

        memoryTracker.refresh(persistentTasks, ActionListener.wrap(aVoid -> {}, ESTestCase::assertNull));

        if (isMaster) {
            for (int i = 1; i <= numMlJobTasks; ++i) {
                String jobId = "job" + i;
                verify(jobResultsProvider, times(1)).getEstablishedMemoryUsage(eq(jobId), any(), any(), any(), any());
            }
        } else {
            verify(jobResultsProvider, never()).getEstablishedMemoryUsage(anyString(), any(), any(), any(), any());
        }
    }

    public void testRefreshOne() {

        boolean isMaster = randomBoolean();
        if (isMaster) {
            memoryTracker.onMaster();
        } else {
            memoryTracker.offMaster();
        }

        String jobId = "job";
        boolean haveEstablishedModelMemory = randomBoolean();

        long modelBytes = 1024 * 1024;
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Consumer<Long> listener = (Consumer<Long>) invocation.getArguments()[3];
            listener.accept(haveEstablishedModelMemory ? modelBytes : 0L);
            return null;
        }).when(jobResultsProvider).getEstablishedMemoryUsage(eq(jobId), any(), any(), any(), any());

        long modelMemoryLimitMb = 2;
        Job job = mock(Job.class);
        when(job.getAnalysisLimits()).thenReturn(new AnalysisLimits(modelMemoryLimitMb, 4L));
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Job> listener = (ActionListener<Job>) invocation.getArguments()[1];
            listener.onResponse(job);
            return null;
        }).when(jobManager).getJob(eq(jobId), any());

        AtomicReference<Long> refreshedMemoryRequirement = new AtomicReference<>();
        memoryTracker.refreshJobMemory(jobId, ActionListener.wrap(refreshedMemoryRequirement::set, ESTestCase::assertNull));

        if (isMaster) {
            if (haveEstablishedModelMemory) {
                assertEquals(Long.valueOf(modelBytes + Job.PROCESS_MEMORY_OVERHEAD.getBytes()),
                    memoryTracker.getJobMemoryRequirement(jobId));
            } else {
                assertEquals(Long.valueOf(ByteSizeUnit.MB.toBytes(modelMemoryLimitMb) + Job.PROCESS_MEMORY_OVERHEAD.getBytes()),
                    memoryTracker.getJobMemoryRequirement(jobId));
            }
        } else {
            assertNull(memoryTracker.getJobMemoryRequirement(jobId));
        }

        assertEquals(memoryTracker.getJobMemoryRequirement(jobId), refreshedMemoryRequirement.get());

        memoryTracker.removeJob(jobId);
        assertNull(memoryTracker.getJobMemoryRequirement(jobId));
    }

    private PersistentTasksCustomMetaData.PersistentTask<OpenJobAction.JobParams> makeTestTask(String jobId) {
        return new PersistentTasksCustomMetaData.PersistentTask<>("job-" + jobId, MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams(jobId),
            0, PersistentTasksCustomMetaData.INITIAL_ASSIGNMENT);
    }
}
