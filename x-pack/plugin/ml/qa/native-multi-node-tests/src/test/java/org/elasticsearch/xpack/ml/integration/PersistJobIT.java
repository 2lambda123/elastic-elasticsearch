/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.ml.action.FlushJobAction;
import org.elasticsearch.xpack.core.ml.action.PersistJobAction;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.junit.After;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class PersistJobIT extends MlNativeAutodetectIntegTestCase {

    private static final long BUCKET_SPAN_SECONDS = 300;
    private static final TimeValue BUCKET_SPAN = TimeValue.timeValueSeconds(BUCKET_SPAN_SECONDS);

    @After
    public void cleanUpJobs() {
        cleanUp();
    }

    public void testPersistJob() throws Exception {
        String jobId = "persist-job-test";
        runJob(jobId);

        PersistJobAction.Response r = persistJob(jobId);
        assertTrue(r.isPersisted());

        // Persisting the job will create a model snapshot
        assertBusy(() -> {
            List<ModelSnapshot> snapshots = getModelSnapshots(jobId);
            assertFalse(snapshots.isEmpty());
        });
    }

    // check that state is persisted after time has been advanced even if no new data is seen in the interim
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/40347")
    public void testPersistJobOnGracefulShutdown_givenTimeAdvancedAfterNoNewData() throws Exception {
        String jobId = "time-advanced-after-no-new-data-test";

        // open and run a job with a small data set
        runJob(jobId);
        FlushJobAction.Response flushResponse = flushJob(jobId, true);

        closeJob(jobId);

        // Check that state has been persisted
        SearchResponse stateDocsResponse1 = client().prepareSearch(AnomalyDetectorsIndex.jobStateIndexPattern())
            .setFetchSource(false)
            .setTrackTotalHits(true)
            .setSize(10000)
            .get();

        int numQuantileRecords = 0;
        int numStateRecords = 0;
        for (SearchHit hit : stateDocsResponse1.getHits().getHits()) {
            logger.info(hit.getId());
            if (hit.getId().contains("quantiles")) {
                ++numQuantileRecords;
            } else if (hit.getId().contains("model_state")) {
                ++numStateRecords;
            }
        }
        assertThat(stateDocsResponse1.getHits().getTotalHits().value, equalTo(2L));
        assertThat(numQuantileRecords, equalTo(1));
        assertThat(numStateRecords, equalTo(1));

        // re-open the job
        openJob(jobId);

        // advance time
        long lastFinalizedBucketEnd = flushResponse.getLastFinalizedBucketEnd().getTime();
        FlushJobAction.Request advanceTimeRequest = new FlushJobAction.Request(jobId);
        advanceTimeRequest.setAdvanceTime(String.valueOf(lastFinalizedBucketEnd + BUCKET_SPAN_SECONDS * 1000));
        advanceTimeRequest.setCalcInterim(false);
        assertThat(client().execute(FlushJobAction.INSTANCE, advanceTimeRequest).actionGet().isFlushed(), is(true));

        closeJob(jobId);

        // Check that a new state record exists.
        SearchResponse stateDocsResponse2 = client().prepareSearch(AnomalyDetectorsIndex.jobStateIndexPattern())
            .setFetchSource(false)
            .setTrackTotalHits(true)
            .setSize(10000)
            .get();

        numQuantileRecords = 0;
        numStateRecords = 0;
        for (SearchHit hit : stateDocsResponse2.getHits().getHits()) {
            logger.info(hit.getId());
            if (hit.getId().contains("quantiles")) {
                ++numQuantileRecords;
            } else if (hit.getId().contains("model_state")) {
                ++numStateRecords;
            }
        }

        assertThat(stateDocsResponse2.getHits().getTotalHits().value, equalTo(3L));
        assertThat(numQuantileRecords, equalTo(1));
        assertThat(numStateRecords, equalTo(2));

        deleteJob(jobId);
    }

    // Check an edge case where time is manually advanced before any valid data is seen
    public void testPersistJobOnGracefulShutdown_givenNoDataAndTimeAdvanced() throws Exception {
        String jobId = "no-data-and-time-advanced-test";

        createAndOpenJob(jobId);

        // Manually advance time.
        FlushJobAction.Request advanceTimeRequest = new FlushJobAction.Request(jobId);
        advanceTimeRequest.setAdvanceTime(String.valueOf(BUCKET_SPAN_SECONDS * 1000));
        advanceTimeRequest.setCalcInterim(false);
        assertThat(client().execute(FlushJobAction.INSTANCE, advanceTimeRequest).actionGet().isFlushed(), is(true));

        closeJob(jobId);

        // Check that state has been persisted
        SearchResponse stateDocsResponse = client().prepareSearch(AnomalyDetectorsIndex.jobStateIndexPattern())
            .setFetchSource(false)
            .setTrackTotalHits(true)
            .setSize(10000)
            .get();

        int numQuantileRecords = 0;
        int numStateRecords = 0;
        for (SearchHit hit : stateDocsResponse.getHits().getHits()) {
            logger.info(hit.getId());
            if (hit.getId().contains("quantiles")) {
                ++numQuantileRecords;
            } else if (hit.getId().contains("model_state")) {
                ++numStateRecords;
            }
        }
        assertThat(stateDocsResponse.getHits().getTotalHits().value, equalTo(2L));
        assertThat(numQuantileRecords, equalTo(1));
        assertThat(numStateRecords, equalTo(1));

        // now check that the job can be happily restored - even though no data has been seen
        AcknowledgedResponse ack = openJob(jobId);
        assertTrue(ack.isAcknowledged());

        closeJob(jobId);
        deleteJob(jobId);
    }

    // Check an edge case where a job is opened and then immediately closed
    public void testPersistJobOnGracefulShutdown_givenNoDataAndNoTimeAdvance() throws Exception {
        String jobId = "no-data-and-no-time-advance-test";

        createAndOpenJob(jobId);

        closeJob(jobId);

        // Check that state has not been persisted
        SearchResponse stateDocsResponse = client().prepareSearch(AnomalyDetectorsIndex.jobStateIndexPattern())
            .setFetchSource(false)
            .setTrackTotalHits(true)
            .setSize(10000)
            .get();

        assertThat(stateDocsResponse.getHits().getTotalHits().value, equalTo(0L));

        deleteJob(jobId);
    }

    private void createAndOpenJob(String jobId) throws Exception {
        Detector.Builder detector = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Arrays.asList(detector.build()));
        analysisConfig.setBucketSpan(BUCKET_SPAN);
        Job.Builder job = new Job.Builder(jobId);
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(new DataDescription.Builder());
        registerJob(job);
        putJob(job);

        openJob(job.getId());
    }

    private void runJob(String jobId) throws Exception {
        createAndOpenJob(jobId);
        List<String> data = generateData(System.currentTimeMillis(), BUCKET_SPAN, 10, bucketIndex -> randomIntBetween(10, 20));
        postData(jobId, data.stream().collect(Collectors.joining()));
    }
}
