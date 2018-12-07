/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.analytics.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.analytics.DataFrameAnalysis;
import org.elasticsearch.xpack.ml.analytics.DataFrameDataExtractor;
import org.elasticsearch.xpack.ml.analytics.DataFrameDataExtractorFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

public class AnalyticsProcessManager {

    private static final Logger LOGGER = LogManager.getLogger(AnalyticsProcessManager.class);

    private final Client client;
    private final Environment environment;
    private final ThreadPool threadPool;
    private final AnalyticsProcessFactory processFactory;

    public AnalyticsProcessManager(Client client, Environment environment, ThreadPool threadPool,
                                   AnalyticsProcessFactory analyticsProcessFactory) {
        this.client = Objects.requireNonNull(client);
        this.environment = Objects.requireNonNull(environment);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.processFactory = Objects.requireNonNull(analyticsProcessFactory);
    }

    public void runJob(String jobId, DataFrameDataExtractorFactory dataExtractorFactory) {
        threadPool.generic().execute(() -> {
            DataFrameDataExtractor dataExtractor = dataExtractorFactory.newExtractor(false);
            AnalyticsProcess process = createProcess(jobId, createProcessConfig(dataExtractor));
            ExecutorService executorService = threadPool.executor(MachineLearning.AUTODETECT_THREAD_POOL_NAME);
            AnalyticsResultProcessor resultProcessor = new AnalyticsResultProcessor(client, dataExtractorFactory.newExtractor(true));
            executorService.execute(() -> resultProcessor.process(process));
            executorService.execute(() -> processData(jobId, dataExtractor, process, resultProcessor));
        });
    }

    private void processData(String jobId, DataFrameDataExtractor dataExtractor, AnalyticsProcess process,
                             AnalyticsResultProcessor resultProcessor) {
        try {
            writeHeaderRecord(dataExtractor, process);
            writeDataRows(dataExtractor, process);
            process.writeEndOfDataMessage();
            process.flushStream();

            LOGGER.info("[{}] Waiting for result processor to complete", jobId);
            resultProcessor.awaitForCompletion();
            LOGGER.info("[{}] Result processor has completed", jobId);
        } catch (IOException e) {
            LOGGER.error(new ParameterizedMessage("[{}] Error writing data to the process", jobId), e);
        } finally {
            LOGGER.info("[{}] Closing process", jobId);
            try {
                process.close();
                LOGGER.info("[{}] Closed process", jobId);
            } catch (IOException e) {
                LOGGER.error("[{}] Error closing data frame analyzer process", jobId);
            }
        }
    }

    private void writeDataRows(DataFrameDataExtractor dataExtractor, AnalyticsProcess process) throws IOException {
        // The extra field is the control field (should be an empty string)
        String[] record = new String[dataExtractor.getFieldNames().size() + 1];
        // The value of the control field should be an empty string for data frame rows
        record[record.length - 1] = "";

        while (dataExtractor.hasNext()) {
            Optional<List<DataFrameDataExtractor.Row>> rows = dataExtractor.next();
            if (rows.isPresent()) {
                for (DataFrameDataExtractor.Row row : rows.get()) {
                    if (row.shouldSkip() == false) {
                        String[] rowValues = row.getValues();
                        System.arraycopy(rowValues, 0, record, 0, rowValues.length);
                        process.writeRecord(record);
                    }
                }
            }
        }
    }

    private void writeHeaderRecord(DataFrameDataExtractor dataExtractor, AnalyticsProcess process) throws IOException {
        List<String> fieldNames = dataExtractor.getFieldNames();
        String[] headerRecord = new String[fieldNames.size() + 1];
        for (int i = 0; i < fieldNames.size(); i++) {
            headerRecord[i] = fieldNames.get(i);
        }
        // The field name of the control field is dot
        headerRecord[headerRecord.length - 1] = ".";
        process.writeRecord(headerRecord);
    }

    private AnalyticsProcess createProcess(String jobId, AnalyticsProcessConfig analyticsProcessConfig) {
        // TODO We should rename the thread pool to reflect its more general use now, e.g. JOB_PROCESS_THREAD_POOL_NAME
        ExecutorService executorService = threadPool.executor(MachineLearning.AUTODETECT_THREAD_POOL_NAME);
        AnalyticsProcess process = processFactory.createAnalyticsProcess(jobId, analyticsProcessConfig, executorService);
        if (process.isProcessAlive() == false) {
            throw ExceptionsHelper.serverError("Failed to start analytics process");
        }
        return process;
    }

    private AnalyticsProcessConfig createProcessConfig(DataFrameDataExtractor dataExtractor) {
        DataFrameDataExtractor.DataSummary dataSummary = dataExtractor.collectDataSummary();
        AnalyticsProcessConfig config = new AnalyticsProcessConfig(dataSummary.rows, dataSummary.cols,
                new ByteSizeValue(1, ByteSizeUnit.GB), 1, new DataFrameAnalysis("outliers"));
        return config;
    }
}
