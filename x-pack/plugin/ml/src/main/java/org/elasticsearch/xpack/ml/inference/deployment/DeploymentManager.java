/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.inference.deployment.TrainedModelDeploymentState;
import org.elasticsearch.xpack.core.ml.inference.deployment.TrainedModelDeploymentTaskState;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchProcess;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchProcessFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class DeploymentManager {

    private static final Logger logger = LogManager.getLogger(DeploymentManager.class);

    private final Client client;
    private final PyTorchProcessFactory pyTorchProcessFactory;
    private final ExecutorService executorServiceForDeployment;
    private final ExecutorService executorServiceForProcess;
    private final ConcurrentMap<Long, ProcessContext> processContextByAllocation = new ConcurrentHashMap<>();

    public DeploymentManager(Client client, ThreadPool threadPool, PyTorchProcessFactory pyTorchProcessFactory) {
        this.client = Objects.requireNonNull(client);
        this.pyTorchProcessFactory = Objects.requireNonNull(pyTorchProcessFactory);
        this.executorServiceForDeployment = threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME);
        this.executorServiceForProcess = threadPool.executor(MachineLearning.JOB_COMMS_THREAD_POOL_NAME);
    }

    public void startDeployment(TrainedModelDeploymentTask task) {
        executorServiceForDeployment.execute(() -> doStartDeployment(task));
    }

    private void doStartDeployment(TrainedModelDeploymentTask task) {
        logger.info("[{}] Starting model deployment", task.getModelId());

        ProcessContext processContext = new ProcessContext(task.getModelId());

        if (processContextByAllocation.putIfAbsent(task.getAllocationId(), processContext) != null) {
            throw ExceptionsHelper.serverError("[{}] Could not create process as one already exists", task.getModelId());
        }

        processContext.startProcess();

        try {
            processContext.loadModel();
        } catch (IOException e) {
            logger.error(new ParameterizedMessage("[{}] error loading model", task.getModelId()), e);
        }

        TrainedModelDeploymentTaskState startedState = new TrainedModelDeploymentTaskState(
            TrainedModelDeploymentState.STARTED, task.getAllocationId(), null);
        task.updatePersistentTaskState(startedState, ActionListener.wrap(
            response -> logger.info("[{}] trained model deployment started", task.getModelId()),
            task::markAsFailed
        ));
    }

    public void stopDeployment(TrainedModelDeploymentTask task) {
        ProcessContext processContext;
        synchronized (processContextByAllocation) {
            processContext = processContextByAllocation.get(task.getAllocationId());
        }
        if (processContext != null) {
            logger.debug("[{}] Stopping deployment", task.getModelId());
            processContext.killProcess();
        } else {
            logger.debug("[{}] No process context to stop", task.getModelId());
        }
    }

    class ProcessContext {

        private final String modelId;
        private final SetOnce<PyTorchProcess> process = new SetOnce<>();

        ProcessContext(String modelId) {
            this.modelId = Objects.requireNonNull(modelId);
        }

        synchronized void startProcess() {
            process.set(pyTorchProcessFactory.createProcess(modelId, executorServiceForProcess, onProcessCrash()));
        }

        synchronized void killProcess() {
            if (process.get() == null) {
                return;
            }
            try {
                process.get().kill(true);
            } catch (IOException e) {
                logger.error(new ParameterizedMessage("[{}] Failed to kill process", modelId), e);
            }
        }

        private Consumer<String> onProcessCrash() {
            return reason -> {
                logger.error("[{}] process crashed due to reason [{}]", modelId, reason);
            };
        }

        void loadModel() throws IOException {
            // Here we should be reading the model location from the deployment config.
            // Hardcoding this for the prototype.
            String index = "test-models";
            String docId = "simple-model";

            GetResponse modelGetResponse = client.get(new GetRequest(index, docId)).actionGet();
            if (modelGetResponse.isExists() == false) {
                throw ExceptionsHelper.badRequestException("[{}] no model was found", modelId);
            }
            Map<String, Object> sourceAsMap = modelGetResponse.getSourceAsMap();
            int modelSizeAfterUnbase64 = (int) sourceAsMap.get("size");
            String modelBase64 = (String) sourceAsMap.get("model");
            process.get().loadModel(modelBase64, modelSizeAfterUnbase64);
        }
    }

}
