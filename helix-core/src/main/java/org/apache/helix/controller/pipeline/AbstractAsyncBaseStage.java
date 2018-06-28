package org.apache.helix.controller.pipeline;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.common.DedupEventProcessor;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.ClusterEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractAsyncBaseStage extends AbstractBaseStage {
  private static final Logger logger = LoggerFactory.getLogger(AbstractAsyncBaseStage.class);

  @Override
  public void process(final ClusterEvent event) throws Exception {
    String pipelineType = event.getAttribute(AttributeName.PipelineType.name());
    final String taskType = getAsyncTaskDedupType(pipelineType);
    DedupEventProcessor<String, Runnable> worker =
        getAsyncWorkerFromClusterEvent(event, getAsyncWorkerType());
    if (worker == null) {
      throw new StageException("No async worker found for " + taskType);
    }

    worker.queueEvent(taskType, new Runnable() {
      @Override
      public void run() {
        long startTimestamp = System.currentTimeMillis();
        logger.info("START AsyncProcess: {}", taskType);
        try {
          execute(event);
        } catch (Exception e) {
          logger.error("Failed to process {} asynchronously", taskType, e);
        }
        long endTimestamp = System.currentTimeMillis();
        logger.info("END AsyncProcess: {}, took {} ms", taskType, endTimestamp - startTimestamp);
      }
    });
    logger.info("Submitted asynchronous {} task to worker", taskType);
  }

  /**
   * Stage that implements AbstractAsyncBaseStage should implement this method
   * to get it's worker
   * @return AsyncWorkerType
   */
  public abstract AsyncWorkerType getAsyncWorkerType();

  /**
   * Implements stages main logic
   *
   * @param event ClusterEvent
   * @throws Exception exception
   */
  public abstract void execute(final ClusterEvent event) throws Exception;

  private String getAsyncTaskDedupType(String pipelineType) {
    return String
        .format("%s::%s", pipelineType, getClass().getSimpleName());
  }
}
