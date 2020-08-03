package org.apache.helix.controller.stages;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.HashSet;
import java.util.Set;

import org.apache.helix.HelixManager;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractAsyncBaseStage;
import org.apache.helix.controller.pipeline.AsyncWorkerType;
import org.apache.helix.controller.rebalancer.util.RebalanceScheduler;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.WorkflowConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskGarbageCollectionStage extends AbstractAsyncBaseStage {
  private static Logger LOG = LoggerFactory.getLogger(TaskGarbageCollectionStage.class);
  private static RebalanceScheduler _rebalanceScheduler = new RebalanceScheduler();

  @Override
  public AsyncWorkerType getAsyncWorkerType() {
    return AsyncWorkerType.TaskJobPurgeWorker;
  }

  @Override
  public void execute(ClusterEvent event) {
    WorkflowControllerDataProvider dataProvider =
        event.getAttribute(AttributeName.ControllerDataProvider.name());
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());

    if (dataProvider == null || manager == null) {
      LOG.warn(
          "ResourceControllerDataProvider or HelixManager is null for event {}({}) in cluster {}. Skip TaskGarbageCollectionStage.",
          event.getEventId(), event.getEventType(), event.getClusterName());
      return;
    }

    Set<WorkflowConfig> existingWorkflows =
        new HashSet<>(dataProvider.getWorkflowConfigMap().values());
    for (WorkflowConfig workflowConfig : existingWorkflows) {
      // clean up the expired jobs if it is a queue.
      if (workflowConfig != null && (!workflowConfig.isTerminable() || workflowConfig
          .isJobQueue())) {
        try {
          TaskUtil.purgeExpiredJobs(workflowConfig.getWorkflowId(), workflowConfig,
              dataProvider.getWorkflowContext(workflowConfig.getWorkflowId()), manager,
              _rebalanceScheduler);
        } catch (Exception e) {
          LOG.warn(String.format("Failed to purge job for workflow %s with reason %s",
              workflowConfig.getWorkflowId(), e.toString()));
        }
      }
    }

  }
}
