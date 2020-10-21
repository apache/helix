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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixManager;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractAsyncBaseStage;
import org.apache.helix.controller.pipeline.AsyncWorkerType;
import org.apache.helix.controller.rebalancer.util.RebalanceScheduler;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
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
  public void process(ClusterEvent event) throws Exception {
    // Use main thread to compute what jobs need to be purged, and what workflows need to be gc'ed.
    // This is to avoid race conditions since the cache will be modified. After this work, then the
    // async work will happen.
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    if (manager == null) {
      LOG.warn(
          "HelixManager is null for event {}({}) in cluster {}. Skip TaskGarbageCollectionStage.",
          event.getEventId(), event.getEventType(), event.getClusterName());
      return;
    }

    Map<String, Set<String>> expiredJobsMap = new HashMap<>();
    Set<String> workflowsToBePurged = new HashSet<>();
    WorkflowControllerDataProvider dataProvider =
        event.getAttribute(AttributeName.ControllerDataProvider.name());
    for (Map.Entry<String, ZNRecord> entry : dataProvider.getContexts().entrySet()) {
      WorkflowConfig workflowConfig = dataProvider.getWorkflowConfig(entry.getKey());
      if (workflowConfig != null && (!workflowConfig.isTerminable() || workflowConfig
          .isJobQueue())) {
        WorkflowContext workflowContext = dataProvider.getWorkflowContext(entry.getKey());
        if (workflowContext == null) {
          continue;
        }
        long purgeInterval = workflowConfig.getJobPurgeInterval();
        if (purgeInterval <= 0) {
          continue;
        }
        long currentTime = System.currentTimeMillis();
        long nextPurgeTime = workflowContext.getLastJobPurgeTime() + purgeInterval;
        if (nextPurgeTime <= currentTime) {
          nextPurgeTime = currentTime + purgeInterval;
          // Find jobs that are ready to be purged
          Set<String> expiredJobs = TaskUtil
              .getExpiredJobsFromCache(dataProvider, workflowConfig, workflowContext, manager);
          if (!expiredJobs.isEmpty()) {
            expiredJobsMap.put(workflowConfig.getWorkflowId(), expiredJobs);
          }
        }
        scheduleNextJobPurge(workflowConfig.getWorkflowId(), nextPurgeTime, _rebalanceScheduler,
            manager);
      } else if (workflowConfig == null && entry.getValue() != null && entry.getValue().getId()
          .equals(TaskUtil.WORKFLOW_CONTEXT_KW)) {
        // Find workflows that need to be purged
        workflowsToBePurged.add(entry.getKey());
      }
    }
    event.addAttribute(AttributeName.TO_BE_PURGED_JOBS_MAP.name(),
        Collections.unmodifiableMap(expiredJobsMap));
    event.addAttribute(AttributeName.TO_BE_PURGED_WORKFLOWS.name(),
        Collections.unmodifiableSet(workflowsToBePurged));

    super.process(event);
  }

  @Override
  public void execute(ClusterEvent event) {
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    if (manager == null) {
      LOG.warn(
          "HelixManager is null for event {}({}) in cluster {}. Skip TaskGarbageCollectionStage async execution.",
          event.getEventId(), event.getEventType(), event.getClusterName());
      return;
    }

    Map<String, Set<String>> expiredJobsMap =
        event.getAttribute(AttributeName.TO_BE_PURGED_JOBS_MAP.name());
    Set<String> toBePurgedWorkflows =
        event.getAttribute(AttributeName.TO_BE_PURGED_WORKFLOWS.name());

    for (Map.Entry<String, Set<String>> entry : expiredJobsMap.entrySet()) {
      try {
        TaskUtil.purgeExpiredJobs(entry.getKey(), entry.getValue(), manager, _rebalanceScheduler);
      } catch (Exception e) {
        LOG.warn("Failed to purge job for workflow {}!", entry.getKey(), e);
      }
    }

    TaskUtil.workflowGarbageCollection(toBePurgedWorkflows, manager);
  }

  private static void scheduleNextJobPurge(String workflow, long nextPurgeTime,
      RebalanceScheduler rebalanceScheduler, HelixManager manager) {
    long currentScheduledTime = rebalanceScheduler.getRebalanceTime(workflow);
    if (currentScheduledTime == -1 || currentScheduledTime > nextPurgeTime) {
      rebalanceScheduler.scheduleRebalance(manager, workflow, nextPurgeTime);
    }
  }
}
