package org.apache.helix.task;

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


import java.util.HashMap;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.WorkflowControllerDataProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom rebalancer implementation for the {@code Workflow} in task state model.
 */
public class WorkflowRebalancer extends TaskRebalancer {
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowRebalancer.class);
  private WorkflowDispatcher _workflowDispatcher = new WorkflowDispatcher();

  @Override
  public ResourceAssignment computeBestPossiblePartitionState(
      WorkflowControllerDataProvider clusterData,
      IdealState taskIs, Resource resource, CurrentStateOutput currStateOutput) {
    final String workflow = resource.getResourceName();
    long startTime = System.currentTimeMillis();
    LOG.debug("Computer Best Partition for workflow: " + workflow);
    _workflowDispatcher.init(_manager);
    WorkflowContext workflowCtx = _workflowDispatcher
        .getOrInitializeWorkflowContext(workflow, clusterData.getTaskDataCache());
    WorkflowConfig workflowCfg = clusterData.getWorkflowConfig(workflow);

    _workflowDispatcher.setClusterStatusMonitor(_clusterStatusMonitor);
    _workflowDispatcher.updateCache(clusterData);
    _workflowDispatcher.updateWorkflowStatus(workflow, workflowCfg, workflowCtx, currStateOutput,
        new BestPossibleStateOutput());
    _workflowDispatcher.assignWorkflow(workflow, workflowCfg, workflowCtx, currStateOutput,
        new BestPossibleStateOutput(), new HashMap<String, Resource>());

    LOG.debug(String.format("WorkflowRebalancer computation takes %d ms for workflow %s",
        System.currentTimeMillis() - startTime, workflow));
    return buildEmptyAssignment(workflow, currStateOutput);
  }



  @Override
  public IdealState computeNewIdealState(String resourceName, IdealState currentIdealState,
      CurrentStateOutput currentStateOutput, WorkflowControllerDataProvider clusterData) {
    // Nothing to do here with workflow resource.
    return currentIdealState;
  }
}
