package org.apache.helix.controller.stages.task;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import com.google.common.collect.Maps;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.task.AssignableInstanceManager;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.task.WorkflowDispatcher;
import org.apache.helix.task.assigner.AssignableInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskSchedulingStage extends AbstractBaseStage {
  private static final Logger logger = LoggerFactory.getLogger(TaskSchedulingStage.class.getName());
  private Map<String, PriorityQueue<WorkflowObject>> _quotaBasedWorkflowPQs = Maps.newHashMap();
  private WorkflowDispatcher _workflowDispatcher;

  @Override
  public void process(ClusterEvent event) throws Exception {
    _eventId = event.getEventId();
    CurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.name());
    final Map<String, Resource> resourceMap =
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());
    WorkflowControllerDataProvider cache = event.getAttribute(AttributeName.ControllerDataProvider.name());

    if (currentStateOutput == null || resourceMap == null || cache == null) {
      throw new StageException(
          "Missing attributes in event:" + event + ". Requires CURRENT_STATE|RESOURCES|DataCache");
    }


    // Build quota capacity based on Current State and Pending Messages
    cache.getAssignableInstanceManager().buildAssignableInstancesFromCurrentState(
        cache.getClusterConfig(), cache.getTaskDataCache(), cache.getLiveInstances(), cache.getInstanceConfigMap(),
        currentStateOutput, resourceMap);

    cache.getAssignableInstanceManager().logQuotaProfileJSON(false);

    // Reset current INIT/RUNNING tasks on participants for throttling
    cache.resetActiveTaskCount(currentStateOutput);

    ClusterStatusMonitor clusterStatusMonitor =
        event.getAttribute(AttributeName.clusterStatusMonitor.name());
    buildQuotaBasedWorkflowPQsAndInitDispatchers(cache,
        (HelixManager) event.getAttribute(AttributeName.helixmanager.name()), clusterStatusMonitor);

    final BestPossibleStateOutput bestPossibleStateOutput =
        compute(event, resourceMap, currentStateOutput);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);

    if (clusterStatusMonitor != null) {
      clusterStatusMonitor.updateAvailableThreadsPerJob(cache.getAssignableInstanceManager()
          .getGlobalCapacityMap());
    }
  }

  private BestPossibleStateOutput compute(ClusterEvent event, Map<String, Resource> resourceMap,
      CurrentStateOutput currentStateOutput) {
    // After compute all workflows and jobs, there are still task resources need to be DROPPED
    Map<String, Resource> restOfResources = new HashMap<>(resourceMap);
    WorkflowControllerDataProvider cache = event.getAttribute(AttributeName.ControllerDataProvider.name());
    BestPossibleStateOutput output = new BestPossibleStateOutput();
    final List<String> failureResources = new ArrayList<>();
    // Queues only for Workflows
    scheduleWorkflows(resourceMap, cache, restOfResources, failureResources, currentStateOutput, output);
    for (String jobName : cache.getTaskDataCache().getDispatchedJobs()) {
      updateResourceMap(jobName, resourceMap, output.getPartitionStateMap(jobName).partitionSet());
      restOfResources.remove(jobName);
    }

    // Jobs that exist in current states but are missing corresponding JobConfigs or WorkflowConfigs
    // or WorkflowContexts need to be cleaned up. Note that restOfResources can only be jobs,
    // because workflow resources are created based on Configs only - workflows don't have
    // CurrentStates
    for (String resourceName : restOfResources.keySet()) {
      _workflowDispatcher.processJobForDrop(resourceName, currentStateOutput, output);
    }

    return output;
  }

  class WorkflowObject implements Comparable<WorkflowObject> {
    String _workflowId;
    Long _rankingValue;

    public WorkflowObject(String workflowId, long rankingValue) {
      _workflowId = workflowId;
      _rankingValue = rankingValue;
    }

    @Override
    public int compareTo(WorkflowObject o) {
      return (int) (_rankingValue - o._rankingValue);
    }
  }

  private void buildQuotaBasedWorkflowPQsAndInitDispatchers(WorkflowControllerDataProvider cache, HelixManager manager,
      ClusterStatusMonitor monitor) {
    _quotaBasedWorkflowPQs.clear();
    Map<String, String> quotaRatioMap = cache.getClusterConfig().getTaskQuotaRatioMap();

    // Create quota based queues
    if (quotaRatioMap == null || quotaRatioMap.size() == 0) {
      _quotaBasedWorkflowPQs
          .put(AssignableInstance.DEFAULT_QUOTA_TYPE, new PriorityQueue<WorkflowObject>());
    } else {
      for (String quotaType : quotaRatioMap.keySet()) {
        _quotaBasedWorkflowPQs.put(quotaType, new PriorityQueue<WorkflowObject>());
      }
    }

    for (String workflowId : cache.getWorkflowConfigMap().keySet()) {
      WorkflowConfig workflowConfig = cache.getWorkflowConfig(workflowId);
      String workflowType = getQuotaType(workflowConfig);
      // TODO: We can support customized sorting field for user. Currently sort by creation time
      _quotaBasedWorkflowPQs.get(workflowType)
          .add(new WorkflowObject(workflowId, workflowConfig.getRecord().getCreationTime()));
    }
    if (_workflowDispatcher == null) {
      _workflowDispatcher = new WorkflowDispatcher();
    }
    _workflowDispatcher.init(manager);
    _workflowDispatcher.setClusterStatusMonitor(monitor);
    _workflowDispatcher.updateCache(cache);
  }

  private void scheduleWorkflows(Map<String, Resource> resourceMap, WorkflowControllerDataProvider cache,
      Map<String, Resource> restOfResources, List<String> failureResources,
      CurrentStateOutput currentStateOutput, BestPossibleStateOutput bestPossibleOutput) {
    AssignableInstanceManager assignableInstanceManager = cache.getAssignableInstanceManager();
    for (PriorityQueue<WorkflowObject> quotaBasedWorkflowPQ : _quotaBasedWorkflowPQs.values()) {
      Iterator<WorkflowObject> it = quotaBasedWorkflowPQ.iterator();
      while (it.hasNext()) {
        String workflowId = it.next()._workflowId;
        Resource resource = resourceMap.get(workflowId);
        // TODO : Resource is null could be workflow just created without any IdealState.
        // Let's remove this check when Helix is independent from IdealState
        if (resource != null) {
          try {
            WorkflowContext context = _workflowDispatcher
                .getOrInitializeWorkflowContext(workflowId, cache.getTaskDataCache());
            _workflowDispatcher
                .updateWorkflowStatus(workflowId, cache.getWorkflowConfig(workflowId), context,
                    currentStateOutput, bestPossibleOutput);
            String quotaType = getQuotaType(cache.getWorkflowConfig(workflowId));
            restOfResources.remove(workflowId);
            if (assignableInstanceManager.hasGlobalCapacity(quotaType)) {
              _workflowDispatcher.assignWorkflow(workflowId, cache.getWorkflowConfig(workflowId),
                  context, currentStateOutput, bestPossibleOutput);
            } else {
              LogUtil.logInfo(logger, _eventId, String.format(
                  "Fail to schedule new jobs assignment for Workflow %s due to quota %s is full",
                  workflowId, quotaType));
            }
          } catch (Exception e) {
            LogUtil.logError(logger, _eventId,
                "Error computing assignment for Workflow " + workflowId + ". Skipping.", e);
            failureResources.add(workflowId);
          }
        }
      }
    }
  }

  private void updateResourceMap(String jobName, Map<String, Resource> resourceMap,
      Set<Partition> partitionSet) {
    Resource resource = new Resource(jobName);
    for (Partition partition : partitionSet) {
      resource.addPartition(partition.getPartitionName());
    }
    resource.setStateModelDefRef(TaskConstants.STATE_MODEL_NAME);
    resource.setStateModelFactoryName(HelixConstants.DEFAULT_STATE_MODEL_FACTORY);
    resourceMap.put(jobName, resource);
  }

  private String getQuotaType(WorkflowConfig workflowConfig) {
    String workflowType = workflowConfig.getWorkflowType();
    if (workflowType == null || !_quotaBasedWorkflowPQs.containsKey(workflowType)) {
      workflowType = AssignableInstance.DEFAULT_QUOTA_TYPE;
    }
    return workflowType;
  }
}
