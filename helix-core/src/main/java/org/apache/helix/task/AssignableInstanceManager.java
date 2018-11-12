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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.common.caches.TaskDataCache;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.task.assigner.AssignableInstance;
import org.apache.helix.task.assigner.TaskAssignResult;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssignableInstanceManager {

  private static final Logger LOG = LoggerFactory.getLogger(AssignableInstanceManager.class);
  public static final int QUOTA_TYPE_NOT_EXIST = -1;
  // Instance name -> AssignableInstance
  private Map<String, AssignableInstance> _assignableInstanceMap;
  // TaskID -> TaskAssignResult TODO: Hunter: Move this if not needed
  private Map<String, TaskAssignResult> _taskAssignResultMap;

  // With one dimentional quota, and every task only needs 1 quota, this map will save some work
  // in case quota is full. But if there is multi-dimential quota, such aggregated quota map may not
  // help in most of the cases, as this global view does not mean that a single instance has all
  // these quota available.
  // This map is quota type -> remaining global quota
  private Map<String, Integer> _globalThreadBasedQuotaMap;

  /**
   * Basic constructor for AssignableInstanceManager to allow an empty instantiation.
   * buildAssignableInstances() must be explicitly called after instantiation.
   */
  public AssignableInstanceManager() {
    _assignableInstanceMap = new ConcurrentHashMap<>();
    _taskAssignResultMap = new ConcurrentHashMap<>();
    _globalThreadBasedQuotaMap = new ConcurrentHashMap<>();
  }

  /**
   * Builds AssignableInstances and restores TaskAssignResults from scratch by reading from
   * TaskDataCache. It re-computes current quota profile for each AssignableInstance.
   * @param clusterConfig
   * @param taskDataCache
   * @param liveInstances
   * @param instanceConfigs
   */
  public void buildAssignableInstances(ClusterConfig clusterConfig, TaskDataCache taskDataCache,
      Map<String, LiveInstance> liveInstances, Map<String, InstanceConfig> instanceConfigs) {
    // Reset all cached information
    _assignableInstanceMap.clear();
    _taskAssignResultMap.clear();

    // Create all AssignableInstance objects based on what's in liveInstances
    for (Map.Entry<String, LiveInstance> liveInstanceEntry : liveInstances.entrySet()) {
      // Prepare instance-specific metadata
      String instanceName = liveInstanceEntry.getKey();
      LiveInstance liveInstance = liveInstanceEntry.getValue();
      if (!instanceConfigs.containsKey(instanceName)) {
        continue; // Ill-formatted input; skip over this instance
      }
      InstanceConfig instanceConfig = instanceConfigs.get(instanceName);

      // Create an AssignableInstance
      AssignableInstance assignableInstance =
          new AssignableInstance(clusterConfig, instanceConfig, liveInstance);
      _assignableInstanceMap.put(instanceConfig.getInstanceName(), assignableInstance);
      LOG.debug("AssignableInstance created for instance: {}", instanceName);
    }

    // Update task profiles by traversing all TaskContexts
    Map<String, JobConfig> jobConfigMap = taskDataCache.getJobConfigMap();
    for (String jobName : jobConfigMap.keySet()) {
      JobConfig jobConfig = jobConfigMap.get(jobName);
      JobContext jobContext = taskDataCache.getJobContext(jobName);
      if (jobConfig == null || jobContext == null) {
        LOG.debug(
            "JobConfig or JobContext for this job is null. Skipping this job! Job name: {}, JobConfig: {}, JobContext: {}",
            jobName, jobConfig, jobContext);
        continue; // Ignore this job if either the config or context is null
      }

      // First, check that the workflow and job are in valid states. This is important because
      // sometimes aborted jobs do not get a proper update of their task states, meaning there could
      // be INIT and RUNNING tasks we want to ignore
      String workflowName = jobConfig.getWorkflow();
      WorkflowConfig workflowConfig = taskDataCache.getWorkflowConfig(workflowName);
      WorkflowContext workflowContext = taskDataCache.getWorkflowContext(workflowName);
      if (workflowConfig == null || workflowContext == null) {
        // There is no workflow config or context - meaning no tasks are currently scheduled and
        // invalid, so skip this job
        continue;
      }
      TaskState workflowState = workflowContext.getWorkflowState();
      TaskState jobState = workflowContext.getJobState(jobName);
      if (isResourceTerminalOrStopped(workflowState) || isResourceTerminalOrStopped(jobState)) {
        continue;
      }

      String quotaType = jobConfig.getJobType();
      if (quotaType == null) {
        quotaType = AssignableInstance.DEFAULT_QUOTA_TYPE;
      }
      Set<Integer> taskIndices = jobContext.getPartitionSet(); // Each integer represents a task in
      // this job (this is NOT taskId)
      for (int taskIndex : taskIndices) {
        TaskPartitionState taskState = jobContext.getPartitionState(taskIndex);
        if (taskState == TaskPartitionState.INIT || taskState == TaskPartitionState.RUNNING) {
          // Because task state is INIT or RUNNING, find the right AssignableInstance and subtract
          // the right amount of resources. STOPPED means it's been cancelled, so it will be
          // re-assigned and therefore does not use instances' resources

          String assignedInstance = jobContext.getAssignedParticipant(taskIndex);
          String taskId = jobContext.getTaskIdForPartition(taskIndex);
          if (taskId == null) {
            // For targeted tasks, taskId will be null
            // We instead use pName (see FixedTargetTaskAssignmentCalculator)
            taskId = String.format("%s_%s", jobConfig.getJobId(), taskIndex);
          }
          if (assignedInstance == null) {
            LOG.debug(
                "This task's TaskContext does not have an assigned instance! Task will be ignored. "
                    + "Job: {}, TaskId: {}, TaskIndex: {}",
                jobContext.getName(), taskId, taskIndex);
            continue;
          }
          if (_assignableInstanceMap.containsKey(assignedInstance)) {
            TaskConfig taskConfig = jobConfig.getTaskConfig(taskId);
            AssignableInstance assignableInstance = _assignableInstanceMap.get(assignedInstance);
            TaskAssignResult taskAssignResult =
                assignableInstance.restoreTaskAssignResult(taskId, taskConfig, quotaType);
            if (taskAssignResult.isSuccessful()) {
              _taskAssignResultMap.put(taskId, taskAssignResult);
              LOG.debug("TaskAssignResult restored for taskId: {}, assigned on instance: {}",
                  taskId, assignedInstance);
            }
          } else {
            LOG.debug(
                "While building AssignableInstance map, discovered that the instance a task is assigned to is no "
                    + "longer a LiveInstance! TaskAssignResult will not be created and no resource will be taken "
                    + "up for this task. Job: {}, TaskId: {}, TaskIndex: {}, Instance: {}",
                jobContext.getName(), taskId, taskIndex, assignedInstance);
          }
        }
      }
    }
    LOG.info(
        "AssignableInstanceManager built AssignableInstances from scratch based on contexts in TaskDataCache due to Controller switch or ClusterConfig change.");
    computeGlobalThreadBasedCapacity();
  }

  /**
   * Updates AssignableInstances when there are changes in LiveInstances or InstanceConfig. This
   * update only keeps an up-to-date count of AssignableInstances and does NOT re-build tasks
   * (because it's costly).
   * Call this when there is only LiveInstance/InstanceConfig change.
   * @param clusterConfig
   * @param liveInstances
   * @param instanceConfigs
   */
  public void updateAssignableInstances(ClusterConfig clusterConfig,
      Map<String, LiveInstance> liveInstances, Map<String, InstanceConfig> instanceConfigs) {
    // Keep a collection to determine what's no longer a LiveInstance, in which case the
    // corresponding AssignableInstance must be removed
    Collection<AssignableInstance> staleAssignableInstances =
        new HashSet<>(_assignableInstanceMap.values());

    // Loop over new LiveInstances
    for (Map.Entry<String, LiveInstance> liveInstanceEntry : liveInstances.entrySet()) {
      // Prepare instance-specific metadata
      String instanceName = liveInstanceEntry.getKey();
      LiveInstance liveInstance = liveInstanceEntry.getValue();
      if (!instanceConfigs.containsKey(instanceName)) {
        continue; // Ill-formatted input; skip over this instance
      }
      InstanceConfig instanceConfig = instanceConfigs.get(instanceName);

      // Update configs for currently existing instance
      if (_assignableInstanceMap.containsKey(instanceName)) {
        _assignableInstanceMap.get(instanceName).updateConfigs(clusterConfig, instanceConfig,
            liveInstance);
      } else {
        // create a new AssignableInstance for a newly added LiveInstance; this is a new
        // LiveInstance so TaskAssignResults are not re-created and no tasks are assigned
        AssignableInstance assignableInstance =
            new AssignableInstance(clusterConfig, instanceConfig, liveInstance);
        _assignableInstanceMap.put(instanceName, assignableInstance);
        LOG.debug("AssignableInstance created for instance: {} during updateAssignableInstances",
            instanceName);
      }
      // Remove because we've confirmed that this AssignableInstance is a LiveInstance as well
      staleAssignableInstances.remove(_assignableInstanceMap.get(instanceName));
    }

    // AssignableInstances that are not live need to be removed from the map because they are not
    // live
    for (AssignableInstance instanceToBeRemoved : staleAssignableInstances) {
      // Remove all tasks on this instance first
      for (String taskToRemove : instanceToBeRemoved.getCurrentAssignments()) {
        // Check that AssignableInstances match
        if (_taskAssignResultMap.containsKey(taskToRemove)) {
          if (_taskAssignResultMap.get(taskToRemove).getAssignableInstance().getInstanceName()
              .equals(instanceToBeRemoved.getInstanceName())) {
            _taskAssignResultMap.remove(taskToRemove); // TODO: Hunter: Move this if necessary
            LOG.debug(
                "TaskAssignResult removed because its assigned instance is no longer live. TaskID: {}, instance: {}",
                taskToRemove, instanceToBeRemoved.getInstanceName());
          }
        }
      }
      _assignableInstanceMap.remove(instanceToBeRemoved.getInstanceName());
      LOG.debug(
          "Non-live AssignableInstance removed for instance: {} during updateAssignableInstances",
          instanceToBeRemoved.getInstanceName());
    }
    LOG.info(
        "AssignableInstanceManager updated AssignableInstances due to LiveInstance/InstanceConfig change.");

    computeGlobalThreadBasedCapacity();
  }

  /**
   * Returns all instanceName -> AssignableInstance mappings.
   * @return assignableInstanceMap
   */
  public Map<String, AssignableInstance> getAssignableInstanceMap() {
    return Collections.unmodifiableMap(_assignableInstanceMap);
  }

  /**
   * Returns all AssignableInstances that support a given quota type.
   * @param quotaType
   * @return unmodifiable set of AssignableInstances
   */
  public Set<AssignableInstance> getAssignableInstancesForQuotaType(String quotaType) {
    // TODO: Currently, quota types are global settings across all AssignableInstances. When this
    // TODO: becomes customizable, we need to actually implement this so that it doesn't return all
    // TODO: AssignableInstances
    return Collections.unmodifiableSet(new HashSet<>(_assignableInstanceMap.values()));
  }

  /**
   * Returns taskId -> TaskAssignResult mappings.
   * @return taskAssignResultMap
   */
  public Map<String, TaskAssignResult> getTaskAssignResultMap() {
    return _taskAssignResultMap;
  }

  /**
   * Check remained global quota of certain quota type for skipping redundant computation
   * @param quotaType
   * @return
   */
  public boolean hasGlobalCapacity(String quotaType) {
    return _globalThreadBasedQuotaMap.containsKey(quotaType)
        && _globalThreadBasedQuotaMap.get(quotaType) > 0;
  }

  /**
   * Check whether quota maps contains the quota type or not
   * @param quotaType
   * @return
   */
  public boolean hasQuotaType(String quotaType) {
    return _globalThreadBasedQuotaMap.containsKey(quotaType);
  }

  /**
   * Wrapper for AssignableInstance release
   * @param instanceName
   * @param taskConfig
   * @param quotaType
   */
  public void release(String instanceName, TaskConfig taskConfig, String quotaType) {
    if (quotaType == null) {
      LOG.debug("Task {}'s quotaType is null. Trying to release as DEFAULT type.",
          taskConfig.getId());
      quotaType = AssignableInstance.DEFAULT_QUOTA_TYPE;
    }
    if (_assignableInstanceMap.containsKey(instanceName)) {
      _assignableInstanceMap.get(instanceName).release(taskConfig, quotaType);
    }

    if (_globalThreadBasedQuotaMap.containsKey(quotaType)) {
      _globalThreadBasedQuotaMap.put(quotaType, _globalThreadBasedQuotaMap.get(quotaType) + 1);
    }
  }

  /**
   * Wrapper for AssignableInstance tryAssign
   * @param instanceName
   * @param task
   * @param quotaType
   * @return
   * @throws IllegalArgumentException
   */
  public TaskAssignResult tryAssign(String instanceName, TaskConfig task, String quotaType)
      throws IllegalArgumentException {
    if (_assignableInstanceMap.containsKey(instanceName)) {
      return _assignableInstanceMap.get(instanceName).tryAssign(task, quotaType);
    }
    return null;
  }

  /**
   * Wrapper for AssignableInstance assign
   * @param instanceName
   * @param result
   * @throws IllegalStateException
   */
  public void assign(String instanceName, TaskAssignResult result) throws IllegalStateException {
    if (result != null && _assignableInstanceMap.containsKey(instanceName)) {
      _assignableInstanceMap.get(instanceName).assign(result);
    }

    if (_globalThreadBasedQuotaMap.containsKey(result.getQuotaType())) {
      _globalThreadBasedQuotaMap
          .put(result.getQuotaType(), _globalThreadBasedQuotaMap.get(result.getQuotaType()) - 1);
    }
  }

  /**
   * Get all the AssignableInstance names
   * @return
   */
  public Set<String> getAssignableInstanceNames() {
    return Collections.unmodifiableSet(_assignableInstanceMap.keySet());
  }

  /**
   * Determines whether it's possible for a given workflow or a job to have any running tasks. In
   * other words, rule out all resources that are in terminal states or have been stopped.
   * @param state
   * @return
   */
  private boolean isResourceTerminalOrStopped(TaskState state) {
    if (state == null) {
      // If the state is null, it cannot have currently-running tasks either, so consider it
      // inactive
      return true;
    }
    switch (state) {
    case ABORTED:
    case FAILED:
    case STOPPED:
    case COMPLETED:
    case TIMED_OUT:
    case NOT_STARTED:
      return true;
    }
    return false;
  }

  /*
   * Creates a JSON-style String that shows the quota profile and logs it.
   * TODO: Make this with an associated event ID if this becomes a performance bottleneck
   * @param onlyDisplayIfFull if true, this String will only contain the profile for instances whose
   * quota capacity is at its full to avoid cluttering up the log
   */
  public void logQuotaProfileJSON(boolean onlyDisplayIfFull) {
    // Create a String to use as the log for quota status
    ObjectMapper mapper = new ObjectMapper();
    JsonNode instanceNode = mapper.createObjectNode();

    // Loop through all instances
    for (Map.Entry<String, AssignableInstance> instanceEntry : _assignableInstanceMap.entrySet()) {
      AssignableInstance assignableInstance = instanceEntry.getValue();
      boolean capacityFull = false;
      JsonNode resourceTypeNode = mapper.createObjectNode();
      for (Map.Entry<String, Map<String, Integer>> capacityEntry : assignableInstance
          .getTotalCapacity().entrySet()) {
        String resourceType = capacityEntry.getKey();
        Map<String, Integer> quotaTypeMap = capacityEntry.getValue();
        JsonNode quotaTypeNode = mapper.createObjectNode();
        for (Map.Entry<String, Integer> typeEntry : quotaTypeMap.entrySet()) {
          String quotaType = typeEntry.getKey();
          int totalCapacity = typeEntry.getValue();
          int usedCapacity = assignableInstance.getUsedCapacity().get(resourceType).get(quotaType);
          if (!capacityFull) {
            capacityFull = totalCapacity <= usedCapacity;
          }
          String capacityString = String.format("%d/%d", usedCapacity, totalCapacity);
          ((ObjectNode) quotaTypeNode).put(quotaType, capacityString);
        }
        ((ObjectNode) resourceTypeNode).put(resourceType, quotaTypeNode);
      }
      // If onlyDisplayIfFull, do not add the JsonNode to the parent node
      if (onlyDisplayIfFull && !capacityFull) {
        continue;
      }
      ((ObjectNode) instanceNode).put(instanceEntry.getKey(), resourceTypeNode);
    }
    if (instanceNode.size() > 0) {
      LOG.info("Current quota capacity: {}", instanceNode.toString());
    }
  }

  private void computeGlobalThreadBasedCapacity() {
    _globalThreadBasedQuotaMap.clear();
    for (AssignableInstance assignableInstance : _assignableInstanceMap.values()) {
      Map<String, Map<String, Integer>> capacityMap = assignableInstance.getTotalCapacity();
      for (Map.Entry<String, Integer> entry : capacityMap
          .get(LiveInstance.InstanceResourceType.TASK_EXEC_THREAD.name()).entrySet()) {
        int value = entry.getValue();
        if (_globalThreadBasedQuotaMap.containsKey(entry.getKey())) {
          value += _globalThreadBasedQuotaMap.get(entry.getKey());
        }
        _globalThreadBasedQuotaMap.put(entry.getKey(), value);
      }
    }
  }
}
