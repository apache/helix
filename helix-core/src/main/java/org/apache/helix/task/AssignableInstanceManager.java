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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssignableInstanceManager {

  private static final Logger LOG = LoggerFactory.getLogger(AssignableInstanceManager.class);
  // Instance name -> AssignableInstance
  private Map<String, AssignableInstance> _assignableInstanceMap;
  // TaskID -> TaskAssignResult TODO: Hunter: Move this if not needed
  private Map<String, TaskAssignResult> _taskAssignResultMap;

  /**
   * Basic constructor for AssignableInstanceManager to allow an empty instantiation.
   * buildAssignableInstances() must be explicitly called after instantiation.
   */
  public AssignableInstanceManager() {
    _assignableInstanceMap = new ConcurrentHashMap<>();
    _taskAssignResultMap = new ConcurrentHashMap<>();
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
      LOG.info("AssignableInstance created for instance: {}", instanceName);
    }

    // Update task profiles by traversing all TaskContexts
    Map<String, JobConfig> jobConfigMap = taskDataCache.getJobConfigMap();
    for (String jobName : jobConfigMap.keySet()) {
      JobConfig jobConfig = jobConfigMap.get(jobName);
      JobContext jobContext = taskDataCache.getJobContext(jobName);
      if (jobConfig == null || jobContext == null) {
        LOG.warn(
            "JobConfig or JobContext for this job is null. Skipping this job! Job name: {}, JobConfig: {}, JobContext: {}",
            jobName, jobConfig, jobContext);
        continue; // Ignore this job if either the config or context is null
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
            LOG.warn(
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
              LOG.info("TaskAssignResult restored for taskId: {}, assigned on instance: {}", taskId,
                  assignedInstance);
            }
          } else {
            LOG.warn(
                "While building AssignableInstance map, discovered that the instance a task is assigned to is no "
                    + "longer a LiveInstance! TaskAssignResult will not be created and no resource will be taken "
                    + "up for this task. Job: {}, TaskId: {}, TaskIndex: {}, Instance: {}",
                jobContext.getName(), taskId, taskIndex, assignedInstance);
          }
        }
      }
    }
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
        LOG.info("AssignableInstance created for instance: {} during updateAssignableInstances",
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
            LOG.info(
                "TaskAssignResult removed because its assigned instance is no longer live. TaskID: {}, instance: {}",
                taskToRemove, instanceToBeRemoved.getInstanceName());
          }
        }
      }
      _assignableInstanceMap.remove(instanceToBeRemoved.getInstanceName());
      LOG.info(
          "Non-live AssignableInstance removed for instance: {} during updateAssignableInstances",
          instanceToBeRemoved.getInstanceName());
    }
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
}