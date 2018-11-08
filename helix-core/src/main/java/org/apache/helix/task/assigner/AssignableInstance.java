package org.apache.helix.task.assigner;

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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskStateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AssignableInstance contains instance capacity profile and methods that control capacity and help
 * with task assignment.
 */
public class AssignableInstance {
  private static final Logger logger = LoggerFactory.getLogger(AssignableInstance.class);
  public static final String DEFAULT_QUOTA_TYPE = "DEFAULT";

  /**
   * Fitness score will be calculated from 0 to 1000
   */
  private static final int fitnessScoreFactor = 1000;

  /**
   * Caches IDs of tasks currently assigned to this instance.
   * Every pipeline iteration will compare Task states in this map to Task states in TaskDataCache.
   * Tasks in a terminal state (finished or failed) will be removed as soon as they reach the state.
   */
  private Set<String> _currentAssignments;
  private ClusterConfig _clusterConfig;
  private InstanceConfig _instanceConfig;
  private LiveInstance _liveInstance;

  /**
   * A map recording instance's total capacity:
   * map{resourceType : map{quotaType : quota}}
   */
  private Map<String, Map<String, Integer>> _totalCapacity;

  /**
   * A map recording instance's used capacity
   * map{resourceType : map{quotaType : quota}}
   */
  private Map<String, Map<String, Integer>> _usedCapacity;

  public AssignableInstance(ClusterConfig clusterConfig, InstanceConfig instanceConfig,
      LiveInstance liveInstance) {
    if (clusterConfig == null || instanceConfig == null || liveInstance == null) {
      throw new IllegalArgumentException(String.format(
          "ClusterConfig, InstanceConfig, LiveInstance cannot be null! ClusterConfig null: %s, InstanceConfig null: %s, LiveInstance null: %s",
          clusterConfig == null, instanceConfig == null, liveInstance == null));
    }

    if (!instanceConfig.getInstanceName().equals(liveInstance.getInstanceName())) {
      throw new IllegalArgumentException(
          String.format("Instance name from LiveInstance (%s) and InstanceConfig (%s) don't match!",
              liveInstance.getInstanceName(), instanceConfig.getInstanceName()));
    }
    _clusterConfig = clusterConfig;
    _instanceConfig = instanceConfig;
    _liveInstance = liveInstance;

    _currentAssignments = new HashSet<>();
    _totalCapacity = new HashMap<>();
    _usedCapacity = new HashMap<>();
    refreshTotalCapacity();
  }

  /**
   * When task quota ratio / instance's resource capacity change, we need to update instance
   * capacity cache. Couple of corner cases to clarify for updating capacity:
   * 1. User shrinks capacity and used capacity exceeds total capacity - current assignment
   * will not be affected (used > total is ok) but no further assignment decision will
   * be made on this instance until spaces get freed up
   * 2. User removed a quotaType but there are still tasks with stale quota type assigned on
   * this instance - current assignment will not be affected, and further assignment will
   * NOT be made for stale quota type
   * 3. User removed a resourceType but there are still tasks with stale resource type assigned
   * on this instance - current assignment will not be affected, but no further assignment
   * with stale resource type request will be allowed on this instance
   */
  private void refreshTotalCapacity() {
    // Create a temp total capacity record in case we fail to parse configurations, we
    // still retain existing source of truth
    Map<String, Map<String, Integer>> tempTotalCapacity = new HashMap<>();
    Map<String, String> typeQuotaRatio = _clusterConfig.getTaskQuotaRatioMap();
    Map<String, String> resourceCapacity = _liveInstance.getResourceCapacityMap();

    if (resourceCapacity == null) {
      resourceCapacity = new HashMap<>();
      resourceCapacity.put(LiveInstance.InstanceResourceType.TASK_EXEC_THREAD.name(),
          Integer.toString(TaskStateModelFactory.TASK_THREADPOOL_SIZE));
      logger.debug("No resource capacity provided in LiveInstance {}, assuming default capacity: {}",
          _instanceConfig.getInstanceName(), resourceCapacity);
    }

    if (typeQuotaRatio == null) {
      typeQuotaRatio = new HashMap<>();
      typeQuotaRatio.put(DEFAULT_QUOTA_TYPE, Integer.toString(1));
      logger.debug("No quota type ratio provided in LiveInstance {}, assuming default ratio: {}",
          _instanceConfig.getInstanceName(), typeQuotaRatio);
    }

    logger.debug(
        "Updating capacity for AssignableInstance {}. Resource Capacity: {}; Type Quota Ratio: {}",
        _instanceConfig.getInstanceName(), resourceCapacity, typeQuotaRatio);

    // Reconcile current and new resource types
    try {
      for (final Map.Entry<String, String> resEntry : resourceCapacity.entrySet()) {
        String resourceType = resEntry.getKey();
        int capacity = Integer.valueOf(resEntry.getValue());

        if (!_totalCapacity.containsKey(resourceType)) {
          logger.debug("Adding InstanceResourceType {}", resourceType);
          _usedCapacity.put(resourceType, new HashMap<String, Integer>());
        }
        tempTotalCapacity.put(resourceType, new HashMap<String, Integer>());

        int totalRatio = 0;
        for (String val : typeQuotaRatio.values()) {
          totalRatio += Integer.valueOf(val);
        }

        // Setup per-type resource quota based on given total capacity
        for (Map.Entry<String, String> typeQuotaEntry : typeQuotaRatio.entrySet()) {
          // Calculate total quota for a given type
          String quotaType = typeQuotaEntry.getKey();
          int quotaRatio = Integer.valueOf(typeQuotaEntry.getValue());
          int quota = Math.round(capacity * (float) quotaRatio / (float) totalRatio);

          // Honor non-zero quota ratio for non-zero capacity even if it is rounded to zero
          if (capacity != 0 && quotaRatio != 0 && quota == 0) {
            quota = 1;
          }

          // record total quota of the resource
          tempTotalCapacity.get(resourceType).put(quotaType, quota);

          // Add quota for new quota type
          if (!_usedCapacity.get(resourceType).containsKey(quotaType)) {
            logger.debug("Adding QuotaType {} for resource {}", quotaType, resourceType);
            _usedCapacity.get(resourceType).put(quotaType, 0);
          }
        }

        // For removed quota type, remove record from used capacity
        _usedCapacity.get(resourceType).keySet().retainAll(typeQuotaRatio.keySet());
      }

      // Update total capacity map
      _totalCapacity = tempTotalCapacity;

      // Purge used capacity for resource deleted
      _usedCapacity.keySet().retainAll(resourceCapacity.keySet());

      logger.debug(
          "Finished updating capacity for AssignableInstance {}. Current capacity {}. Current usage: {}",
          _instanceConfig.getInstanceName(), _totalCapacity, _usedCapacity);
    } catch (Exception e) {
      // TODO: properly escalate error
      logger.error(
          "Failed to update capacity for AssignableInstance {}, still using current capacity {}. Current usage: {}",
          _instanceConfig.getInstanceName(), _totalCapacity, _usedCapacity, e);
    }
  }

  /**
   * Update this AssignableInstance with new configs
   * @param clusterConfig cluster config
   * @param instanceConfig instance config
   * @param liveInstance live instance object
   */
  public void updateConfigs(ClusterConfig clusterConfig, InstanceConfig instanceConfig,
      LiveInstance liveInstance) {
    logger.debug("Updating configs for AssignableInstance {}", _instanceConfig.getInstanceName());
    boolean refreshCapacity = false;
    if (clusterConfig != null && clusterConfig.getTaskQuotaRatioMap() != null) {
      if (!clusterConfig.getTaskQuotaRatioMap().equals(_clusterConfig.getTaskQuotaRatioMap())) {
        refreshCapacity = true;
      }
      _clusterConfig = clusterConfig;
      logger.debug("Updated cluster config");
    }

    if (liveInstance != null) {
      if (!_instanceConfig.getInstanceName().equals(liveInstance.getInstanceName())) {
        logger.error(
            "Cannot update live instance with different instance name. Current: {}; new: {}",
            _instanceConfig.getInstanceName(), liveInstance.getInstanceName());
      } else {
        if (liveInstance.getResourceCapacityMap() != null && !liveInstance.getResourceCapacityMap()
            .equals(_liveInstance.getResourceCapacityMap())) {
          refreshCapacity = true;
        }
        _liveInstance = liveInstance;
        logger.debug("Updated live instance");
      }
    }

    if (instanceConfig != null) {
      if (!_instanceConfig.getInstanceName().equals(instanceConfig.getInstanceName())) {
        logger.error(
            "Cannot update instance config with different instance name. Current: {}; new: {}",
            _instanceConfig.getInstanceName(), instanceConfig.getInstanceName());
      } else {
        _instanceConfig = instanceConfig;
        logger.debug("Updated instance config");
      }
    }

    if (refreshCapacity) {
      refreshTotalCapacity();
    }

    logger.debug("Updated configs for AssignableInstance {}", _instanceConfig.getInstanceName());
  }

  /**
   * Tries to assign the given task on this instance and returns TaskAssignResult. Instance capacity
   * profile is NOT modified by tryAssign.
   * When calculating fitness of an assignment, this function will rate assignment from 0 to 1000,
   * and the assignment that has a higher score will be a better fit.
   * @param task task config
   * @param quotaType quota type of the task
   * @return TaskAssignResult
   * @throws IllegalArgumentException if task is null
   */
  public synchronized TaskAssignResult tryAssign(TaskConfig task, String quotaType)
      throws IllegalArgumentException {
    if (task == null) {
      throw new IllegalArgumentException("Task is null!");
    }

    if (_currentAssignments.contains(task.getId())) {
      logger.debug(
          "Task: {} of quotaType: {} is already assigned to this instance. Instance name: {}",
          task.getId(), quotaType, getInstanceName());

      return new TaskAssignResult(task, quotaType, this, false, 0,
          TaskAssignResult.FailureReason.TASK_ALREADY_ASSIGNED,
          String.format("Task %s is already assigned to this instance. Need to release it first",
              task.getId()));
    }

    // For now we only have 1 type of resource so just hard code it here
    String resourceType = LiveInstance.InstanceResourceType.TASK_EXEC_THREAD.name();

    // Fail when no such resource type
    if (!_totalCapacity.containsKey(resourceType)) {

      logger.debug(
          "AssignableInstance does not support the given resourceType: {}. Task: {}, quotaType: {}, Instance name: {}",
          resourceType, task.getId(), quotaType, getInstanceName());

      return new TaskAssignResult(task, quotaType, this, false, 0,
          TaskAssignResult.FailureReason.NO_SUCH_RESOURCE_TYPE,
          String.format("Requested resource type %s not supported. Available resource types: %s",
              resourceType, _totalCapacity.keySet()));
    }

    // If quotaType is null, treat it as DEFAULT
    if (quotaType == null || quotaType.equals("")) {
      quotaType = DEFAULT_QUOTA_TYPE;
    }
    if (!_totalCapacity.get(resourceType).containsKey(quotaType)) {

      logger.debug(
          "AssignableInstance does not support the given quotaType: {}. Task: {}, quotaType: {}, Instance name: {}. Task will be assigned as DEFAULT type.",
          quotaType, task.getId(), quotaType, getInstanceName());
      quotaType = DEFAULT_QUOTA_TYPE;

    }

    int capacity = _totalCapacity.get(resourceType).get(quotaType);
    int usage = _usedCapacity.get(resourceType).get(quotaType);

    // Fail with insufficient quota
    if (capacity <= usage) {

      logger.debug(
          "AssignableInstance does not have enough capacity for quotaType: {}. Task: {}, quotaType: {}, Instance name: {}. Total capacity: {} Current usage: {}",
          quotaType, task.getId(), quotaType, getInstanceName(), capacity, usage);

      return new TaskAssignResult(task, quotaType, this, false, 0,
          TaskAssignResult.FailureReason.INSUFFICIENT_QUOTA,
          String.format("Insufficient quota %s::%s. Capacity: %s, Current Usage: %s", resourceType,
              quotaType, capacity, usage));
    }

    // More remaining capacity leads to higher fitness score
    int fitness = Math.round((float) (capacity - usage) / capacity * fitnessScoreFactor);

    return new TaskAssignResult(task, quotaType, this, true, fitness, null, "");
  }

  /**
   * Performs the following to accept a task:
   * 1. Deduct the amount of resource required by this task
   * 2. Add this TaskAssignResult to _currentAssignments
   * @param result TaskAssignResult
   * @throws IllegalStateException if TaskAssignResult is not successful or the task is double
   *           assigned, or the task is not assigned to this instance
   */
  public synchronized void assign(TaskAssignResult result) throws IllegalStateException {
    if (!result.isSuccessful()) {
      throw new IllegalStateException("Cannot assign a failed result: " + result);
    }

    if (!result.getInstanceName().equals(getInstanceName())) {
      throw new IllegalStateException(String.format(
          "Cannot assign a result for a different instance. This instance: %s; Result: %s",
          getInstanceName(), result));
    }

    if (_currentAssignments.contains(result.getTaskConfig().getId())) {
      throw new IllegalStateException(
          "Cannot double assign task " + result.getTaskConfig().getId());
    }

    _currentAssignments.add(result.getTaskConfig().getId());

    // update resource usage
    // TODO (harry): get requested resource type from task config
    String resourceType = LiveInstance.InstanceResourceType.TASK_EXEC_THREAD.name();
    String quotaType = result.getQuotaType();

    // Resource type / quota type might have already changed, i.e. we are recovering
    // current assignments for a live instance, but currently running tasks's quota
    // type has already been removed by user. So we do the deduction with best effort
    // Note that if the quota type is not found within the resource, task must have been scheduled
    // to DEFAULT type
    // because we schedule undefined types to DEFAULT, so we increment usage to DEFAULT
    if (_usedCapacity.containsKey(resourceType)) {
      // Check that this resourceType contains the given quotaType
      if (_usedCapacity.get(resourceType).containsKey(quotaType)) {
        int curUsage = _usedCapacity.get(resourceType).get(quotaType);
        _usedCapacity.get(resourceType).put(quotaType, curUsage + 1);
      } else {
        // quotaType is not found, treat it as DEFAULT
        int curUsage = _usedCapacity.get(resourceType).get(AssignableInstance.DEFAULT_QUOTA_TYPE);
        _usedCapacity.get(resourceType).put(AssignableInstance.DEFAULT_QUOTA_TYPE, curUsage + 1);
      }
    } else {
      // resourceType is not found. Leave a warning log and will not touch quota
      logger.debug(
          "Task's requested resource type is not supported. TaskConfig: %s; UsedCapacity: %s; ResourceType: %s",
          result.getTaskConfig(), _usedCapacity, resourceType);
    }
    logger.debug("Assigned task {} to instance {}", result.getTaskConfig().getId(),
        _instanceConfig.getInstanceName());
  }

  /**
   * Performs the following to release resource for a task:
   * 1. Release the resource by adding back what the task required.
   * 2. Remove the TaskAssignResult from _currentAssignments
   * Note that if the given quotaType is null, AssignableInstance will try to release from DEFAULT
   * type.
   * @param taskConfig config of this task
   * @param quotaType quota type this task belongs to
   */
  public synchronized void release(TaskConfig taskConfig, String quotaType) {
    if (!_currentAssignments.contains(taskConfig.getId())) {
      logger.debug("Task {} is not assigned on instance {}", taskConfig.getId(),
          _instanceConfig.getInstanceName());
      return;
    }

    String resourceType = LiveInstance.InstanceResourceType.TASK_EXEC_THREAD.name();

    // We might be releasing a task whose resource requirement / quota type is out-dated,
    // thus we need to check to avoid NPE
    if (_usedCapacity.containsKey(resourceType)) {
      if (_usedCapacity.get(resourceType).containsKey(quotaType)) {
        int curUsage = _usedCapacity.get(resourceType).get(quotaType);
        _usedCapacity.get(resourceType).put(quotaType, curUsage - 1);
      } else {
        // This task must have run as DEFAULT type because it was not found in the quota config
        // So make adjustments for DEFAULT
        int curUsage = _usedCapacity.get(resourceType).get(AssignableInstance.DEFAULT_QUOTA_TYPE);
        _usedCapacity.get(resourceType).put(AssignableInstance.DEFAULT_QUOTA_TYPE, curUsage - 1);
      }
    }

    // If the resource type is not found, we just remove from currentAssignments since no adjustment
    // can be made
    _currentAssignments.remove(taskConfig.getId());
    logger.debug("Released task {} from instance {}", taskConfig.getId(),
        _instanceConfig.getInstanceName());
  }

  /**
   * This method restores a TaskAssignResult for a given task in this AssignableInstance when this
   * AssignableInstance is created from scratch due to events like a controller switch. It
   * returns a TaskAssignResult to be used for proper release of resources when the task is in a
   * terminal state.
   * @param taskId of the task
   * @param taskConfig of the task
   * @return TaskAssignResult with isSuccessful = true if successful. If assigning it to an instance
   *         fails, TaskAssignResult's getSuccessful() will return false
   */
  public TaskAssignResult restoreTaskAssignResult(String taskId, TaskConfig taskConfig,
      String quotaType) {
    TaskAssignResult assignResult = new TaskAssignResult(taskConfig, quotaType, this, true,
        fitnessScoreFactor, null, "Recovered TaskAssignResult from current state");
    try {
      assign(assignResult);
    } catch (IllegalStateException e) {
      logger.error("Failed to restore current TaskAssignResult for task {}.", taskId, e);
      return new TaskAssignResult(taskConfig, quotaType, this, false, fitnessScoreFactor, null,
          "Recovered TaskAssignResult from current state");
    }
    return assignResult;
  }

  /**
   * Returns a set of taskIDs
   */
  public Set<String> getCurrentAssignments() {
    return _currentAssignments;
  }

  /**
   * Returns the name of this instance.
   */
  public String getInstanceName() {
    return _instanceConfig.getInstanceName();
  }

  /**
   * Returns total capacity of the AssignableInstance
   * @return map{resourceType : map{quotaType : quota}}
   */
  public Map<String, Map<String, Integer>> getTotalCapacity() {
    return _totalCapacity;
  }

  /**
   * Returns used capacity of the AssignableInstance
   * @return map{resourceType : map{quotaType : usedQuota}}
   */
  public Map<String, Map<String, Integer>> getUsedCapacity() {
    return _usedCapacity;
  }
}
