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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.helix.model.LiveInstance;
import org.apache.helix.task.AssignableInstanceManager;
import org.apache.helix.task.TaskConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class ThreadCountBasedTaskAssigner implements TaskAssigner {
  private static final Logger logger = LoggerFactory.getLogger(ThreadCountBasedTaskAssigner.class);
  private static final int SCHED_QUEUE_INIT_CAPACITY = 200;

  private AssignableInstanceManager _assignableInstanceManager;

  /**
   * Assigns given tasks to given AssignableInstances assuming the DEFAULT quota type for all tasks.
   * @param assignableInstances AssignableInstances
   * @param tasks TaskConfigs of the same quota type
   * @return taskID -> TaskAssignmentResult mappings
   */
  public Map<String, TaskAssignResult> assignTasks(Iterable<AssignableInstance> assignableInstances,
      Iterable<TaskConfig> tasks) {
    return assignTasks(assignableInstances, tasks, AssignableInstance.DEFAULT_QUOTA_TYPE);
  }

  /**
   * This is a simple task assigning algorithm that uses the following assumptions to achieve
   * efficiency in assigning tasks:
   * 1. All tasks have same quota type
   * 2. All tasks only need 1 thread for assignment, no other things to consider
   * The algorithm ensures the spread-out of tasks with same quota type or tasks from same job, with
   * best effort.
   * NOTE: once we have more things to consider during scheduling, we will need to come up with
   * a more generic task assignment algorithm.
   * @param assignableInstances AssignableInstances
   * @param tasks TaskConfigs of the same quota type
   * @param quotaType quota type of the tasks
   * @return taskID -> TaskAssignmentResult mappings
   */
  @Override
  public Map<String, TaskAssignResult> assignTasks(Iterable<AssignableInstance> assignableInstances,
      Iterable<TaskConfig> tasks, String quotaType) {
    throw new NotImplementedException();
  }

  @Override
  public Map<String, TaskAssignResult> assignTasks(
      AssignableInstanceManager assignableInstanceManager, Collection<String> instances,
      Iterable<TaskConfig> tasks, String quotaType) {
    Iterable<AssignableInstance> assignableInstances = new HashSet<>();
    // Only add the AssignableInstances that are also in instances
    for (String instance : instances) {
      ((HashSet<AssignableInstance>) assignableInstances)
          .add(assignableInstanceManager.getAssignableInstance(instance));
    }

    if (tasks == null || !tasks.iterator().hasNext()) {
      logger.warn("No task to assign!");
      return Collections.emptyMap();
    }
    if (assignableInstances == null || !assignableInstances.iterator().hasNext()) {
      logger.warn("No instance to assign!");
      return buildNoInstanceAssignment(tasks, quotaType);
    }
    if (quotaType == null || quotaType.equals("") || quotaType.equals("null")) {
      // Sometimes null is stored as a String literal
      logger.warn("Quota type is null. Assigning it as DEFAULT type!");
      quotaType = AssignableInstance.DEFAULT_QUOTA_TYPE;
    }

    logger.info("Assigning tasks with quota type {}", quotaType);

    // Build a sched queue
    PriorityQueue<AssignableInstance> queue = buildSchedQueue(quotaType, assignableInstances);

    // Assign
    Map<String, TaskAssignResult> assignResults = new HashMap<>();
    TaskAssignResult lastFailure = null;
    for (TaskConfig task : tasks) {

      // Dedup
      if (assignResults.containsKey(task.getId())) {
        logger.warn("Duplicated task assignment {}", task);
        continue;
      }

      // TODO: Review this logic
      // TODO: 1. It assumes that the only mode of failure is due to insufficient capacity. This
      // assumption may not always be true. Verify
      // TODO: 2. All TaskAssignResults will get failureReason/Description/TaskID for the first task
      // that failed. This will need correction
      // Every time we try to assign the task to the least-used instance, if that fails,
      // we assume all subsequent tasks will fail with same reason
      if (lastFailure != null) {
        assignResults.put(task.getId(),
            new TaskAssignResult(task, quotaType, null, false, lastFailure.getFitnessScore(),
                lastFailure.getFailureReason(), lastFailure.getFailureDescription()));
        continue;
      }

      // Try to assign the task to least used instance
      AssignableInstance instance = queue.poll();
      TaskAssignResult result = instance.tryAssign(task, quotaType);
      assignResults.put(task.getId(), result);

      if (!result.isSuccessful()) {
        // For all failure reasons other than duplicated assignment, we can fail
        // subsequent tasks
        lastFailure = result;
      } else {
        // If the task is successfully accepted by the instance, assign it to the instance
        assignableInstanceManager.assign(instance.getInstanceName(), result);

        // requeue the instance to rank again
        queue.offer(instance);
      }
    }
    logger.info("Finished assigning tasks with quota type {}", quotaType);
    return assignResults;
  }

  private PriorityQueue<AssignableInstance> buildSchedQueue(String quotaType,
      Iterable<AssignableInstance> instances) {
    AssignableInstanceComparator comparator = new AssignableInstanceComparator(quotaType);
    PriorityQueue<AssignableInstance> queue =
        new PriorityQueue<>(SCHED_QUEUE_INIT_CAPACITY, comparator);
    for (AssignableInstance assignableInstance : instances) {
      queue.offer(assignableInstance);
    }
    return queue;
  }

  private Map<String, TaskAssignResult> buildNoInstanceAssignment(Iterable<TaskConfig> tasks,
      String quotaType) {
    Map<String, TaskAssignResult> result = new HashMap<>();
    for (TaskConfig taskConfig : tasks) {
      result.put(taskConfig.getId(), new TaskAssignResult(taskConfig, quotaType, null, false, 0,
          TaskAssignResult.FailureReason.INSUFFICIENT_QUOTA, "No assignable instance to assign"));
    }
    return result;
  }

  private class AssignableInstanceComparator implements Comparator<AssignableInstance> {

    /**
     * Resource type this comparator needs to compare
     */
    private final String RESOURCE_TYPE = LiveInstance.InstanceResourceType.TASK_EXEC_THREAD.name();

    /**
     * Resource quota type this comparator needs to compare
     */
    private final String _quotaType;

    public AssignableInstanceComparator(String quotaType) {
      _quotaType = quotaType;
    }

    /**
     * Using this comparator, AssignableInstance will be sorted based on availability of
     * quota given job type in the priority queue. Top of the queue will be the one with
     * highest priority
     * @return a negative integer, zero, or a positive integer as the
     *         first argument is less than, equal to, or greater than the
     *         second
     */
    @Override
    public int compare(AssignableInstance o1, AssignableInstance o2) {
      Integer o1RemainingCapacity = getRemainingUsage(o1.getTotalCapacity(), o1.getUsedCapacity());
      Integer o2RemainingCapacity = getRemainingUsage(o2.getTotalCapacity(), o2.getUsedCapacity());
      return o2RemainingCapacity - o1RemainingCapacity;
    }

    private Integer getRemainingUsage(Map<String, Map<String, Integer>> capacity,
        Map<String, Map<String, Integer>> used) {
      if (capacity.containsKey(RESOURCE_TYPE)) {
        String quotaType = AssignableInstance.DEFAULT_QUOTA_TYPE;
        if (capacity.get(RESOURCE_TYPE).containsKey(_quotaType)) {
          // If the quotaType is not supported, sort as DEFAULT because it will be assigned as
          // DEFAULT
          quotaType = _quotaType;
        }
        return capacity.get(RESOURCE_TYPE).get(quotaType) - used.get(RESOURCE_TYPE).get(quotaType);
      }
      return 0;
    }
  }

  public void init(AssignableInstanceManager assignableInstanceManager) {
    _assignableInstanceManager = assignableInstanceManager;
  }
}
