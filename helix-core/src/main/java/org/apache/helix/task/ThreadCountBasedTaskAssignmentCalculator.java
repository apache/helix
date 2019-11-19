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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.task.assigner.TaskAssignResult;
import org.apache.helix.task.assigner.TaskAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThreadCountBasedTaskAssignmentCalculator is an implementation of TaskAssignmentCalculator. It
 * serves as a wrapper around ThreadCountBasedTaskAssigner so that it could be used in the existing
 * Task Framework Controller pipeline (WorkflowRebalancer and JobRebalancer).
 */
public class ThreadCountBasedTaskAssignmentCalculator extends TaskAssignmentCalculator {
  private static final Logger LOG =
      LoggerFactory.getLogger(ThreadCountBasedTaskAssignmentCalculator.class);
  private TaskAssigner _taskAssigner;
  private AssignableInstanceManager _assignableInstanceManager;

  /**
   * Constructor for ThreadCountBasedTaskAssignmentCalculator. Requires an instance of
   * ThreadCountBasedTaskAssigner and the up-to-date AssignableInstanceManager.
   * @param taskAssigner
   * @param assignableInstanceManager
   */
  public ThreadCountBasedTaskAssignmentCalculator(TaskAssigner taskAssigner,
      AssignableInstanceManager assignableInstanceManager) {
    _taskAssigner = taskAssigner;
    _assignableInstanceManager = assignableInstanceManager;
  }

  @Override
  public Set<Integer> getAllTaskPartitions(JobConfig jobCfg, JobContext jobCtx,
      WorkflowConfig workflowCfg, WorkflowContext workflowCtx,
      Map<String, IdealState> idealStateMap) {
    Map<String, TaskConfig> taskMap = jobCfg.getTaskConfigMap();
    Map<String, Integer> taskIdMap = jobCtx.getTaskIdPartitionMap();
    for (TaskConfig taskCfg : taskMap.values()) {
      String taskId = taskCfg.getId();
      int nextPartition = jobCtx.getPartitionSet().size();
      if (!taskIdMap.containsKey(taskId)) {
        jobCtx.setTaskIdForPartition(nextPartition, taskId);
      }
    }
    return jobCtx.getPartitionSet();
  }

  @Override
  public Map<String, SortedSet<Integer>> getTaskAssignment(CurrentStateOutput currStateOutput,
      ResourceAssignment prevAssignment, Collection<String> instances, JobConfig jobCfg,
      JobContext jobContext, WorkflowConfig workflowCfg, WorkflowContext workflowCtx,
      Set<Integer> partitionSet, Map<String, IdealState> idealStateMap) {

    if (jobCfg.getTargetResource() != null) {
      LOG.error(
          "Target resource is not null, should call FixedTaskAssignmentCalculator, target resource : {}",
          jobCfg.getTargetResource());
      return new HashMap<>();
    }

    // Convert the filtered partitionSet (partition numbers) to TaskConfigs
    Iterable<TaskConfig> taskConfigs = getFilteredTaskConfigs(partitionSet, jobCfg, jobContext);

    // Get the quota type to assign tasks to
    String quotaType = getQuotaType(workflowCfg, jobCfg);

    // Assign tasks to AssignableInstances
    Map<String, TaskAssignResult> taskAssignResultMap =
        _taskAssigner.assignTasks(_assignableInstanceManager, instances, taskConfigs, quotaType);

    // TODO: Do this with Quota Manager is ready
    // Cache TaskAssignResultMap to prevent double-assign
    // This will be used in AbstractTaskDispatcher to release tasks that aren't actually
    // scheduled/throttled
    // _assignableInstanceManager.getTaskAssignResultMap().putAll(taskAssignResultMap);

    // Get TaskId->PartitionNumber mappings for conversion
    Map<String, Integer> taskIdPartitionMap = jobContext.getTaskIdPartitionMap();

    // Instantiate the result map that maps instance to set of task (partition) mappings
    Map<String, SortedSet<Integer>> taskAssignment = new HashMap<>();

    // Loop through all TaskAssignResults and convert the result to the format compliant to
    // TaskAssignmentCalculator's API
    for (Map.Entry<String, TaskAssignResult> assignResultEntry : taskAssignResultMap.entrySet()) {
      TaskAssignResult taskAssignResult = assignResultEntry.getValue();

      if (taskAssignResult.isSuccessful()) {
        String instanceName = taskAssignResult.getInstanceName();
        String taskId = taskAssignResult.getTaskConfig().getId();
        // Since return value contains SortedSet<Integer> which is a set of partition numbers, we
        // must convert the taskID (given in TaskAssignResult) to its corresponding partition
        // number using taskIdPartitionMap found in JobContext
        if (!taskIdPartitionMap.containsKey(taskId)) {
          LOG.warn(
              "Task is not found in taskIdPartitionMap. Skipping this task! JobID: {}, TaskID: {}",
              jobCfg.getJobId(), taskId);
          continue;
        }
        int partitionNumberForTask = taskIdPartitionMap.get(taskId);
        if (!taskAssignment.containsKey(instanceName)) {
          taskAssignment.put(instanceName, new TreeSet<Integer>());
        }
        taskAssignment.get(instanceName).add(partitionNumberForTask);
      }
    }
    return taskAssignment;
  }

  /**
   * Returns TaskConfigs whose partition numbers (ids) are present in filteredPartitionNumbers. This
   * means that these tasks should have the state of INIT, RUNNING, or null. This function basically
   * converts partition numbers to corresponding TaskConfigs.
   * @param jobContext
   * @param filteredPartitionNumbers
   * @return
   */
  private Iterable<TaskConfig> getFilteredTaskConfigs(Set<Integer> filteredPartitionNumbers,
      JobConfig jobConfig, JobContext jobContext) {
    Set<TaskConfig> filteredTaskConfigs = new HashSet<>();
    for (int partitionNumber : filteredPartitionNumbers) {
      String taskId = jobContext.getTaskIdForPartition(partitionNumber);
      TaskConfig taskConfig = jobConfig.getTaskConfig(taskId);
      filteredTaskConfigs.add(taskConfig);
    }
    return filteredTaskConfigs;
  }
}
