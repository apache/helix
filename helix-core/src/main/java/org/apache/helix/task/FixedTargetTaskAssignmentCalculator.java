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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.task.assigner.TaskAssignResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A TaskAssignmentCalculator for when a task group must be assigned according to partitions/states
 * on a target
 * resource. Here, tasks are co-located according to where a resource's partitions are, as well as
 * (if desired) only where those partitions are in a given state.
 */
public class FixedTargetTaskAssignmentCalculator extends TaskAssignmentCalculator {
  private static final Logger LOG =
      LoggerFactory.getLogger(FixedTargetTaskAssignmentCalculator.class);
  private AssignableInstanceManager _assignableInstanceManager;

  /**
   * Default constructor. Because of quota-based scheduling support, we need
   * AssignableInstanceManager. This constructor should not be used.
   */
  @Deprecated
  public FixedTargetTaskAssignmentCalculator() {
  }

  /**
   * Constructor for FixedTargetTaskAssignmentCalculator. Requires AssignableInstanceManager for
   * "charging" resources per task.
   * @param assignableInstanceManager
   */
  public FixedTargetTaskAssignmentCalculator(AssignableInstanceManager assignableInstanceManager) {
    _assignableInstanceManager = assignableInstanceManager;
  }

  @Override
  public Set<Integer> getAllTaskPartitions(JobConfig jobCfg, JobContext jobCtx,
      WorkflowConfig workflowCfg, WorkflowContext workflowCtx,
      Map<String, IdealState> idealStateMap) {
    IdealState tgtIs = idealStateMap.get(jobCfg.getTargetResource());
    return getAllTaskPartitions(tgtIs, jobCfg, jobCtx);
  }

  @Override
  public Map<String, SortedSet<Integer>> getTaskAssignment(CurrentStateOutput currStateOutput,
      ResourceAssignment prevAssignment, Collection<String> instances, JobConfig jobCfg,
      JobContext jobContext, WorkflowConfig workflowCfg, WorkflowContext workflowCtx,
      Set<Integer> partitionSet, Map<String, IdealState> idealStateMap) {
    return computeAssignmentAndChargeResource(currStateOutput, prevAssignment, instances,
        workflowCfg, jobCfg, jobContext, partitionSet, idealStateMap);
  }

  /**
   * Returns the set of all partition ids for a job.
   * If a set of partition ids was explicitly specified in the config, that is used. Otherwise, we
   * use the list of all partition ids from the target resource.
   * return empty set if target resource does not exist.
   */
  private static Set<Integer> getAllTaskPartitions(IdealState tgtResourceIs, JobConfig jobCfg,
      JobContext taskCtx) {
    Map<String, List<Integer>> currentTargets = taskCtx.getPartitionsByTarget();
    SortedSet<String> targetPartitions = Sets.newTreeSet();
    if (jobCfg.getTargetPartitions() != null) {
      targetPartitions.addAll(jobCfg.getTargetPartitions());
    } else {
      if (tgtResourceIs != null) {
        targetPartitions.addAll(tgtResourceIs.getPartitionSet());
      } else {
        LOG.warn("Missing target resource for the scheduled job!");
      }
    }

    Set<Integer> taskPartitions = Sets.newTreeSet();
    for (String targetPartition : targetPartitions) {
      taskPartitions
          .addAll(getPartitionsForTargetPartition(targetPartition, currentTargets, taskCtx));
    }
    return taskPartitions;
  }

  private static List<Integer> getPartitionsForTargetPartition(String targetPartition,
      Map<String, List<Integer>> currentTargets, JobContext jobCtx) {
    if (!currentTargets.containsKey(targetPartition)) {
      int nextId = jobCtx.getPartitionSet().size();
      jobCtx.setPartitionTarget(nextId, targetPartition);
      return Lists.newArrayList(nextId);
    } else {
      return currentTargets.get(targetPartition);
    }
  }

  /**
   * NOTE: this method has been deprecated due to the addition of quota-based task scheduling.
   * Get partition assignments for the target resource, but only for the partitions of interest.
   * @param currStateOutput The current state of the instances in the cluster.
   * @param instances The instances.
   * @param tgtIs The ideal state of the target resource.
   * @param tgtStates Only partitions in this set of states will be considered. If null, partitions
   *          do not need to
   *          be in any specific state to be considered.
   * @param includeSet The set of partitions to consider.
   * @return A map of instance vs set of partition ids assigned to that instance.
   */
  @Deprecated
  private static Map<String, SortedSet<Integer>> getTgtPartitionAssignment(
      CurrentStateOutput currStateOutput, Iterable<String> instances, IdealState tgtIs,
      Set<String> tgtStates, Set<Integer> includeSet, JobContext jobCtx) {
    Map<String, SortedSet<Integer>> result = new HashMap<>();
    for (String instance : instances) {
      result.put(instance, new TreeSet<Integer>());
    }

    Map<String, List<Integer>> partitionsByTarget = jobCtx.getPartitionsByTarget();
    for (String pName : tgtIs.getPartitionSet()) {
      List<Integer> partitions = partitionsByTarget.get(pName);
      if (partitions == null || partitions.size() < 1) {
        continue;
      }
      int pId = partitions.get(0);
      if (includeSet.contains(pId)) {
        for (String instance : instances) {
          Message pendingMessage = currStateOutput.getPendingMessage(tgtIs.getResourceName(),
              new Partition(pName), instance);
          if (pendingMessage != null) {
            continue;
          }

          String s = currStateOutput.getCurrentState(tgtIs.getResourceName(), new Partition(pName),
              instance);
          if (s != null && (tgtStates == null || tgtStates.contains(s))) {
            result.get(instance).add(pId);
          }
        }
      }
    }
    return result;
  }

  /**
   * Calculate the assignment for given tasks. This assignment also charges resources for each task
   * and takes resource/quota availability into account while assigning.
   * @param currStateOutput
   * @param prevAssignment
   * @param liveInstances
   * @param jobCfg
   * @param jobContext
   * @param taskPartitionSet
   * @param idealStateMap
   * @return instance -> set of task partition numbers
   */
  private Map<String, SortedSet<Integer>> computeAssignmentAndChargeResource(
      CurrentStateOutput currStateOutput, ResourceAssignment prevAssignment,
      Collection<String> liveInstances, WorkflowConfig workflowCfg, JobConfig jobCfg,
      JobContext jobContext, Set<Integer> taskPartitionSet, Map<String, IdealState> idealStateMap) {

    // Note: targeted jobs also take up capacity in quota-based scheduling
    // "Charge" resources for the tasks
    String quotaType = getQuotaType(workflowCfg, jobCfg);

    // IdealState of the target resource
    IdealState targetIdealState = idealStateMap.get(jobCfg.getTargetResource());
    if (targetIdealState == null) {
      LOG.warn("Missing target resource for the scheduled job!");
      return Collections.emptyMap();
    }

    // The "states" you want to assign to. Assign to the partitions of the target resource if these
    // partitions are in one of these states
    Set<String> targetStates = jobCfg.getTargetPartitionStates();

    Map<String, SortedSet<Integer>> result = new HashMap<>();
    for (String instance : liveInstances) {
      result.put(instance, new TreeSet<Integer>());
    }

    // <Target resource partition name -> list of task partition numbers> mapping
    Map<String, List<Integer>> partitionsByTarget = jobContext.getPartitionsByTarget();
    for (String targetResourcePartitionName : targetIdealState.getPartitionSet()) {
      // Get all task partition numbers to be assigned to this targetResource partition
      List<Integer> taskPartitions = partitionsByTarget.get(targetResourcePartitionName);
      if (taskPartitions == null || taskPartitions.size() < 1) {
        continue; // No tasks to assign, skip
      }
      // Get one task to be assigned to this targetResource partition
      int targetPartitionId = taskPartitions.get(0);
      // First, see if that task needs to be assigned at this time
      if (taskPartitionSet.contains(targetPartitionId)) {
        for (String instance : liveInstances) {
          // See if there is a pending message on this instance on this partition for the target
          // resource
          // If there is, we should wait until the pending message gets processed, so skip
          // assignment this time around
          Message pendingMessage =
              currStateOutput.getPendingMessage(targetIdealState.getResourceName(),
                  new Partition(targetResourcePartitionName), instance);
          if (pendingMessage != null) {
            continue;
          }

          // See if there a partition exists on this instance
          String currentState = currStateOutput.getCurrentState(targetIdealState.getResourceName(),
              new Partition(targetResourcePartitionName), instance);
          if (currentState != null
              && (targetStates == null || targetStates.contains(currentState))) {

            // Prepare pName and taskConfig for assignment
            String pName = String.format("%s_%s", jobCfg.getJobId(), targetPartitionId);
            if (!jobCfg.getTaskConfigMap().containsKey(pName)) {
              jobCfg.getTaskConfigMap().put(pName,
                  new TaskConfig(null, null, pName, targetResourcePartitionName));
            }
            TaskConfig taskConfig = jobCfg.getTaskConfigMap().get(pName);

            // On LiveInstance change, RUNNING or other non-terminal tasks might get re-assigned. If
            // the new assignment differs from prevAssignment, release. If the assigned instances
            // from old and new assignments are the same, then do nothing and let it keep running
            // The following checks if two assignments (old and new) differ
            Map<String, String> instanceMap = prevAssignment.getReplicaMap(new Partition(pName));
            Iterator<String> itr = instanceMap.keySet().iterator();
            // First, check if this taskPartition has been ever assigned before by checking
            // prevAssignment
            if (itr.hasNext()) {
              String prevInstance = itr.next();
              if (!prevInstance.equals(instance)) {
                // Old and new assignments are different. We need to release from prevInstance, and
                // this task will be assigned to a different instance
                if (_assignableInstanceManager.getAssignableInstanceNames()
                    .contains(prevInstance)) {
                  _assignableInstanceManager.release(prevInstance, taskConfig, quotaType);
                } else {
                  // This instance must be no longer live
                  LOG.warn(
                      "Task {} was reassigned from old instance: {} to new instance: {}. However, old instance: {} is not found in AssignableInstanceMap. The old instance is possibly no longer a LiveInstance. This task will not be released.",
                      pName, prevAssignment, instance);
                }
              } else {
                // Old and new assignments are the same, so just skip assignment for this
                // taskPartition so that it can just keep running
                break;
              }
            }

            // Actual assignment logic: try to charge resources first and assign if successful
            if (_assignableInstanceManager.getAssignableInstanceNames().contains(instance)) {
              // Try to assign first
              TaskAssignResult taskAssignResult =
                  _assignableInstanceManager.tryAssign(instance, taskConfig, quotaType);
              if (taskAssignResult.isSuccessful()) {
                // There exists a partition, the states match up, and tryAssign successful. Assign!
                _assignableInstanceManager.assign(instance, taskAssignResult);
                result.get(instance).add(targetPartitionId);
                // To prevent double assign of the tasks on other replicas of the targetResource
                // partition
                break;
              } else if ((!taskAssignResult.isSuccessful() && taskAssignResult
                  .getFailureReason() == TaskAssignResult.FailureReason.TASK_ALREADY_ASSIGNED)) {
                // In case of double assign, we can still include it in the assignment because
                // RUNNING->RUNNING message will just be ignored by the participant
                // AssignableInstance should already have it assigned, so do not double-charge
                result.get(instance).add(targetPartitionId);
                break;
              } else {
                LOG.warn(
                    "Unable to assign the task to this AssignableInstance. Skipping this instance. Task: {}, Instance: {}, TaskAssignResult: {}",
                    pName, instance, taskAssignResult);
              }
            } else {
              LOG.error(
                  "AssignableInstance does not exist for this LiveInstance: {}. This should never happen! Will not assign to this instance.",
                  instance);
            }
          }
        }
      }
    }
    return result;
  }
}
