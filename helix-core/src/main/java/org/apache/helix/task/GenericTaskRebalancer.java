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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.helix.ZNRecord;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Resource;
import org.apache.helix.api.State;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.stages.ResourceCurrentState;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceAssignment;

import com.google.common.base.Function;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * This class does an assignment based on an automatic rebalancing strategy, rather than requiring
 * assignment to target partitions and states of another resource
 */
public class GenericTaskRebalancer extends TaskRebalancer {
  /** Reassignment policy for this algorithm */
  private RetryPolicy _retryPolicy = new DefaultRetryReassigner();

  @Override
  public Set<Integer> getAllTaskPartitions(JobConfig jobCfg, JobContext jobCtx,
      WorkflowConfig workflowCfg, WorkflowContext workflowCtx, Cluster cache) {
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
  public Map<ParticipantId, SortedSet<Integer>> getTaskAssignment(
      ResourceCurrentState currStateOutput, ResourceAssignment prevAssignment,
      Collection<ParticipantId> instances, JobConfig jobCfg, final JobContext jobContext,
      WorkflowConfig workflowCfg, WorkflowContext workflowCtx, Set<Integer> partitionSet,
      Cluster cache) {
    // Gather input to the full auto rebalancing algorithm
    LinkedHashMap<State, Integer> states = new LinkedHashMap<State, Integer>();
    states.put(State.from("ONLINE"), 1);

    // Only map partitions whose assignment we care about
    final Set<TaskPartitionState> honoredStates =
        Sets.newHashSet(TaskPartitionState.INIT, TaskPartitionState.RUNNING,
            TaskPartitionState.STOPPED);
    Set<Integer> filteredPartitionSet = Sets.newHashSet();
    for (Integer p : partitionSet) {
      TaskPartitionState state = (jobContext == null) ? null : jobContext.getPartitionState(p);
      if (state == null || honoredStates.contains(state)) {
        filteredPartitionSet.add(p);
      }
    }

    // Transform from partition id to fully qualified partition name
    List<Integer> partitionNums = Lists.newArrayList(partitionSet);
    Collections.sort(partitionNums);
    final ResourceId resourceId = prevAssignment.getResourceId();
    List<PartitionId> partitions =
        new ArrayList<PartitionId>(Lists.transform(partitionNums,
            new Function<Integer, PartitionId>() {
              @Override
              public PartitionId apply(Integer partitionNum) {
                return PartitionId.from(resourceId + "_" + partitionNum);
              }
            }));

    // Compute the current assignment
    Map<PartitionId, Map<ParticipantId, State>> currentMapping = Maps.newHashMap();
    for (PartitionId partition : currStateOutput.getCurrentStateMappedPartitions(resourceId)) {
      if (!filteredPartitionSet.contains(pId(partition.toString()))) {
        // not computing old partitions
        continue;
      }
      Map<ParticipantId, State> allPreviousDecisionMap = Maps.newHashMap();
      if (prevAssignment != null) {
        allPreviousDecisionMap.putAll(prevAssignment.getReplicaMap(partition));
      }
      allPreviousDecisionMap.putAll(currStateOutput.getCurrentStateMap(resourceId, partition));
      allPreviousDecisionMap.putAll(currStateOutput.getPendingStateMap(resourceId, partition));
      currentMapping.put(partition, allPreviousDecisionMap);
    }

    // Get the assignment keyed on partition
    AutoRebalanceStrategy strategy =
        new AutoRebalanceStrategy(resourceId, partitions, states, Integer.MAX_VALUE,
            new AutoRebalanceStrategy.DefaultPlacementScheme());
    List<ParticipantId> allNodes =
        Lists.newArrayList(getEligibleInstances(jobCfg, currStateOutput, instances, cache));
    Collections.sort(allNodes);
    ZNRecord record = strategy.typedComputePartitionAssignment(allNodes, currentMapping, allNodes);
    Map<String, List<String>> preferenceLists = record.getListFields();

    // Convert to an assignment keyed on participant
    Map<ParticipantId, SortedSet<Integer>> taskAssignment = Maps.newHashMap();
    for (Map.Entry<String, List<String>> e : preferenceLists.entrySet()) {
      String partitionName = e.getKey();
      partitionName = String.valueOf(pId(partitionName));
      List<String> preferenceList = e.getValue();
      for (String participantName : preferenceList) {
        ParticipantId participantId = ParticipantId.from(participantName);
        if (!taskAssignment.containsKey(participantId)) {
          taskAssignment.put(participantId, new TreeSet<Integer>());
        }
        taskAssignment.get(participantId).add(Integer.valueOf(partitionName));
      }
    }

    // Finally, adjust the assignment if tasks have been failing
    taskAssignment = _retryPolicy.reassign(jobCfg, jobContext, allNodes, taskAssignment);
    return taskAssignment;
  }

  /**
   * Filter a list of instances based on targeted resource policies
   * @param jobCfg the job configuration
   * @param currStateOutput the current state of all instances in the cluster
   * @param instances valid instances
   * @param cache current snapshot of the cluster
   * @return a set of instances that can be assigned to
   */
  private Set<ParticipantId> getEligibleInstances(JobConfig jobCfg,
      ResourceCurrentState currStateOutput, Iterable<ParticipantId> instances, Cluster cache) {
    // No target resource means any instance is available
    Set<ParticipantId> allInstances = Sets.newHashSet(instances);
    String targetResource = jobCfg.getTargetResource();
    if (targetResource == null) {
      return allInstances;
    }

    // Bad ideal state means don't assign
    Resource resource = cache.getResource(ResourceId.from(targetResource));
    IdealState idealState = (resource != null) ? resource.getIdealState() : null;
    if (idealState == null) {
      return Collections.emptySet();
    }

    // Get the partitions on the target resource to use
    Set<String> partitions = idealState.getPartitionSet();
    List<String> targetPartitions = jobCfg.getTargetPartitions();
    if (targetPartitions != null && !targetPartitions.isEmpty()) {
      partitions.retainAll(targetPartitions);
    }

    // Based on state matches, add eligible instances
    Set<ParticipantId> eligibleInstances = Sets.newHashSet();
    Set<String> targetStates = jobCfg.getTargetPartitionStates();
    for (String partition : partitions) {
      Map<ParticipantId, State> stateMap =
          currStateOutput.getCurrentStateMap(ResourceId.from(targetResource),
              PartitionId.from(partition));
      Map<ParticipantId, State> pendingStateMap =
          currStateOutput.getPendingStateMap(ResourceId.from(targetResource),
              PartitionId.from(partition));
      for (Map.Entry<ParticipantId, State> e : stateMap.entrySet()) {
        ParticipantId instanceName = e.getKey();
        State state = e.getValue();
        State pending = pendingStateMap.get(instanceName);
        if (pending != null) {
          continue;
        }
        if (targetStates == null || targetStates.isEmpty()
            || targetStates.contains(state.toString())) {
          eligibleInstances.add(instanceName);
        }
      }
    }
    allInstances.retainAll(eligibleInstances);
    return allInstances;
  }

  public interface RetryPolicy {
    /**
     * Adjust the assignment to allow for reassignment if a task keeps failing where it's currently
     * assigned
     * @param jobCfg the job configuration
     * @param jobCtx the job context
     * @param instances instances that can serve tasks
     * @param origAssignment the unmodified assignment
     * @return the adjusted assignment
     */
    Map<ParticipantId, SortedSet<Integer>> reassign(JobConfig jobCfg, JobContext jobCtx,
        Collection<ParticipantId> instances, Map<ParticipantId, SortedSet<Integer>> origAssignment);
  }

  private static class DefaultRetryReassigner implements RetryPolicy {
    @Override
    public Map<ParticipantId, SortedSet<Integer>> reassign(JobConfig jobCfg, JobContext jobCtx,
        Collection<ParticipantId> instances, Map<ParticipantId, SortedSet<Integer>> origAssignment) {
      // Compute an increasing integer ID for each instance
      BiMap<ParticipantId, Integer> instanceMap = HashBiMap.create(instances.size());
      int instanceIndex = 0;
      for (ParticipantId instance : instances) {
        instanceMap.put(instance, instanceIndex++);
      }

      // Move partitions
      Map<ParticipantId, SortedSet<Integer>> newAssignment = Maps.newHashMap();
      for (Map.Entry<ParticipantId, SortedSet<Integer>> e : origAssignment.entrySet()) {
        ParticipantId instance = e.getKey();
        SortedSet<Integer> partitions = e.getValue();
        Integer instanceId = instanceMap.get(instance);
        if (instanceId != null) {
          for (int p : partitions) {
            // Determine for each partition if there have been failures with the current assignment
            // strategy, and if so, force a shift in assignment for that partition only
            int shiftValue = getNumInstancesToShift(jobCfg, jobCtx, instances, p);
            int newInstanceId = (instanceId + shiftValue) % instances.size();
            ParticipantId newInstance = instanceMap.inverse().get(newInstanceId);
            if (newInstance == null) {
              newInstance = instance;
            }
            if (!newAssignment.containsKey(newInstance)) {
              newAssignment.put(newInstance, new TreeSet<Integer>());
            }
            newAssignment.get(newInstance).add(p);
          }
        } else {
          // In case something goes wrong, just keep the previous assignment
          newAssignment.put(instance, partitions);
        }
      }
      return newAssignment;
    }

    /**
     * In case tasks fail, we may not want to schedule them in the same place. This method allows us
     * to compute a shifting value so that we can systematically choose other instances to try
     * @param jobCfg the job configuration
     * @param jobCtx the job context
     * @param instances instances that can be chosen
     * @param p the partition to look up
     * @return the shifting value
     */
    private int getNumInstancesToShift(JobConfig jobCfg, JobContext jobCtx,
        Collection<ParticipantId> instances, int p) {
      int numAttempts = jobCtx.getPartitionNumAttempts(p);
      int maxNumAttempts = jobCfg.getMaxAttemptsPerTask();
      int numInstances = Math.min(instances.size(), jobCfg.getMaxForcedReassignmentsPerTask() + 1);
      return numAttempts / (maxNumAttempts / numInstances);
    }
  }
}
