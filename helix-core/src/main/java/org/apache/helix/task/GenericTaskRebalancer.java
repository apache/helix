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
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
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
      WorkflowConfig workflowCfg, WorkflowContext workflowCtx, ClusterDataCache cache) {
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
      final JobContext jobContext, WorkflowConfig workflowCfg, WorkflowContext workflowCtx,
      Set<Integer> partitionSet, ClusterDataCache cache) {
    // Gather input to the full auto rebalancing algorithm
    LinkedHashMap<String, Integer> states = new LinkedHashMap<String, Integer>();
    states.put("ONLINE", 1);

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
    final String resourceId = prevAssignment.getResourceName();
    List<String> partitions =
        new ArrayList<String>(Lists.transform(partitionNums, new Function<Integer, String>() {
          @Override
          public String apply(Integer partitionNum) {
            return resourceId + "_" + partitionNum;
          }
        }));

    // Compute the current assignment
    Map<String, Map<String, String>> currentMapping = Maps.newHashMap();
    for (Partition partition : currStateOutput.getCurrentStateMappedPartitions(resourceId)) {
      if (!filteredPartitionSet.contains(pId(partition.getPartitionName()))) {
        // not computing old partitions
        continue;
      }
      Map<String, String> allPreviousDecisionMap = Maps.newHashMap();
      if (prevAssignment != null) {
        allPreviousDecisionMap.putAll(prevAssignment.getReplicaMap(partition));
      }
      allPreviousDecisionMap.putAll(currStateOutput.getCurrentStateMap(resourceId, partition));
      allPreviousDecisionMap.putAll(currStateOutput.getPendingStateMap(resourceId, partition));
      currentMapping.put(partition.getPartitionName(), allPreviousDecisionMap);
    }

    // Get the assignment keyed on partition
    AutoRebalanceStrategy strategy =
        new AutoRebalanceStrategy(resourceId, partitions, states, Integer.MAX_VALUE,
            new AutoRebalanceStrategy.DefaultPlacementScheme());
    List<String> allNodes =
        Lists.newArrayList(getEligibleInstances(jobCfg, currStateOutput, instances, cache));
    Collections.sort(allNodes);
    ZNRecord record = strategy.computePartitionAssignment(allNodes, currentMapping, allNodes);
    Map<String, List<String>> preferenceLists = record.getListFields();

    // Convert to an assignment keyed on participant
    Map<String, SortedSet<Integer>> taskAssignment = Maps.newHashMap();
    for (Map.Entry<String, List<String>> e : preferenceLists.entrySet()) {
      String partitionName = e.getKey();
      partitionName = String.valueOf(pId(partitionName));
      List<String> preferenceList = e.getValue();
      for (String participantName : preferenceList) {
        if (!taskAssignment.containsKey(participantName)) {
          taskAssignment.put(participantName, new TreeSet<Integer>());
        }
        taskAssignment.get(participantName).add(Integer.valueOf(partitionName));
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
  private Set<String> getEligibleInstances(JobConfig jobCfg, CurrentStateOutput currStateOutput,
      Iterable<String> instances, ClusterDataCache cache) {
    // No target resource means any instance is available
    Set<String> allInstances = Sets.newHashSet(instances);
    String targetResource = jobCfg.getTargetResource();
    if (targetResource == null) {
      return allInstances;
    }

    // Bad ideal state means don't assign
    IdealState idealState = cache.getIdealState(targetResource);
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
    Set<String> eligibleInstances = Sets.newHashSet();
    Set<String> targetStates = jobCfg.getTargetPartitionStates();
    for (String partition : partitions) {
      Map<String, String> stateMap =
          currStateOutput.getCurrentStateMap(targetResource, new Partition(partition));
      Map<String, String> pendingStateMap =
          currStateOutput.getPendingStateMap(targetResource, new Partition(partition));
      for (Map.Entry<String, String> e : stateMap.entrySet()) {
        String instanceName = e.getKey();
        String state = e.getValue();
        String pending = pendingStateMap.get(instanceName);
        if (pending != null) {
          continue;
        }
        if (targetStates == null || targetStates.isEmpty() || targetStates.contains(state)) {
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
    Map<String, SortedSet<Integer>> reassign(JobConfig jobCfg, JobContext jobCtx,
        Collection<String> instances, Map<String, SortedSet<Integer>> origAssignment);
  }

  private static class DefaultRetryReassigner implements RetryPolicy {
    @Override
    public Map<String, SortedSet<Integer>> reassign(JobConfig jobCfg, JobContext jobCtx,
        Collection<String> instances, Map<String, SortedSet<Integer>> origAssignment) {
      // Compute an increasing integer ID for each instance
      BiMap<String, Integer> instanceMap = HashBiMap.create(instances.size());
      int instanceIndex = 0;
      for (String instance : instances) {
        instanceMap.put(instance, instanceIndex++);
      }

      // Move partitions
      Map<String, SortedSet<Integer>> newAssignment = Maps.newHashMap();
      for (Map.Entry<String, SortedSet<Integer>> e : origAssignment.entrySet()) {
        String instance = e.getKey();
        SortedSet<Integer> partitions = e.getValue();
        Integer instanceId = instanceMap.get(instance);
        if (instanceId != null) {
          for (int p : partitions) {
            // Determine for each partition if there have been failures with the current assignment
            // strategy, and if so, force a shift in assignment for that partition only
            int shiftValue = getNumInstancesToShift(jobCfg, jobCtx, instances, p);
            int newInstanceId = (instanceId + shiftValue) % instances.size();
            String newInstance = instanceMap.inverse().get(newInstanceId);
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
        Collection<String> instances, int p) {
      int numAttempts = jobCtx.getPartitionNumAttempts(p);
      int maxNumAttempts = jobCfg.getMaxAttemptsPerTask();
      int numInstances = Math.min(instances.size(), jobCfg.getMaxForcedReassignmentsPerTask() + 1);
      return numAttempts / (maxNumAttempts / numInstances);
    }
  }
}
