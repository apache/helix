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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.helix.HelixException;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.util.JenkinsHash;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * This class does an assignment based on an automatic rebalancing strategy, rather than requiring
 * assignment to target partitions and states of another resource
 */
public class GenericTaskAssignmentCalculator extends TaskAssignmentCalculator {
  private static final Logger LOG = Logger.getLogger(GenericTaskAssignmentCalculator.class);

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
    String resourceId = prevAssignment.getResourceName();

    // Compute the current assignment
    Map<String, Map<String, String>> currentMapping = Maps.newHashMap();
    for (Partition partition : currStateOutput.getCurrentStateMappedPartitions(resourceId)) {
      if (!filteredPartitionSet.contains(TaskUtil.getPartitionId(partition.getPartitionName()))) {
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
    if (jobCfg.getTargetResource() != null) {
      LOG.error(
          "Target resource is not null, should call FixedTaskAssignmentCalculator, target resource : "
              + jobCfg.getTargetResource());
      return new HashMap<String, SortedSet<Integer>>();
    }

    List<String> allNodes = Lists.newArrayList(instances);
    ConsistentHashingPlacement placement = new ConsistentHashingPlacement(allNodes);
    Map<String, SortedSet<Integer>> taskAssignment =
        placement.computeMapping(jobCfg, jobContext, partitionNums, resourceId);

    return taskAssignment;
  }

  private class ConsistentHashingPlacement {
    private JenkinsHash _hashFunction;
    private ConsistentHashSelector _selector;
    private int _numInstances;

    public ConsistentHashingPlacement(List<String> potentialInstances) {
      _hashFunction = new JenkinsHash();
      _selector = new ConsistentHashSelector(potentialInstances);
      _numInstances = potentialInstances.size();
    }

    public Map<String, SortedSet<Integer>> computeMapping(JobConfig jobConfig,
        JobContext jobContext, List<Integer> partitions, String resourceId) {
      if (_numInstances == 0) {
        return new HashMap<String, SortedSet<Integer>>();
      }

      Map<String, SortedSet<Integer>> taskAssignment = Maps.newHashMap();

      for (int partition : partitions) {
        long hashedValue = new String(resourceId + "_" + partition).hashCode();
        int shiftTimes;
        int numAttempts = jobContext.getPartitionNumAttempts(partition);
        int maxAttempts = jobConfig.getMaxAttemptsPerTask();

        if (jobConfig.getMaxAttemptsPerTask() < _numInstances) {
          shiftTimes = numAttempts == -1 ? 0 : numAttempts;
        } else {
          shiftTimes = (maxAttempts == 0)
              ? 0
              : jobContext.getPartitionNumAttempts(partition) / (maxAttempts / _numInstances);
        }
        // Hash the value based on the shifting time. The default shift time will be 0.
        for (int i = 0; i <= shiftTimes; i++) {
          hashedValue = _hashFunction.hash(hashedValue);
        }
        String selectedInstance = select(hashedValue);
        if (selectedInstance != null) {
          if (!taskAssignment.containsKey(selectedInstance)) {
            taskAssignment.put(selectedInstance, new TreeSet<Integer>());
          }
          taskAssignment.get(selectedInstance).add(partition);
        }
      }
      return taskAssignment;
    }

    private String select(long data) throws HelixException {
      return _selector.get(data);
    }

    private class ConsistentHashSelector {
      private final static int DEFAULT_TOKENS_PER_INSTANCE = 1000;
      private final SortedMap<Long, String> circle = new TreeMap<Long, String>();
      protected int instanceSize = 0;

      public ConsistentHashSelector(List<String> instances) {
        for (String instance : instances) {
          long tokenCount = DEFAULT_TOKENS_PER_INSTANCE;
          add(instance, tokenCount);
          instanceSize++;
        }
      }

      public void add(String instance, long numberOfReplicas) {
        for (int i = 0; i < numberOfReplicas; i++) {
          circle.put(_hashFunction.hash(instance.hashCode(), i), instance);
        }
      }

      public void remove(String instance, long numberOfReplicas) {
        for (int i = 0; i < numberOfReplicas; i++) {
          circle.remove(_hashFunction.hash(instance.hashCode(), i));
        }
      }

      public String get(long data) {
        if (circle.isEmpty()) {
          return null;
        }
        long hash = _hashFunction.hash(data);
        if (!circle.containsKey(hash)) {
          SortedMap<Long, String> tailMap = circle.tailMap(hash);
          hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        return circle.get(hash);
      }
    }
  }
}
