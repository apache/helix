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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.helix.ZNRecord;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.State;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.stages.ResourceCurrentState;
import org.apache.helix.controller.strategy.AutoRebalanceStrategy;
import org.apache.helix.model.ResourceAssignment;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A task rebalancer that evenly assigns tasks to nodes
 */
public class IndependentTaskRebalancer extends AbstractTaskRebalancer {

  @Override
  public Set<Integer> getAllTaskPartitions(TaskConfig taskCfg, WorkflowConfig workflowCfg,
      WorkflowContext workflowCtx, Cluster cluster) {
    Set<Integer> taskPartitions = new HashSet<Integer>();
    if (taskCfg.getTargetPartitions() != null) {
      for (Integer pId : taskCfg.getTargetPartitions()) {
        taskPartitions.add(pId);
      }
    }
    return taskPartitions;
  }

  @Override
  public Map<String, SortedSet<Integer>> getTaskAssignment(ResourceCurrentState currStateOutput,
      ResourceAssignment prevAssignment, Iterable<ParticipantId> instanceList, TaskConfig taskCfg,
      final TaskContext taskContext, WorkflowConfig workflowCfg, WorkflowContext workflowCtx,
      Set<Integer> partitionSet, Cluster cluster) {
    // Gather input to the full auto rebalancing algorithm
    LinkedHashMap<State, Integer> states = new LinkedHashMap<State, Integer>();
    states.put(State.from("ONLINE"), 1);

    // Only map partitions whose assignment we care about
    final Set<TaskPartitionState> honoredStates =
        Sets.newHashSet(TaskPartitionState.INIT, TaskPartitionState.RUNNING,
            TaskPartitionState.STOPPED);
    Set<Integer> filteredPartitionSet = Sets.newHashSet();
    for (Integer p : partitionSet) {
      TaskPartitionState state = (taskContext == null) ? null : taskContext.getPartitionState(p);
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
                return PartitionId.from(resourceId, partitionNum.toString());
              }
            }));

    // Compute the current assignment
    Map<PartitionId, Map<ParticipantId, State>> currentMapping = Maps.newHashMap();
    for (PartitionId partitionId : currStateOutput.getCurrentStateMappedPartitions(resourceId)) {
      if (!filteredPartitionSet.contains(pId(partitionId.toString()))) {
        // not computing old partitions
        continue;
      }
      Map<ParticipantId, State> allPreviousDecisionMap = Maps.newHashMap();
      if (prevAssignment != null) {
        allPreviousDecisionMap.putAll(prevAssignment.getReplicaMap(partitionId));
      }
      allPreviousDecisionMap.putAll(currStateOutput.getCurrentStateMap(resourceId, partitionId));
      allPreviousDecisionMap.putAll(currStateOutput.getPendingStateMap(resourceId, partitionId));
      currentMapping.put(partitionId, allPreviousDecisionMap);
    }

    // Get the assignment keyed on partition
    AutoRebalanceStrategy strategy =
        new AutoRebalanceStrategy(resourceId, partitions, states, Integer.MAX_VALUE,
            new AutoRebalanceStrategy.DefaultPlacementScheme());
    List<ParticipantId> allNodes = Lists.newArrayList(instanceList);
    ZNRecord record = strategy.typedComputePartitionAssignment(allNodes, currentMapping, allNodes);
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
    return taskAssignment;
  }
}
