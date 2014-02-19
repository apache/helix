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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.helix.api.Cluster;
import org.apache.helix.api.Resource;
import org.apache.helix.api.State;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.stages.ResourceCurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceAssignment;

/**
 * Custom rebalancer implementation for the {@code Task} state model. Tasks are assigned to
 * instances hosting target resource partitions in target states
 */
public class TaskRebalancer extends AbstractTaskRebalancer {
  @Override
  public Set<Integer> getAllTaskPartitions(TaskConfig taskCfg, WorkflowConfig workflowCfg,
      WorkflowContext workflowCtx, Cluster cluster) {
    return getAllTaskPartitions(getTgtIdealState(taskCfg, cluster), taskCfg);
  }

  @Override
  public Map<String, SortedSet<Integer>> getTaskAssignment(ResourceCurrentState currStateOutput,
      ResourceAssignment prevAssignment, Iterable<ParticipantId> instanceList, TaskConfig taskCfg,
      TaskContext taskCtx, WorkflowConfig workflowCfg, WorkflowContext workflowCtx,
      Set<Integer> partitionSet, Cluster cluster) {
    IdealState tgtIs = getTgtIdealState(taskCfg, cluster);
    if (tgtIs == null) {
      return Collections.emptyMap();
    }
    Set<String> tgtStates = taskCfg.getTargetPartitionStates();
    return getTgtPartitionAssignment(currStateOutput, instanceList, tgtIs, tgtStates, partitionSet);
  }

  /**
   * Gets the ideal state of the target resource of this task
   * @param taskCfg task config containing target resource id
   * @param cluster snapshot of the cluster containing the task and target resource
   * @return target resource ideal state, or null
   */
  private static IdealState getTgtIdealState(TaskConfig taskCfg, Cluster cluster) {
    ResourceId tgtResourceId = ResourceId.from(taskCfg.getTargetResource());
    Resource resource = cluster.getResource(tgtResourceId);
    return resource != null ? resource.getIdealState() : null;
  }

  /**
   * Returns the set of all partition ids for a task.
   * <p/>
   * If a set of partition ids was explicitly specified in the config, that is used. Otherwise, we
   * use the list of all partition ids from the target resource.
   */
  private static Set<Integer> getAllTaskPartitions(IdealState tgtResourceIs, TaskConfig taskCfg) {
    if (tgtResourceIs == null) {
      return null;
    }
    Set<Integer> taskPartitions = new HashSet<Integer>();
    if (taskCfg.getTargetPartitions() != null) {
      for (Integer pId : taskCfg.getTargetPartitions()) {
        taskPartitions.add(pId);
      }
    } else {
      for (String pName : tgtResourceIs.getPartitionSet()) {
        taskPartitions.add(pId(pName));
      }
    }
    return taskPartitions;
  }

  /**
   * Get partition assignments for the target resource, but only for the partitions of interest.
   * @param currStateOutput The current state of the instances in the cluster.
   * @param instanceList The set of instances.
   * @param tgtIs The ideal state of the target resource.
   * @param tgtStates Only partitions in this set of states will be considered. If null, partitions
   *          do not need to
   *          be in any specific state to be considered.
   * @param includeSet The set of partitions to consider.
   * @return A map of instance vs set of partition ids assigned to that instance.
   */
  private static Map<String, SortedSet<Integer>> getTgtPartitionAssignment(
      ResourceCurrentState currStateOutput, Iterable<ParticipantId> instanceList, IdealState tgtIs,
      Set<String> tgtStates, Set<Integer> includeSet) {
    Map<String, SortedSet<Integer>> result = new HashMap<String, SortedSet<Integer>>();
    for (ParticipantId instance : instanceList) {
      result.put(instance.stringify(), new TreeSet<Integer>());
    }

    for (String pName : tgtIs.getPartitionSet()) {
      int pId = pId(pName);
      if (includeSet.contains(pId)) {
        for (ParticipantId instance : instanceList) {
          State s =
              currStateOutput.getCurrentState(ResourceId.from(tgtIs.getResourceName()),
                  PartitionId.from(pName), instance);
          String state = (s == null ? null : s.toString());
          if (tgtStates == null || tgtStates.contains(state)) {
            result.get(instance).add(pId);
          }
        }
      }
    }

    return result;
  }
}
