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
import java.util.List;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * A rebalancer for when a task group must be assigned according to partitions/states on a target
 * resource. Here, tasks are colocated according to where a resource's partitions are, as well as
 * (if desired) only where those partitions are in a given state.
 */
public class FixedTargetTaskRebalancer extends TaskRebalancer {

  @Override
  public Set<Integer> getAllTaskPartitions(JobConfig jobCfg, JobContext jobCtx,
      WorkflowConfig workflowCfg, WorkflowContext workflowCtx, Cluster cache) {
    return getAllTaskPartitions(getTgtIdealState(jobCfg, cache), jobCfg, jobCtx);
  }

  @Override
  public Map<ParticipantId, SortedSet<Integer>> getTaskAssignment(
      ResourceCurrentState currStateOutput, ResourceAssignment prevAssignment,
      Collection<ParticipantId> instances, JobConfig jobCfg, JobContext jobContext,
      WorkflowConfig workflowCfg, WorkflowContext workflowCtx, Set<Integer> partitionSet,
      Cluster cache) {
    IdealState tgtIs = getTgtIdealState(jobCfg, cache);
    if (tgtIs == null) {
      return Collections.emptyMap();
    }
    Set<String> tgtStates = jobCfg.getTargetPartitionStates();
    return getTgtPartitionAssignment(currStateOutput, instances, tgtIs, tgtStates, partitionSet,
        jobContext);
  }

  /**
   * Gets the ideal state of the target resource of this job
   * @param jobCfg job config containing target resource id
   * @param cluster snapshot of the cluster containing the task and target resource
   * @return target resource ideal state, or null
   */
  private static IdealState getTgtIdealState(JobConfig jobCfg, Cluster cache) {
    String tgtResourceId = jobCfg.getTargetResource();
    Resource resource = cache.getResource(ResourceId.from(tgtResourceId));
    return resource.getIdealState();
  }

  /**
   * Returns the set of all partition ids for a job.
   * <p/>
   * If a set of partition ids was explicitly specified in the config, that is used. Otherwise, we
   * use the list of all partition ids from the target resource.
   */
  private static Set<Integer> getAllTaskPartitions(IdealState tgtResourceIs, JobConfig jobCfg,
      JobContext taskCtx) {
    if (tgtResourceIs == null) {
      return null;
    }
    Map<String, List<Integer>> currentTargets = taskCtx.getPartitionsByTarget();
    SortedSet<String> targetPartitions = Sets.newTreeSet();
    if (jobCfg.getTargetPartitions() != null) {
      targetPartitions.addAll(jobCfg.getTargetPartitions());
    } else {
      targetPartitions.addAll(tgtResourceIs.getPartitionSet());
    }

    Set<Integer> taskPartitions = Sets.newTreeSet();
    for (String pName : targetPartitions) {
      taskPartitions.addAll(getPartitionsForTargetPartition(pName, currentTargets, taskCtx));
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
  private static Map<ParticipantId, SortedSet<Integer>> getTgtPartitionAssignment(
      ResourceCurrentState currStateOutput, Collection<ParticipantId> instances, IdealState tgtIs,
      Set<String> tgtStates, Set<Integer> includeSet, JobContext jobCtx) {
    Map<ParticipantId, SortedSet<Integer>> result =
        new HashMap<ParticipantId, SortedSet<Integer>>();
    for (ParticipantId instance : instances) {
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
        for (ParticipantId instance : instances) {
          State pending =
              currStateOutput.getPendingState(ResourceId.from(tgtIs.getResourceName()),
                  PartitionId.from(pName), instance);
          if (pending != null) {
            continue;
          }
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
