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

import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.log4j.Logger;

/**
 * A TaskAssignmentCalculator for when a task group must be assigned according to partitions/states on a target
 * resource. Here, tasks are co-located according to where a resource's partitions are, as well as
 * (if desired) only where those partitions are in a given state.
 */
public class FixedTargetTaskAssignmentCalculator extends TaskAssignmentCalculator {
  private static final Logger LOG = Logger.getLogger(FixedTargetTaskAssignmentCalculator.class);

  @Override public Set<Integer> getAllTaskPartitions(JobConfig jobCfg, JobContext jobCtx,
      WorkflowConfig workflowCfg, WorkflowContext workflowCtx,
      Map<String, IdealState> idealStateMap) {
    return getAllTaskPartitions(getTgtIdealState(jobCfg, idealStateMap), jobCfg, jobCtx);
  }

  @Override
  public Map<String, SortedSet<Integer>> getTaskAssignment(CurrentStateOutput currStateOutput,
      ResourceAssignment prevAssignment, Collection<String> instances, JobConfig jobCfg,
      JobContext jobContext, WorkflowConfig workflowCfg, WorkflowContext workflowCtx,
      Set<Integer> partitionSet, Map<String, IdealState> idealStateMap) {
    IdealState tgtIs = getTgtIdealState(jobCfg, idealStateMap);
    if (tgtIs == null) {
      LOG.warn("Missing target resource for the scheduled job!");
      return Collections.emptyMap();
    }
    Set<String> tgtStates = jobCfg.getTargetPartitionStates();
    return getTgtPartitionAssignment(currStateOutput, instances, tgtIs, tgtStates, partitionSet,
        jobContext);
  }

  /**
   * Gets the ideal state of the target resource of this job
   * @param jobCfg job config containing target resource id
   * @param idealStateMap the map of resource name map to ideal state
   * @return target resource ideal state, or null
   */
  private static IdealState getTgtIdealState(JobConfig jobCfg,
      Map<String, IdealState> idealStateMap) {
    String tgtResourceId = jobCfg.getTargetResource();
    return idealStateMap.get(tgtResourceId);
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
  private static Map<String, SortedSet<Integer>> getTgtPartitionAssignment(
      CurrentStateOutput currStateOutput, Iterable<String> instances, IdealState tgtIs,
      Set<String> tgtStates, Set<Integer> includeSet, JobContext jobCtx) {
    Map<String, SortedSet<Integer>> result = new HashMap<String, SortedSet<Integer>>();
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
          Message pendingMessage =
              currStateOutput.getPendingState(tgtIs.getResourceName(), new Partition(pName),
                  instance);
          if (pendingMessage != null) {
            continue;
          }

          String s =
              currStateOutput.getCurrentState(tgtIs.getResourceName(), new Partition(pName),
                  instance);
          if (s != null && (tgtStates == null || tgtStates.contains(s))) {
            result.get(instance).add(pId);
          }
        }
      }
    }

    return result;
  }
}
