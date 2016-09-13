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
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ResourceAssignment;
/**
 * A rebalancer for when a task group must be assigned according to partitions/states on a target
 * resource. Here, tasks are colocated according to where a resource's partitions are, as well as
 * (if desired) only where those partitions are in a given state.
 */

/**
 * This rebalancer is deprecated, left here only for back-compatible. *
 */
@Deprecated public class FixedTargetTaskRebalancer extends DeprecatedTaskRebalancer {
  private FixedTargetTaskAssignmentCalculator taskAssignmentCalculator =
      new FixedTargetTaskAssignmentCalculator();

  @Override public Set<Integer> getAllTaskPartitions(JobConfig jobCfg, JobContext jobCtx,
      WorkflowConfig workflowCfg, WorkflowContext workflowCtx, ClusterDataCache cache) {
    return taskAssignmentCalculator
        .getAllTaskPartitions(jobCfg, jobCtx, workflowCfg, workflowCtx, cache.getIdealStates());
  }

  @Override public Map<String, SortedSet<Integer>> getTaskAssignment(
      CurrentStateOutput currStateOutput, ResourceAssignment prevAssignment,
      Collection<String> instances, JobConfig jobCfg, JobContext jobContext,
      WorkflowConfig workflowCfg, WorkflowContext workflowCtx, Set<Integer> partitionSet,
      ClusterDataCache cache) {
    return taskAssignmentCalculator
        .getTaskAssignment(currStateOutput, prevAssignment, instances, jobCfg, jobContext,
            workflowCfg, workflowCtx, partitionSet, cache.getIdealStates());
  }
}
