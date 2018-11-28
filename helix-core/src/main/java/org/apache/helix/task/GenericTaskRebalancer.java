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

import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ResourceAssignment;


/**
 * This class does an assignment based on an automatic rebalancing strategy, rather than requiring
 * assignment to target partitions and states of another resource
 */
/** This rebalancer is deprecated, left here only for back-compatible. **/
@Deprecated
public class GenericTaskRebalancer extends DeprecatedTaskRebalancer {
  private GenericTaskAssignmentCalculator taskAssignmentCalculator =
      new GenericTaskAssignmentCalculator();

  @Override
  public Set<Integer> getAllTaskPartitions(JobConfig jobCfg, JobContext jobCtx,
      WorkflowConfig workflowCfg, WorkflowContext workflowCtx, WorkflowControllerDataProvider cache) {
    return taskAssignmentCalculator
        .getAllTaskPartitions(jobCfg, jobCtx, workflowCfg, workflowCtx, cache.getIdealStates());
  }

  @Override
  public Map<String, SortedSet<Integer>> getTaskAssignment(CurrentStateOutput currStateOutput,
      ResourceAssignment prevAssignment, Collection<String> instances, JobConfig jobCfg,
      final JobContext jobContext, WorkflowConfig workflowCfg, WorkflowContext workflowCtx,
      Set<Integer> partitionSet, WorkflowControllerDataProvider cache) {
    return taskAssignmentCalculator
        .getTaskAssignment(currStateOutput, prevAssignment, instances, jobCfg, jobContext,
            workflowCfg, workflowCtx, partitionSet, cache.getIdealStates());
  }
}
