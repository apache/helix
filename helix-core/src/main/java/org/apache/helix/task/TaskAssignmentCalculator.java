package org.apache.helix.task;

import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceAssignment;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

public abstract class TaskAssignmentCalculator {
  /**
   * Get all the partitions that should be created by this task
   *
   * @param jobCfg the task configuration
   * @param jobCtx the task context
   * @param workflowCfg the workflow configuration
   * @param workflowCtx the workflow context
   * @param idealStateMap the map of resource name map to ideal state
   * @return set of partition numbers
   */
  public abstract Set<Integer> getAllTaskPartitions(JobConfig jobCfg, JobContext jobCtx,
      WorkflowConfig workflowCfg, WorkflowContext workflowCtx,
      Map<String, IdealState> idealStateMap);

  /**
   * Compute an assignment of tasks to instances
   *
   * @param currStateOutput the current state of the instances
   * @param prevAssignment the previous task partition assignment
   * @param instances the instances
   * @param jobCfg the task configuration
   * @param jobContext the task context
   * @param workflowCfg the workflow configuration
   * @param workflowCtx the workflow context
   * @param partitionSet the partitions to assign
   * @param idealStateMap the map of resource name map to ideal state
   * @return map of instances to set of partition numbers
   */
  public abstract Map<String, SortedSet<Integer>> getTaskAssignment(
      CurrentStateOutput currStateOutput, ResourceAssignment prevAssignment,
      Collection<String> instances, JobConfig jobCfg, JobContext jobContext,
      WorkflowConfig workflowCfg, WorkflowContext workflowCtx, Set<Integer> partitionSet,
      Map<String, IdealState> idealStateMap);
}
