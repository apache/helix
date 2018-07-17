package org.apache.helix.task;

import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceAssignment;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import org.apache.helix.task.assigner.AssignableInstance;


public abstract class TaskAssignmentCalculator {
  /**
   * Get all the partitions/tasks that belong to this job.
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

  /**
   * Returns the correct type for this job. Note that if the parent workflow has a type, then all of
   * its jobs will inherit the type from the workflow.
   * @param workflowConfig
   * @param jobConfig
   * @return
   */
  String getQuotaType(WorkflowConfig workflowConfig, JobConfig jobConfig) {
    String workflowType = workflowConfig.getWorkflowType();
    if (workflowType == null || workflowType.equals("")) {
      // Workflow type is null, so we go by the job type
      String jobType = jobConfig.getJobType();
      if (jobType == null || jobType.equals("")) {
        // Job type is null, so we use DEFAULT
        return AssignableInstance.DEFAULT_QUOTA_TYPE;
      }
      return jobType;
    }
    return workflowType;
  }
}