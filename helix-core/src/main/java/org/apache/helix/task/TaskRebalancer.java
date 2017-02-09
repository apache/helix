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

import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.controller.rebalancer.util.RebalanceScheduler;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

/**
 * Abstract rebalancer class for the {@code Task} state model.
 */
public abstract class TaskRebalancer implements Rebalancer, MappingCalculator {
  public static final String START_TIME_KEY = "StartTime";
  private static final Logger LOG = Logger.getLogger(TaskRebalancer.class);

  // For connection management
  protected HelixManager _manager;
  protected static RebalanceScheduler _scheduledRebalancer = new RebalanceScheduler();
  protected ClusterStatusMonitor _clusterStatusMonitor;

  @Override public void init(HelixManager manager) {
    _manager = manager;
  }

  @Override public abstract ResourceAssignment computeBestPossiblePartitionState(
      ClusterDataCache clusterData, IdealState taskIs, Resource resource,
      CurrentStateOutput currStateOutput);

  /**
   * Checks if the workflow has finished (either completed or failed).
   * Set the state in workflow context properly.
   *
   * @param ctx Workflow context containing job states
   * @param cfg Workflow config containing set of jobs
   * @return returns true if the workflow either completed (all tasks are {@link TaskState#COMPLETED})
   * or failed (any task is {@link TaskState#FAILED}, false otherwise.
   */
  protected boolean isWorkflowFinished(WorkflowContext ctx, WorkflowConfig cfg) {
    boolean incomplete = false;
    int failedJobs = 0;
    for (String job : cfg.getJobDag().getAllNodes()) {
      TaskState jobState = ctx.getJobState(job);
      if (!cfg.isJobQueue() && jobState == TaskState.FAILED) {
        failedJobs ++;
        if (failedJobs > cfg.getFailureThreshold()) {
          ctx.setWorkflowState(TaskState.FAILED);
          _clusterStatusMonitor.updateWorkflowCounters(cfg, TaskState.FAILED);
          for (String jobToFail : cfg.getJobDag().getAllNodes()) {
            if (ctx.getJobState(jobToFail) == TaskState.IN_PROGRESS) {
              ctx.setJobState(jobToFail, TaskState.ABORTED);
              _clusterStatusMonitor
                  .updateJobCounters(TaskUtil.getJobCfg(_manager, jobToFail), TaskState.ABORTED);
            }
          }
          return true;
        }
      }
      if (jobState != TaskState.COMPLETED && jobState != TaskState.FAILED) {
        incomplete = true;
      }
    }

    if (!incomplete && cfg.isTerminable()) {
      ctx.setWorkflowState(TaskState.COMPLETED);
      _clusterStatusMonitor.updateWorkflowCounters(cfg, TaskState.COMPLETED);
      return true;
    }

    return false;
  }

  /**
   * Checks if the workflow has been stopped.
   *
   * @param ctx Workflow context containing task states
   * @param cfg Workflow config containing set of tasks
   * @return returns true if all tasks are {@link TaskState#STOPPED}, false otherwise.
   */
  protected boolean isWorkflowStopped(WorkflowContext ctx, WorkflowConfig cfg) {
    for (String job : cfg.getJobDag().getAllNodes()) {
      TaskState jobState = ctx.getJobState(job);
      if (jobState != null && (jobState.equals(TaskState.IN_PROGRESS) || jobState
          .equals(TaskState.STOPPING))) {
        return false;
      }
    }
    return true;
  }

  protected ResourceAssignment buildEmptyAssignment(String name,
      CurrentStateOutput currStateOutput) {
    ResourceAssignment assignment = new ResourceAssignment(name);
    Set<Partition> partitions = currStateOutput.getCurrentStateMappedPartitions(name);
    for (Partition partition : partitions) {
      Map<String, String> currentStateMap = currStateOutput.getCurrentStateMap(name, partition);
      Map<String, String> replicaMap = Maps.newHashMap();
      for (String instanceName : currentStateMap.keySet()) {
        replicaMap.put(instanceName, HelixDefinedState.DROPPED.toString());
      }
      assignment.addReplicaMap(partition, replicaMap);
    }
    return assignment;
  }

  /**
   * Check all the dependencies of a job to determine whether the job is ready to be scheduled.
   *
   * @param job
   * @param workflowCfg
   * @param workflowCtx
   * @return
   */
  protected boolean isJobReadyToSchedule(String job, WorkflowConfig workflowCfg,
      WorkflowContext workflowCtx) {
    int notStartedCount = 0;
    int failedCount = 0;
    int incompleteParentCount = 0;

    for (String parent : workflowCfg.getJobDag().getDirectParents(job)) {
      TaskState jobState = workflowCtx.getJobState(parent);
      if (jobState == null || jobState == TaskState.NOT_STARTED) {
        ++notStartedCount;
      } else if (jobState == TaskState.FAILED) {
        ++failedCount;
      } else if (jobState != TaskState.COMPLETED) {
        incompleteParentCount++;
      }
    }

    // If there is any parent job not started, this job should not be scheduled
    if (notStartedCount > 0) {
      LOG.debug(String
          .format("Job %s is not ready to start, notStartedParent(s)=%d.", job, notStartedCount));
      return false;
    }

    // If there is parent job failed, schedule the job only when ignore dependent
    // job failure enabled
    JobConfig jobConfig = TaskUtil.getJobCfg(_manager, job);
    if (failedCount > 0 && !jobConfig.isIgnoreDependentJobFailure()) {
      markJobFailed(job, null, workflowCfg, workflowCtx);
      LOG.debug(
          String.format("Job %s is not ready to start, failedCount(s)=%d.", job, failedCount));
      return false;
    }

    if (workflowCfg.isJobQueue()) {
      // If job comes from a JobQueue, it should apply the parallel job logics
      int incompleteAllCount = getInCompleteJobCount(workflowCfg, workflowCtx);
      if (incompleteAllCount >= workflowCfg.getParallelJobs()) {
        LOG.debug(String.format("Job %s is not ready to schedule, inCompleteJobs(s)=%d.", job,
            incompleteAllCount));
        return false;
      }
    } else {
      // If this job comes from a generic workflow, job will not be scheduled until
      // all the direct parent jobs finished
      if (incompleteParentCount > 0) {
        LOG.debug(String.format("Job %s is not ready to start, notFinishedParent(s)=%d.", job,
            incompleteParentCount));
        return false;
      }
    }

    return true;
  }

  protected boolean isJobStarted(String job, WorkflowContext workflowContext) {
    TaskState jobState = workflowContext.getJobState(job);
    return (jobState != null && jobState != TaskState.NOT_STARTED);
  }

  /**
   * Count the number of jobs in a workflow that are in progress.
   *
   * @param workflowCfg
   * @param workflowCtx
   * @return
   */
  protected int getInCompleteJobCount(WorkflowConfig workflowCfg, WorkflowContext workflowCtx) {
    int inCompleteCount = 0;
    for (String jobName : workflowCfg.getJobDag().getAllNodes()) {
      TaskState jobState = workflowCtx.getJobState(jobName);
      if (jobState == TaskState.IN_PROGRESS || jobState == TaskState.STOPPED) {
        ++inCompleteCount;
      }
    }

    return inCompleteCount;
  }

  protected void markJobFailed(String jobName, JobContext jobContext, WorkflowConfig workflowConfig,
      WorkflowContext workflowContext) {
    long currentTime = System.currentTimeMillis();
    workflowContext.setJobState(jobName, TaskState.FAILED);
    if (jobContext != null) {
      jobContext.setFinishTime(currentTime);
    }
    if (isWorkflowFinished(workflowContext, workflowConfig)) {
      workflowContext.setFinishTime(currentTime);
    }
  }

  /**
   * Check if a workflow is ready to schedule.
   *
   * @param workflowCfg the workflow to check
   * @return true if the workflow is ready for schedule, false if not ready
   */
  protected boolean isWorkflowReadyForSchedule(WorkflowConfig workflowCfg) {
    Date startTime = workflowCfg.getStartTime();
    // Workflow with non-scheduled config or passed start time is ready to schedule.
    return (startTime == null || startTime.getTime() <= System.currentTimeMillis());
  }

  /**
   * Cleans up IdealState and external view associated with a job/workflow resource.
   */
  protected static void cleanupIdealStateExtView(HelixDataAccessor accessor, final String resourceName) {
    LOG.info("Cleaning up idealstate and externalView for job: " + resourceName);

    // Delete the ideal state itself.
    PropertyKey isKey = accessor.keyBuilder().idealStates(resourceName);
    if (accessor.getProperty(isKey) != null) {
      if (!accessor.removeProperty(isKey)) {
        LOG.error(String.format(
            "Error occurred while trying to clean up resource %s. Failed to remove node %s from Helix.",
            resourceName, isKey));
      }
    } else {
      LOG.warn(String.format("Idealstate for resource %s does not exist.", resourceName));
    }

    // Delete dead external view
    // because job is already completed, there is no more current state change
    // thus dead external views removal will not be triggered
    PropertyKey evKey = accessor.keyBuilder().externalView(resourceName);
    if (accessor.getProperty(evKey) != null) {
      if (!accessor.removeProperty(evKey)) {
        LOG.error(String.format(
            "Error occurred while trying to clean up resource %s. Failed to remove node %s from Helix.",
            resourceName, evKey));
      }
    }

    LOG.info(String
        .format("Successfully clean up idealstate/externalView for resource %s.", resourceName));
  }

  @Override public IdealState computeNewIdealState(String resourceName,
      IdealState currentIdealState, CurrentStateOutput currentStateOutput,
      ClusterDataCache clusterData) {
    // All of the heavy lifting is in the ResourceAssignment computation,
    // so this part can just be a no-op.
    return currentIdealState;
  }

  /**
   * Set the ClusterStatusMonitor for metrics update
   */
  public void setClusterStatusMonitor(ClusterStatusMonitor clusterStatusMonitor) {
     _clusterStatusMonitor = clusterStatusMonitor;
  }
}
