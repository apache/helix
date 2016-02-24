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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

/**
 * Abstract rebalancer class for the {@code Task} state model.
 */
public abstract class TaskRebalancer implements Rebalancer, MappingCalculator {
  private static final Logger LOG = Logger.getLogger(TaskRebalancer.class);

  // For connection management
  protected HelixManager _manager;
  protected static ScheduledRebalancer _scheduledRebalancer = new ScheduledRebalancer();

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
    for (String job : cfg.getJobDag().getAllNodes()) {
      TaskState jobState = ctx.getJobState(job);
      if (jobState == TaskState.FAILED) {
        ctx.setWorkflowState(TaskState.FAILED);
        return true;
      }
      if (jobState != TaskState.COMPLETED) {
        incomplete = true;
      }
    }

    if (!incomplete && cfg.isTerminable()) {
      ctx.setWorkflowState(TaskState.COMPLETED);
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
      if (jobState != null && jobState != TaskState.COMPLETED && jobState != TaskState.FAILED
          && jobState != TaskState.STOPPED)
        return false;
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
    int inCompleteCount = 0;
    int failedCount = 0;

    for (String ancestor : workflowCfg.getJobDag().getAncestors(job)) {
      TaskState jobState = workflowCtx.getJobState(ancestor);
      if (jobState == null || jobState == TaskState.NOT_STARTED) {
        ++notStartedCount;
      } else if (jobState == TaskState.IN_PROGRESS || jobState == TaskState.STOPPED) {
        ++inCompleteCount;
      } else if (jobState == TaskState.FAILED) {
        ++failedCount;
      }
    }

    if (notStartedCount > 0 || inCompleteCount >= workflowCfg.getParallelJobs()
        || failedCount > 0) {
      LOG.debug(String.format(
          "Job %s is not ready to start, notStartedParent(s)=%d, inCompleteParent(s)=%d, failedParent(s)=%d.",
          job, notStartedCount, inCompleteCount, failedCount));
      return false;
    }

    return true;
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

  // Management of already-scheduled rebalances across all task entities.
  protected static class ScheduledRebalancer {
    private class ScheduledTask {
      long _startTime;
      Future _future;

      public ScheduledTask(long _startTime, Future _future) {
        this._startTime = _startTime;
        this._future = _future;
      }

      public long getStartTime() {
        return _startTime;
      }

      public Future getFuture() {
        return _future;
      }
    }

    private final Map<String, ScheduledTask> _rebalanceTasks = new HashMap<String, ScheduledTask>();
    private final ScheduledExecutorService _rebalanceExecutor =
        Executors.newSingleThreadScheduledExecutor();

    /**
     * Add a future rebalance task for resource at given startTime
     *
     * @param resource
     * @param startTime time in milliseconds
     */
    public void scheduleRebalance(HelixManager manager, String resource, long startTime) {
      // Do nothing if there is already a timer set for the this workflow with the same start time.
      ScheduledTask existTask = _rebalanceTasks.get(resource);
      if (existTask != null && existTask.getStartTime() == startTime) {
        LOG.debug("Schedule timer for job: " + resource + " is up to date.");
        return;
      }

      long delay = startTime - System.currentTimeMillis();
      LOG.info("Schedule rebalance with job: " + resource + " at time: " + startTime + " delay: "
          + delay);

      // For workflow not yet scheduled, schedule them and record it
      RebalanceInvoker rebalanceInvoker = new RebalanceInvoker(manager, resource);
      ScheduledFuture future =
          _rebalanceExecutor.schedule(rebalanceInvoker, delay, TimeUnit.MILLISECONDS);
      ScheduledTask prevTask = _rebalanceTasks.put(resource, new ScheduledTask(startTime, future));
      if (prevTask != null && !prevTask.getFuture().isDone()) {
        if (!prevTask.getFuture().cancel(false)) {
          LOG.warn("Failed to cancel scheduled timer task for " + resource);
        }
      }
    }

    /**
     * Get the current schedule time for given resource.
     *
     * @param resource
     * @return existing schedule time or NULL if there is no scheduled task for this resource
     */
    public long getRebalanceTime(String resource) {
      ScheduledTask task = _rebalanceTasks.get(resource);
      if (task != null) {
        return task.getStartTime();
      }
      return -1;
    }

    /**
     * Remove all existing future schedule tasks for the given resource
     *
     * @param resource
     */
    public void removeScheduledRebalance(String resource) {
      ScheduledTask existTask = _rebalanceTasks.remove(resource);
      if (existTask != null && !existTask.getFuture().isDone()) {
        if (!existTask.getFuture().cancel(true)) {
          LOG.warn("Failed to cancel scheduled timer task for " + resource);
        }
        LOG.info(
            "Remove scheduled rebalance task at time " + existTask.getStartTime() + " for resource: "
                + resource);
      }
    }

    /**
     * The simplest possible runnable that will trigger a run of the controller pipeline
     */
    private class RebalanceInvoker implements Runnable {
      private final HelixManager _manager;
      private final String _resource;

      public RebalanceInvoker(HelixManager manager, String resource) {
        _manager = manager;
        _resource = resource;
      }

      @Override public void run() {
        TaskUtil.invokeRebalance(_manager.getHelixDataAccessor(), _resource);
      }
    }
  }
}
