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
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Resource;
import org.apache.helix.api.State;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.context.ControllerContextProvider;
import org.apache.helix.controller.rebalancer.HelixRebalancer;
import org.apache.helix.controller.rebalancer.config.RebalancerConfig;
import org.apache.helix.controller.stages.ResourceCurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfiguration;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Custom rebalancer implementation for the {@code Task} state model.
 */
public abstract class TaskRebalancer implements HelixRebalancer {
  private static final Logger LOG = Logger.getLogger(TaskRebalancer.class);

  // Management of already-scheduled rebalances across jobs
  private static final BiMap<String, Date> SCHEDULED_TIMES = HashBiMap.create();
  private static final ScheduledExecutorService SCHEDULED_EXECUTOR = Executors
      .newSingleThreadScheduledExecutor();

  // For connection management
  private HelixManager _manager;

  /**
   * Get all the partitions that should be created by this task
   * @param jobCfg the task configuration
   * @param jobCtx the task context
   * @param workflowCfg the workflow configuration
   * @param workflowCtx the workflow context
   * @param cache cluster snapshot
   * @return set of partition numbers
   */
  public abstract Set<Integer> getAllTaskPartitions(JobConfig jobCfg, JobContext jobCtx,
      WorkflowConfig workflowCfg, WorkflowContext workflowCtx, Cluster cache);

  /**
   * Compute an assignment of tasks to instances
   * @param currStateOutput the current state of the instances
   * @param prevAssignment the previous task partition assignment
   * @param instances the instances
   * @param jobCfg the task configuration
   * @param taskCtx the task context
   * @param workflowCfg the workflow configuration
   * @param workflowCtx the workflow context
   * @param partitionSet the partitions to assign
   * @param cache cluster snapshot
   * @return map of instances to set of partition numbers
   */
  public abstract Map<ParticipantId, SortedSet<Integer>> getTaskAssignment(
      ResourceCurrentState currStateOutput, ResourceAssignment prevAssignment,
      Collection<ParticipantId> instanceList, JobConfig jobCfg, JobContext jobContext,
      WorkflowConfig workflowCfg, WorkflowContext workflowCtx, Set<Integer> partitionSet,
      Cluster cache);

  @Override
  public void init(HelixManager manager, ControllerContextProvider contextProvider) {
    _manager = manager;
  }

  @Override
  public ResourceAssignment computeResourceMapping(IdealState taskIs,
      RebalancerConfig rebalancerConfig, ResourceAssignment prevAssignment, Cluster cluster,
      ResourceCurrentState currentState) {
    return computeBestPossiblePartitionState(cluster, taskIs,
        cluster.getResource(taskIs.getResourceId()), currentState);
  }

  private ResourceAssignment computeBestPossiblePartitionState(Cluster clusterData,
      IdealState taskIs, Resource resource, ResourceCurrentState currStateOutput) {
    final String resourceName = resource.getId().toString();

    // Fetch job configuration
    Map<String, ResourceConfiguration> resourceConfigs =
        clusterData.getCache().getResourceConfigs();
    JobConfig jobCfg = TaskUtil.getJobCfg(resourceConfigs.get(resourceName));
    if (jobCfg == null) {
      return emptyAssignment(resourceName, currStateOutput);
    }
    String workflowResource = jobCfg.getWorkflow();

    // Fetch workflow configuration and context
    WorkflowConfig workflowCfg = TaskUtil.getWorkflowCfg(resourceConfigs.get(workflowResource));
    if (workflowCfg == null) {
      return emptyAssignment(resourceName, currStateOutput);
    }
    WorkflowContext workflowCtx = TaskUtil.getWorkflowContext(_manager, workflowResource);

    // Initialize workflow context if needed
    if (workflowCtx == null) {
      workflowCtx = new WorkflowContext(new ZNRecord("WorkflowContext"));
      workflowCtx.setStartTime(System.currentTimeMillis());
    }

    // Check parent dependencies
    for (String parent : workflowCfg.getJobDag().getDirectParents(resourceName)) {
      if (workflowCtx.getJobState(parent) == null
          || !workflowCtx.getJobState(parent).equals(TaskState.COMPLETED)) {
        return emptyAssignment(resourceName, currStateOutput);
      }
    }

    // Clean up if workflow marked for deletion
    TargetState targetState = workflowCfg.getTargetState();
    if (targetState == TargetState.DELETE) {
      cleanup(_manager, resourceName, workflowCfg, workflowResource);
      return emptyAssignment(resourceName, currStateOutput);
    }

    // Check if this workflow has been finished past its expiry.
    if (workflowCtx.getFinishTime() != WorkflowContext.UNFINISHED
        && workflowCtx.getFinishTime() + workflowCfg.getExpiry() <= System.currentTimeMillis()) {
      markForDeletion(_manager, workflowResource);
      cleanup(_manager, resourceName, workflowCfg, workflowResource);
      return emptyAssignment(resourceName, currStateOutput);
    }

    // Fetch any existing context information from the property store.
    JobContext jobCtx = TaskUtil.getJobContext(_manager, resourceName);
    if (jobCtx == null) {
      jobCtx = new JobContext(new ZNRecord("TaskContext"));
      jobCtx.setStartTime(System.currentTimeMillis());
    }

    // Check for expired jobs for non-terminable workflows
    long jobFinishTime = jobCtx.getFinishTime();
    if (!workflowCfg.isTerminable() && jobFinishTime != WorkflowContext.UNFINISHED
        && jobFinishTime + workflowCfg.getExpiry() <= System.currentTimeMillis()) {
      cleanup(_manager, resourceName, workflowCfg, workflowResource);
      return emptyAssignment(resourceName, currStateOutput);
    }

    // The job is already in a final state (completed/failed).
    if (workflowCtx.getJobState(resourceName) == TaskState.FAILED
        || workflowCtx.getJobState(resourceName) == TaskState.COMPLETED) {
      return emptyAssignment(resourceName, currStateOutput);
    }

    // Check for readiness, and stop processing if it's not ready
    boolean isReady =
        scheduleIfNotReady(workflowCfg, workflowCtx, workflowResource, resourceName, clusterData);
    if (!isReady) {
      return emptyAssignment(resourceName, currStateOutput);
    }

    // Grab the old assignment, or an empty one if it doesn't exist
    ResourceAssignment prevAssignment = TaskUtil.getPrevResourceAssignment(_manager, resourceName);
    if (prevAssignment == null) {
      prevAssignment = new ResourceAssignment(ResourceId.from(resourceName));
    }

    // Will contain the list of partitions that must be explicitly dropped from the ideal state that
    // is stored in zk.
    // Fetch the previous resource assignment from the property store. This is required because of
    // HELIX-230.
    Set<Integer> partitionsToDrop = new TreeSet<Integer>();

    ResourceAssignment newAssignment =
        computeResourceMapping(resourceName, workflowCfg, jobCfg, prevAssignment, clusterData
            .getLiveParticipantMap().keySet(), currStateOutput, workflowCtx, jobCtx,
            partitionsToDrop, clusterData);

    if (!partitionsToDrop.isEmpty()) {
      for (Integer pId : partitionsToDrop) {
        taskIs.getRecord().getMapFields().remove(pName(resourceName, pId));
      }
      HelixDataAccessor accessor = _manager.getHelixDataAccessor();
      PropertyKey propertyKey = accessor.keyBuilder().idealStates(resourceName);
      accessor.setProperty(propertyKey, taskIs);
    }

    // Update rebalancer context, previous ideal state.
    TaskUtil.setJobContext(_manager, resourceName, jobCtx);
    TaskUtil.setWorkflowContext(_manager, workflowResource, workflowCtx);
    TaskUtil.setPrevResourceAssignment(_manager, resourceName, newAssignment);

    return newAssignment;
  }

  private ResourceAssignment computeResourceMapping(String jobResource,
      WorkflowConfig workflowConfig, JobConfig jobCfg, ResourceAssignment prevAssignment,
      Collection<ParticipantId> liveInstances, ResourceCurrentState currStateOutput,
      WorkflowContext workflowCtx, JobContext jobCtx, Set<Integer> partitionsToDropFromIs,
      Cluster cache) {
    TargetState jobTgtState = workflowConfig.getTargetState();

    // Update running status in workflow context
    if (jobTgtState == TargetState.STOP) {
      workflowCtx.setJobState(jobResource, TaskState.STOPPED);
      // Workflow has been stopped if all jobs are stopped
      if (isWorkflowStopped(workflowCtx, workflowConfig)) {
        workflowCtx.setWorkflowState(TaskState.STOPPED);
      }
    } else {
      workflowCtx.setJobState(jobResource, TaskState.IN_PROGRESS);
      // Workflow is in progress if any task is in progress
      workflowCtx.setWorkflowState(TaskState.IN_PROGRESS);
    }

    // Used to keep track of tasks that have already been assigned to instances.
    Set<Integer> assignedPartitions = new HashSet<Integer>();

    // Used to keep track of tasks that have failed, but whose failure is acceptable
    Set<Integer> skippedPartitions = new HashSet<Integer>();

    // Keeps a mapping of (partition) -> (instance, state)
    Map<Integer, PartitionAssignment> paMap = new TreeMap<Integer, PartitionAssignment>();

    // Process all the current assignments of tasks.
    Set<Integer> allPartitions =
        getAllTaskPartitions(jobCfg, jobCtx, workflowConfig, workflowCtx, cache);
    Map<ParticipantId, SortedSet<Integer>> taskAssignments =
        getTaskPartitionAssignments(liveInstances, prevAssignment, allPartitions);

    long currentTime = System.currentTimeMillis();
    for (ParticipantId instance : taskAssignments.keySet()) {
      Set<Integer> pSet = taskAssignments.get(instance);
      // Used to keep track of partitions that are in one of the final states: COMPLETED, TIMED_OUT,
      // TASK_ERROR, ERROR.
      Set<Integer> donePartitions = new TreeSet<Integer>();
      for (int pId : pSet) {
        final String pName = pName(jobResource, pId);

        // Check for pending state transitions on this (partition, instance).
        State pendingState =
            currStateOutput.getPendingState(ResourceId.from(jobResource), PartitionId.from(pName),
                instance);
        if (pendingState != null) {
          // There is a pending state transition for this (partition, instance). Just copy forward
          // the state assignment from the previous ideal state.
          Map<ParticipantId, State> stateMap =
              prevAssignment.getReplicaMap(PartitionId.from(pName));
          if (stateMap != null) {
            State prevState = stateMap.get(instance);
            paMap.put(pId, new PartitionAssignment(instance.toString(), prevState.toString()));
            assignedPartitions.add(pId);
            LOG.debug(String
                .format(
                    "Task partition %s has a pending state transition on instance %s. Using the previous ideal state which was %s.",
                    pName, instance, prevState));
          }

          continue;
        }

        State currHelixState =
            currStateOutput.getCurrentState(ResourceId.from(jobResource), PartitionId.from(pName),
                instance);
        TaskPartitionState currState =
            (currHelixState != null) ? TaskPartitionState.valueOf(currHelixState.toString()) : null;
        if (currState != null) {
          jobCtx.setPartitionState(pId, currState);
        }

        // Process any requested state transitions.
        State requestedStateStr =
            currStateOutput.getRequestedState(ResourceId.from(jobResource),
                PartitionId.from(pName), instance);
        if (requestedStateStr != null && !requestedStateStr.toString().isEmpty()) {
          TaskPartitionState requestedState =
              TaskPartitionState.valueOf(requestedStateStr.toString());
          if (requestedState.equals(currState)) {
            LOG.warn(String.format(
                "Requested state %s is the same as the current state for instance %s.",
                requestedState, instance));
          }

          paMap.put(pId, new PartitionAssignment(instance.toString(), requestedState.name()));
          assignedPartitions.add(pId);
          LOG.debug(String.format(
              "Instance %s requested a state transition to %s for partition %s.", instance,
              requestedState, pName));
          continue;
        }

        switch (currState) {
        case RUNNING:
        case STOPPED: {
          TaskPartitionState nextState;
          if (jobTgtState == TargetState.START) {
            nextState = TaskPartitionState.RUNNING;
          } else {
            nextState = TaskPartitionState.STOPPED;
          }

          paMap.put(pId, new PartitionAssignment(instance.toString(), nextState.name()));
          assignedPartitions.add(pId);
          LOG.debug(String.format("Setting task partition %s state to %s on instance %s.", pName,
              nextState, instance));
        }
          break;
        case COMPLETED: {
          // The task has completed on this partition. Mark as such in the context object.
          donePartitions.add(pId);
          LOG.debug(String
              .format(
                  "Task partition %s has completed with state %s. Marking as such in rebalancer context.",
                  pName, currState));
          partitionsToDropFromIs.add(pId);
          markPartitionCompleted(jobCtx, pId);
        }
          break;
        case TIMED_OUT:
        case TASK_ERROR:
        case ERROR: {
          donePartitions.add(pId); // The task may be rescheduled on a different instance.
          LOG.debug(String.format(
              "Task partition %s has error state %s. Marking as such in rebalancer context.",
              pName, currState));
          markPartitionError(jobCtx, pId, currState, true);
          // The error policy is to fail the task as soon a single partition fails for a specified
          // maximum number of attempts.
          if (jobCtx.getPartitionNumAttempts(pId) >= jobCfg.getMaxAttemptsPerTask()) {
            // If the user does not require this task to succeed in order for the job to succeed,
            // then we don't have to fail the job right now
            boolean successOptional = false;
            String taskId = jobCtx.getTaskIdForPartition(pId);
            if (taskId != null) {
              TaskConfig taskConfig = jobCfg.getTaskConfig(taskId);
              if (taskConfig != null) {
                successOptional = taskConfig.isSuccessOptional();
              }
            }

            // Similarly, if we have some leeway for how many tasks we can fail, then we don't have
            // to fail the job immediately
            if (skippedPartitions.size() < jobCfg.getFailureThreshold()) {
              successOptional = true;
            }

            if (!successOptional) {
              long finishTime = currentTime;
              workflowCtx.setJobState(jobResource, TaskState.FAILED);
              if (workflowConfig.isTerminable()) {
                workflowCtx.setWorkflowState(TaskState.FAILED);
                workflowCtx.setFinishTime(finishTime);
              }
              jobCtx.setFinishTime(finishTime);
              markAllPartitionsError(jobCtx, currState, false);
              addAllPartitions(allPartitions, partitionsToDropFromIs);
              return emptyAssignment(jobResource, currStateOutput);
            } else {
              skippedPartitions.add(pId);
              partitionsToDropFromIs.add(pId);
            }
          } else {
            // Mark the task to be started at some later time (if enabled)
            markPartitionDelayed(jobCfg, jobCtx, pId);
          }
        }
          break;
        case INIT:
        case DROPPED: {
          // currState in [INIT, DROPPED]. Do nothing, the partition is eligible to be reassigned.
          donePartitions.add(pId);
          LOG.debug(String.format(
              "Task partition %s has state %s. It will be dropped from the current ideal state.",
              pName, currState));
        }
          break;
        default:
          throw new AssertionError("Unknown enum symbol: " + currState);
        }
      }

      // Remove the set of task partitions that are completed or in one of the error states.
      pSet.removeAll(donePartitions);
    }

    // For delayed tasks, trigger a rebalance event for the closest upcoming ready time
    scheduleForNextTask(jobResource, jobCtx, currentTime);

    if (isJobComplete(jobCtx, allPartitions, skippedPartitions)) {
      workflowCtx.setJobState(jobResource, TaskState.COMPLETED);
      jobCtx.setFinishTime(currentTime);
      if (isWorkflowComplete(workflowCtx, workflowConfig)) {
        workflowCtx.setWorkflowState(TaskState.COMPLETED);
        workflowCtx.setFinishTime(currentTime);
      }
    }

    // Make additional task assignments if needed.
    if (jobTgtState == TargetState.START) {
      // Contains the set of task partitions that must be excluded from consideration when making
      // any new assignments.
      // This includes all completed, failed, delayed, and already assigned partitions.
      Set<Integer> excludeSet = Sets.newTreeSet(assignedPartitions);
      addCompletedPartitions(excludeSet, jobCtx, allPartitions);
      excludeSet.addAll(skippedPartitions);
      excludeSet.addAll(getNonReadyPartitions(jobCtx, currentTime));
      // Get instance->[partition, ...] mappings for the target resource.
      Map<ParticipantId, SortedSet<Integer>> tgtPartitionAssignments =
          getTaskAssignment(currStateOutput, prevAssignment, liveInstances, jobCfg, jobCtx,
              workflowConfig, workflowCtx, allPartitions, cache);
      for (Map.Entry<ParticipantId, SortedSet<Integer>> entry : taskAssignments.entrySet()) {
        ParticipantId instance = entry.getKey();
        if (!tgtPartitionAssignments.containsKey(instance)) {
          continue;
        }
        // Contains the set of task partitions currently assigned to the instance.
        Set<Integer> pSet = entry.getValue();
        int numToAssign = jobCfg.getNumConcurrentTasksPerInstance() - pSet.size();
        if (numToAssign > 0) {
          List<Integer> nextPartitions =
              getNextPartitions(tgtPartitionAssignments.get(instance), excludeSet, numToAssign);
          for (Integer pId : nextPartitions) {
            String pName = pName(jobResource, pId);
            paMap.put(pId,
                new PartitionAssignment(instance.toString(), TaskPartitionState.RUNNING.name()));
            excludeSet.add(pId);
            jobCtx.setAssignedParticipant(pId, instance.toString());
            jobCtx.setPartitionState(pId, TaskPartitionState.INIT);
            LOG.debug(String.format("Setting task partition %s state to %s on instance %s.", pName,
                TaskPartitionState.RUNNING, instance));
          }
        }
      }
    }

    // Construct a ResourceAssignment object from the map of partition assignments.
    ResourceAssignment ra = new ResourceAssignment(ResourceId.from(jobResource));
    for (Map.Entry<Integer, PartitionAssignment> e : paMap.entrySet()) {
      PartitionAssignment pa = e.getValue();
      ra.addReplicaMap(PartitionId.from(pName(jobResource, e.getKey())),
          ImmutableMap.of(ParticipantId.from(pa._instance), State.from(pa._state)));
    }
    return ra;
  }

  /**
   * Check if a workflow is ready to schedule, and schedule a rebalance if it is not
   * @param workflowCfg the workflow to check
   * @param workflowCtx the current workflow context
   * @param workflowResource the Helix resource associated with the workflow
   * @param jobResource a job from the workflow
   * @param cache the current snapshot of the cluster
   * @return true if ready, false if not ready
   */
  private boolean scheduleIfNotReady(WorkflowConfig workflowCfg, WorkflowContext workflowCtx,
      String workflowResource, String jobResource, Cluster cache) {

    // Ignore non-scheduled workflows
    if (workflowCfg == null || workflowCfg.getScheduleConfig() == null) {
      return true;
    }

    // Figure out when this should be run, and if it's ready, then just run it
    ScheduleConfig scheduleConfig = workflowCfg.getScheduleConfig();
    Date startTime = scheduleConfig.getStartTime();
    long currentTime = new Date().getTime();
    long delayFromStart = startTime.getTime() - currentTime;

    if (delayFromStart <= 0) {
      // Remove any timers that are past-time for this workflow
      Date scheduledTime = SCHEDULED_TIMES.get(workflowResource);
      if (scheduledTime != null && currentTime > scheduledTime.getTime()) {
        SCHEDULED_TIMES.remove(workflowResource);
      }

      // Recurring workflows are just templates that spawn new workflows
      if (scheduleConfig.isRecurring()) {
        // Skip scheduling this workflow if it's not in a start state
        if (!workflowCfg.getTargetState().equals(TargetState.START)) {
          return false;
        }

        // Skip scheduling this workflow again if the previous run (if any) is still active
        String lastScheduled = workflowCtx.getLastScheduledSingleWorkflow();
        if (lastScheduled != null) {
          WorkflowContext lastWorkflowCtx = TaskUtil.getWorkflowContext(_manager, lastScheduled);
          if (lastWorkflowCtx == null
              || lastWorkflowCtx.getFinishTime() == WorkflowContext.UNFINISHED) {
            return false;
          }
        }

        // Figure out how many jumps are needed, thus the time to schedule the next workflow
        // The negative of the delay is the amount of time past the start time
        long period =
            scheduleConfig.getRecurrenceUnit().toMillis(scheduleConfig.getRecurrenceInterval());
        long offsetMultiplier = (-delayFromStart) / period;
        long timeToSchedule = period * offsetMultiplier + startTime.getTime();

        // Now clone the workflow if this clone has not yet been created
        String newWorkflowName =
            workflowResource + "_" + TaskConstants.SCHEDULED + "_" + offsetMultiplier;
        if (lastScheduled == null || !lastScheduled.equals(newWorkflowName)) {
          Workflow clonedWf =
              TaskUtil.cloneWorkflow(_manager, workflowResource, newWorkflowName, new Date(
                  timeToSchedule));
          TaskDriver driver = new TaskDriver(_manager);
          try {
            // Start the cloned workflow
            driver.start(clonedWf);
          } catch (Exception e) {
            LOG.error("Failed to schedule cloned workflow " + newWorkflowName, e);
          }
          // Persist workflow start regardless of success to avoid retrying and failing
          workflowCtx.setLastScheduledSingleWorkflow(newWorkflowName);
          TaskUtil.setWorkflowContext(_manager, workflowResource, workflowCtx);
        }

        // Change the time to trigger the pipeline to that of the next run
        startTime = new Date(timeToSchedule + period);
        delayFromStart = startTime.getTime() - System.currentTimeMillis();
      } else {
        // This is a one-time workflow and is ready
        return true;
      }
    }

    // No need to schedule the same runnable at the same time
    if (SCHEDULED_TIMES.containsKey(workflowResource)
        || SCHEDULED_TIMES.inverse().containsKey(startTime)) {
      return false;
    }

    scheduleRebalance(workflowResource, jobResource, startTime, delayFromStart);
    return false;
  }

  private void scheduleRebalance(String id, String jobResource, Date startTime, long delayFromStart) {
    // No need to schedule the same runnable at the same time
    if (SCHEDULED_TIMES.containsKey(id) || SCHEDULED_TIMES.inverse().containsKey(startTime)) {
      return;
    }

    // For workflows not yet scheduled, schedule them and record it
    RebalanceInvoker rebalanceInvoker = new RebalanceInvoker(_manager, jobResource);
    SCHEDULED_TIMES.put(id, startTime);
    SCHEDULED_EXECUTOR.schedule(rebalanceInvoker, delayFromStart, TimeUnit.MILLISECONDS);
  }

  private void scheduleForNextTask(String jobResource, JobContext ctx, long now) {
    // Clear current entries if they exist and are expired
    long currentTime = now;
    Date scheduledTime = SCHEDULED_TIMES.get(jobResource);
    if (scheduledTime != null && currentTime > scheduledTime.getTime()) {
      SCHEDULED_TIMES.remove(jobResource);
    }

    // Figure out the earliest schedulable time in the future of a non-complete job
    boolean shouldSchedule = false;
    long earliestTime = Long.MAX_VALUE;
    for (int p : ctx.getPartitionSet()) {
      long retryTime = ctx.getNextRetryTime(p);
      TaskPartitionState state = ctx.getPartitionState(p);
      state = (state != null) ? state : TaskPartitionState.INIT;
      Set<TaskPartitionState> errorStates =
          Sets.newHashSet(TaskPartitionState.ERROR, TaskPartitionState.TASK_ERROR,
              TaskPartitionState.TIMED_OUT);
      if (errorStates.contains(state) && retryTime > currentTime && retryTime < earliestTime) {
        earliestTime = retryTime;
        shouldSchedule = true;
      }
    }

    // If any was found, then schedule it
    if (shouldSchedule) {
      long delay = earliestTime - currentTime;
      Date startTime = new Date(earliestTime);
      scheduleRebalance(jobResource, jobResource, startTime, delay);
    }
  }

  /**
   * Checks if the job has completed.
   * @param ctx The rebalancer context.
   * @param allPartitions The set of partitions to check.
   * @param skippedPartitions partitions that failed, but whose failure is acceptable
   * @return true if all task partitions have been marked with status
   *         {@link TaskPartitionState#COMPLETED} in the rebalancer
   *         context, false otherwise.
   */
  private static boolean isJobComplete(JobContext ctx, Set<Integer> allPartitions,
      Set<Integer> skippedPartitions) {
    for (Integer pId : allPartitions) {
      TaskPartitionState state = ctx.getPartitionState(pId);
      if (!skippedPartitions.contains(pId) && state != TaskPartitionState.COMPLETED) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if the workflow has completed.
   * @param ctx Workflow context containing job states
   * @param cfg Workflow config containing set of jobs
   * @return returns true if all tasks are {@link TaskState#COMPLETED}, false otherwise.
   */
  private static boolean isWorkflowComplete(WorkflowContext ctx, WorkflowConfig cfg) {
    if (!cfg.isTerminable()) {
      return false;
    }
    for (String job : cfg.getJobDag().getAllNodes()) {
      if (ctx.getJobState(job) != TaskState.COMPLETED) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if the workflow has been stopped.
   * @param ctx Workflow context containing task states
   * @param cfg Workflow config containing set of tasks
   * @return returns true if all tasks are {@link TaskState#STOPPED}, false otherwise.
   */
  private static boolean isWorkflowStopped(WorkflowContext ctx, WorkflowConfig cfg) {
    for (String job : cfg.getJobDag().getAllNodes()) {
      if (ctx.getJobState(job) != TaskState.STOPPED && ctx.getJobState(job) != null) {
        return false;
      }
    }
    return true;
  }

  private static void markForDeletion(HelixManager mgr, String resourceName) {
    mgr.getConfigAccessor().set(
        TaskUtil.getResourceConfigScope(mgr.getClusterName(), resourceName),
        WorkflowConfig.TARGET_STATE, TargetState.DELETE.name());
  }

  /**
   * Cleans up all Helix state associated with this job, wiping workflow-level information if this
   * is the last remaining job in its workflow, and the workflow is terminable.
   */
  private static void cleanup(HelixManager mgr, final String resourceName, WorkflowConfig cfg,
      String workflowResource) {
    HelixDataAccessor accessor = mgr.getHelixDataAccessor();

    // Remove any DAG references in workflow
    PropertyKey workflowKey = getConfigPropertyKey(accessor, workflowResource);
    DataUpdater<ZNRecord> dagRemover = new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        JobDag jobDag = JobDag.fromJson(currentData.getSimpleField(WorkflowConfig.DAG));
        for (String child : jobDag.getDirectChildren(resourceName)) {
          jobDag.getChildrenToParents().get(child).remove(resourceName);
        }
        for (String parent : jobDag.getDirectParents(resourceName)) {
          jobDag.getParentsToChildren().get(parent).remove(resourceName);
        }
        jobDag.getChildrenToParents().remove(resourceName);
        jobDag.getParentsToChildren().remove(resourceName);
        jobDag.getAllNodes().remove(resourceName);
        try {
          currentData.setSimpleField(WorkflowConfig.DAG, jobDag.toJson());
        } catch (Exception e) {
          LOG.equals("Could not update DAG for job " + resourceName);
        }
        return currentData;
      }
    };
    accessor.getBaseDataAccessor().update(workflowKey.getPath(), dagRemover,
        AccessOption.PERSISTENT);

    // Delete resource configs.
    PropertyKey cfgKey = getConfigPropertyKey(accessor, resourceName);
    if (!accessor.removeProperty(cfgKey)) {
      throw new RuntimeException(
          String
              .format(
                  "Error occurred while trying to clean up job %s. Failed to remove node %s from Helix. Aborting further clean up steps.",
                  resourceName, cfgKey));
    }
    // Delete property store information for this resource.
    String propStoreKey = getRebalancerPropStoreKey(resourceName);
    if (!mgr.getHelixPropertyStore().remove(propStoreKey, AccessOption.PERSISTENT)) {
      throw new RuntimeException(
          String
              .format(
                  "Error occurred while trying to clean up job %s. Failed to remove node %s from Helix. Aborting further clean up steps.",
                  resourceName, propStoreKey));
    }
    // Finally, delete the ideal state itself.
    PropertyKey isKey = getISPropertyKey(accessor, resourceName);
    if (!accessor.removeProperty(isKey)) {
      throw new RuntimeException(String.format(
          "Error occurred while trying to clean up task %s. Failed to remove node %s from Helix.",
          resourceName, isKey));
    }
    LOG.info(String.format("Successfully cleaned up job resource %s.", resourceName));

    boolean lastInWorkflow = true;
    for (String job : cfg.getJobDag().getAllNodes()) {
      // check if property store information or resource configs exist for this job
      if (mgr.getHelixPropertyStore().exists(getRebalancerPropStoreKey(job),
          AccessOption.PERSISTENT)
          || accessor.getProperty(getConfigPropertyKey(accessor, job)) != null
          || accessor.getProperty(getISPropertyKey(accessor, job)) != null) {
        lastInWorkflow = false;
      }
    }

    // clean up workflow-level info if this was the last in workflow
    if (lastInWorkflow && cfg.isTerminable()) {
      // delete workflow config
      PropertyKey workflowCfgKey = getConfigPropertyKey(accessor, workflowResource);
      if (!accessor.removeProperty(workflowCfgKey)) {
        throw new RuntimeException(
            String
                .format(
                    "Error occurred while trying to clean up workflow %s. Failed to remove node %s from Helix. Aborting further clean up steps.",
                    workflowResource, workflowCfgKey));
      }
      // Delete property store information for this workflow
      String workflowPropStoreKey = getRebalancerPropStoreKey(workflowResource);
      if (!mgr.getHelixPropertyStore().remove(workflowPropStoreKey, AccessOption.PERSISTENT)) {
        throw new RuntimeException(
            String
                .format(
                    "Error occurred while trying to clean up workflow %s. Failed to remove node %s from Helix. Aborting further clean up steps.",
                    workflowResource, workflowPropStoreKey));
      }
    }

  }

  private static String getRebalancerPropStoreKey(String resource) {
    return Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, resource);
  }

  private static PropertyKey getISPropertyKey(HelixDataAccessor accessor, String resource) {
    return accessor.keyBuilder().idealStates(resource);
  }

  private static PropertyKey getConfigPropertyKey(HelixDataAccessor accessor, String resource) {
    return accessor.keyBuilder().resourceConfig(resource);
  }

  private static void addAllPartitions(Set<Integer> toAdd, Set<Integer> destination) {
    for (Integer pId : toAdd) {
      destination.add(pId);
    }
  }

  private static ResourceAssignment emptyAssignment(String name,
      ResourceCurrentState currStateOutput) {
    ResourceId resourceId = ResourceId.from(name);
    ResourceAssignment assignment = new ResourceAssignment(resourceId);
    Set<PartitionId> partitions = currStateOutput.getCurrentStateMappedPartitions(resourceId);
    for (PartitionId partition : partitions) {
      Map<ParticipantId, State> currentStateMap =
          currStateOutput.getCurrentStateMap(resourceId, partition);
      Map<ParticipantId, State> replicaMap = Maps.newHashMap();
      for (ParticipantId participantId : currentStateMap.keySet()) {
        replicaMap.put(participantId, State.from(HelixDefinedState.DROPPED));
      }
      assignment.addReplicaMap(partition, replicaMap);
    }
    return assignment;
  }

  private static void addCompletedPartitions(Set<Integer> set, JobContext ctx,
      Iterable<Integer> pIds) {
    for (Integer pId : pIds) {
      TaskPartitionState state = ctx.getPartitionState(pId);
      if (state == TaskPartitionState.COMPLETED) {
        set.add(pId);
      }
    }
  }

  private static List<Integer> getNextPartitions(SortedSet<Integer> candidatePartitions,
      Set<Integer> excluded, int n) {
    List<Integer> result = new ArrayList<Integer>();
    for (Integer pId : candidatePartitions) {
      if (result.size() >= n) {
        break;
      }

      if (!excluded.contains(pId)) {
        result.add(pId);
      }
    }

    return result;
  }

  private static void markPartitionDelayed(JobConfig cfg, JobContext ctx, int p) {
    long delayInterval = cfg.getTaskRetryDelay();
    if (delayInterval <= 0) {
      return;
    }
    long nextStartTime = ctx.getPartitionFinishTime(p) + delayInterval;
    ctx.setNextRetryTime(p, nextStartTime);
  }

  private static void markPartitionCompleted(JobContext ctx, int pId) {
    ctx.setPartitionState(pId, TaskPartitionState.COMPLETED);
    ctx.setPartitionFinishTime(pId, System.currentTimeMillis());
    ctx.incrementNumAttempts(pId);
  }

  private static void markPartitionError(JobContext ctx, int pId, TaskPartitionState state,
      boolean incrementAttempts) {
    ctx.setPartitionState(pId, state);
    ctx.setPartitionFinishTime(pId, System.currentTimeMillis());
    if (incrementAttempts) {
      ctx.incrementNumAttempts(pId);
    }
  }

  private static void markAllPartitionsError(JobContext ctx, TaskPartitionState state,
      boolean incrementAttempts) {
    for (int pId : ctx.getPartitionSet()) {
      markPartitionError(ctx, pId, state, incrementAttempts);
    }
  }

  /**
   * Return the assignment of task partitions per instance.
   */
  private static Map<ParticipantId, SortedSet<Integer>> getTaskPartitionAssignments(
      Iterable<ParticipantId> instanceList, ResourceAssignment assignment, Set<Integer> includeSet) {
    Map<ParticipantId, SortedSet<Integer>> result =
        new HashMap<ParticipantId, SortedSet<Integer>>();
    for (ParticipantId instance : instanceList) {
      result.put(instance, new TreeSet<Integer>());
    }

    for (PartitionId partition : assignment.getMappedPartitionIds()) {
      int pId = pId(partition.toString());
      if (includeSet.contains(pId)) {
        Map<ParticipantId, State> replicaMap = assignment.getReplicaMap(partition);
        for (ParticipantId instance : replicaMap.keySet()) {
          SortedSet<Integer> pList = result.get(instance);
          if (pList != null) {
            pList.add(pId);
          }
        }
      }
    }
    return result;
  }

  private static Set<Integer> getNonReadyPartitions(JobContext ctx, long now) {
    Set<Integer> nonReadyPartitions = Sets.newHashSet();
    for (int p : ctx.getPartitionSet()) {
      long toStart = ctx.getNextRetryTime(p);
      if (now < toStart) {
        nonReadyPartitions.add(p);
      }
    }
    return nonReadyPartitions;
  }

  /**
   * Computes the partition name given the resource name and partition id.
   */
  protected static String pName(String resource, int pId) {
    return resource + "_" + pId;
  }

  /**
   * Extracts the partition id from the given partition name.
   */
  protected static int pId(String pName) {
    String[] tokens = pName.split("_");
    return Integer.valueOf(tokens[tokens.length - 1]);
  }

  /**
   * An (instance, state) pair.
   */
  private static class PartitionAssignment {
    private final String _instance;
    private final String _state;

    private PartitionAssignment(String instance, String state) {
      _instance = instance;
      _state = state;
    }
  }

  /**
   * The simplest possible runnable that will trigger a run of the controller pipeline
   */
  private static class RebalanceInvoker implements Runnable {
    private final HelixManager _manager;
    private final String _resource;

    public RebalanceInvoker(HelixManager manager, String resource) {
      _manager = manager;
      _resource = resource;
    }

    @Override
    public void run() {
      TaskUtil.invokeRebalance(_manager, _resource);
    }
  }
}
