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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

/**
 * Custom rebalancer implementation for the {@code Job} in task model.
 */
public class JobRebalancer extends TaskRebalancer {
  private static final Logger LOG = Logger.getLogger(JobRebalancer.class);
  private static TaskAssignmentCalculator fixTaskAssignmentCal =
      new FixedTargetTaskAssignmentCalculator();
  private static TaskAssignmentCalculator genericTaskAssignmentCal =
      new GenericTaskAssignmentCalculator();

  private static final String PREV_RA_NODE = "PreviousResourceAssignment";

  @Override
  public ResourceAssignment computeBestPossiblePartitionState(ClusterDataCache clusterData,
      IdealState taskIs, Resource resource, CurrentStateOutput currStateOutput) {
    final String jobName = resource.getResourceName();
    LOG.debug("Computer Best Partition for job: " + jobName);

    // Fetch job configuration
    JobConfig jobCfg = TaskUtil.getJobConfig(_manager, jobName);
    if (jobCfg == null) {
      LOG.error("Job configuration is NULL for " + jobName);
      return buildEmptyAssignment(jobName, currStateOutput);
    }
    String workflowResource = jobCfg.getWorkflow();

    // Fetch workflow configuration and context
    WorkflowConfig workflowCfg = TaskUtil.getWorkflowConfig(_manager, workflowResource);
    if (workflowCfg == null) {
      LOG.error("Workflow configuration is NULL for " + jobName);
      return buildEmptyAssignment(jobName, currStateOutput);
    }

    WorkflowContext workflowCtx = TaskUtil.getWorkflowContext(_manager, workflowResource);
    if (workflowCtx == null) {
      LOG.error("Workflow context is NULL for " + jobName);
      return buildEmptyAssignment(jobName, currStateOutput);
    }

    TargetState targetState = workflowCfg.getTargetState();
    if (targetState != TargetState.START && targetState != TargetState.STOP) {
      LOG.info("Target state is " + targetState.name() + " for workflow " + workflowResource
          + ".Stop scheduling job " + jobName);
      return buildEmptyAssignment(jobName, currStateOutput);
    }

    // Stop current run of the job if workflow or job is already in final state (failed or completed)
    TaskState workflowState = workflowCtx.getWorkflowState();
    TaskState jobState = workflowCtx.getJobState(jobName);
    // The job is already in a final state (completed/failed).
    if (workflowState == TaskState.FAILED || workflowState == TaskState.COMPLETED ||
        jobState == TaskState.FAILED || jobState == TaskState.COMPLETED) {
      LOG.info(String.format(
          "Workflow %s or job %s is already failed or completed, workflow state (%s), job state (%s), clean up job IS.",
          workflowResource, jobName, workflowState, jobState));
      TaskUtil.cleanupJobIdealStateExtView(_manager.getHelixDataAccessor(), jobName);
      _scheduledRebalancer.removeScheduledRebalance(jobName);
      return buildEmptyAssignment(jobName, currStateOutput);
    }

    if (!isWorkflowReadyForSchedule(workflowCfg)) {
      LOG.info("Job is not ready to be run since workflow is not ready " + jobName);
      return buildEmptyAssignment(jobName, currStateOutput);
    }

    if (!isJobStarted(jobName, workflowCtx) && !isJobReadyToSchedule(jobName, workflowCfg,
        workflowCtx)) {
      LOG.info("Job is not ready to run " + jobName);
      return buildEmptyAssignment(jobName, currStateOutput);
    }

    // Fetch any existing context information from the property store.
    JobContext jobCtx = TaskUtil.getJobContext(_manager, jobName);
    if (jobCtx == null) {
      jobCtx = new JobContext(new ZNRecord(TaskUtil.TASK_CONTEXT_KW));
      jobCtx.setStartTime(System.currentTimeMillis());
      workflowCtx.setJobState(jobName, TaskState.IN_PROGRESS);
    }

    scheduleRebalanceForJobTimeout(jobCfg, jobCtx);

    // Grab the old assignment, or an empty one if it doesn't exist
    ResourceAssignment prevAssignment = getPrevResourceAssignment(jobName);
    if (prevAssignment == null) {
      prevAssignment = new ResourceAssignment(jobName);
    }

    // Will contain the list of partitions that must be explicitly dropped from the ideal state that
    // is stored in zk.
    // Fetch the previous resource assignment from the property store. This is required because of
    // HELIX-230.
    Set<String> liveInstances = jobCfg.getInstanceGroupTag() == null
        ? clusterData.getEnabledLiveInstances()
        : clusterData.getEnabledLiveInstancesWithTag(jobCfg.getInstanceGroupTag());

    if (liveInstances.isEmpty()) {
      LOG.error("No available instance found for job!");
    }

    Set<Integer> partitionsToDrop = new TreeSet<Integer>();
    ResourceAssignment newAssignment =
        computeResourceMapping(jobName, workflowCfg, jobCfg, prevAssignment, liveInstances,
            currStateOutput, workflowCtx, jobCtx, partitionsToDrop, clusterData);

    if (!partitionsToDrop.isEmpty()) {
      for (Integer pId : partitionsToDrop) {
        taskIs.getRecord().getMapFields().remove(pName(jobName, pId));
      }
      HelixDataAccessor accessor = _manager.getHelixDataAccessor();
      PropertyKey propertyKey = accessor.keyBuilder().idealStates(jobName);
      accessor.setProperty(propertyKey, taskIs);
    }

    // Update rebalancer context, previous ideal state.
    TaskUtil.setJobContext(_manager, jobName, jobCtx);
    TaskUtil.setWorkflowContext(_manager, workflowResource, workflowCtx);
    setPrevResourceAssignment(jobName, newAssignment);

    LOG.debug("Job " + jobName + " new assignment " + Arrays
        .toString(newAssignment.getMappedPartitions().toArray()));
    return newAssignment;
  }

  private Set<String> getInstancesAssignedToOtherJobs(String currentJobName,
      WorkflowConfig workflowCfg) {
    Set<String> ret = new HashSet<String>();
    for (String jobName : workflowCfg.getJobDag().getAllNodes()) {
      if (jobName.equals(currentJobName)) {
        continue;
      }
      JobContext jobContext = TaskUtil.getJobContext(_manager, jobName);
      if (jobContext == null) {
        continue;
      }
      for (int pId : jobContext.getPartitionSet()) {
        TaskPartitionState partitionState = jobContext.getPartitionState(pId);
        if (partitionState == TaskPartitionState.INIT || partitionState == TaskPartitionState.RUNNING) {
          ret.add(jobContext.getAssignedParticipant(pId));
        }
      }
    }

    return ret;
  }

  private ResourceAssignment computeResourceMapping(String jobResource,
      WorkflowConfig workflowConfig, JobConfig jobCfg, ResourceAssignment prevAssignment,
      Collection<String> liveInstances, CurrentStateOutput currStateOutput,
      WorkflowContext workflowCtx, JobContext jobCtx, Set<Integer> partitionsToDropFromIs,
      ClusterDataCache cache) {
    TargetState jobTgtState = workflowConfig.getTargetState();
    TaskState jobState = workflowCtx.getJobState(jobResource);

    if (jobState == TaskState.IN_PROGRESS && isJobTimeout(jobCtx, jobCfg)) {
      jobState = TaskState.TIMING_OUT;
      workflowCtx.setJobState(jobResource, TaskState.TIMING_OUT);
    } else if (jobState != TaskState.TIMED_OUT && jobState != TaskState.TIMING_OUT) {
      // TIMED_OUT and TIMING_OUT job can't be stopped, because all tasks are being aborted
      // Update running status in workflow context
      if (jobTgtState == TargetState.STOP) {
        if (checkJobStopped(jobCtx)) {
          workflowCtx.setJobState(jobResource, TaskState.STOPPED);
        } else {
          workflowCtx.setJobState(jobResource, TaskState.STOPPING);
        }
        // Workflow has been stopped if all in progress jobs are stopped
        if (isWorkflowStopped(workflowCtx, workflowConfig)) {
          workflowCtx.setWorkflowState(TaskState.STOPPED);
        } else {
          workflowCtx.setWorkflowState(TaskState.STOPPING);
        }
      } else {
        workflowCtx.setJobState(jobResource, TaskState.IN_PROGRESS);
        // Workflow is in progress if any task is in progress
        workflowCtx.setWorkflowState(TaskState.IN_PROGRESS);
      }
    }

    // Used to keep track of tasks that have already been assigned to instances.
    Set<Integer> assignedPartitions = new HashSet<Integer>();

    // Used to keep track of tasks that have failed, but whose failure is acceptable
    Set<Integer> skippedPartitions = new HashSet<Integer>();

    // Keeps a mapping of (partition) -> (instance, state)
    Map<Integer, PartitionAssignment> paMap = new TreeMap<Integer, PartitionAssignment>();

    Set<String> excludedInstances = getInstancesAssignedToOtherJobs(jobResource, workflowConfig);

    // Process all the current assignments of tasks.
    TaskAssignmentCalculator taskAssignmentCal = getAssignmentCalulator(jobCfg);
    Set<Integer> allPartitions = taskAssignmentCal
        .getAllTaskPartitions(jobCfg, jobCtx, workflowConfig, workflowCtx, cache.getIdealStates());

    if (allPartitions == null || allPartitions.isEmpty()) {
      // Empty target partitions, mark the job as FAILED.
      String failureMsg = "Empty task partition mapping for job " + jobResource + ", marked the job as FAILED!";
      LOG.info(failureMsg);
      jobCtx.setInfo(failureMsg);
      markJobFailed(jobResource, jobCtx, workflowConfig, workflowCtx);
      markAllPartitionsError(jobCtx, TaskPartitionState.ERROR, false);
      _clusterStatusMonitor.updateJobCounters(jobCfg, TaskState.FAILED);
      return new ResourceAssignment(jobResource);
    }

    Map<String, SortedSet<Integer>> taskAssignments =
        getTaskPartitionAssignments(liveInstances, prevAssignment, allPartitions);
    long currentTime = System.currentTimeMillis();

    LOG.debug("All partitions: " + allPartitions + " taskAssignment: " + taskAssignments
        + " excludedInstances: " + excludedInstances);

    // Iterate through all instances
    for (String instance : taskAssignments.keySet()) {
      if (excludedInstances.contains(instance)) {
        continue;
      }

      Set<Integer> pSet = taskAssignments.get(instance);
      // Used to keep track of partitions that are in one of the final states: COMPLETED, TIMED_OUT,
      // TASK_ERROR, ERROR.
      Set<Integer> donePartitions = new TreeSet<Integer>();
      for (int pId : pSet) {
        final String pName = pName(jobResource, pId);
        TaskPartitionState currState =
            updateJobContextAndGetTaskCurrentState(currStateOutput, jobResource, pId, pName, instance, jobCtx);

        // Check for pending state transitions on this (partition, instance).
        Message pendingMessage =
            currStateOutput.getPendingState(jobResource, new Partition(pName), instance);
        if (pendingMessage != null) {
          processTaskWithPendingMessage(prevAssignment, pId, pName, instance, pendingMessage, jobState, currState,
              paMap, assignedPartitions);
          continue;
        }

        // Process any requested state transitions.
        String requestedStateStr =
            currStateOutput.getRequestedState(jobResource, new Partition(pName), instance);
        if (requestedStateStr != null && !requestedStateStr.isEmpty()) {
          TaskPartitionState requestedState = TaskPartitionState.valueOf(requestedStateStr);
          if (requestedState.equals(currState)) {
            LOG.warn(String.format(
                "Requested state %s is the same as the current state for instance %s.",
                requestedState, instance));
          }

          paMap.put(pId, new PartitionAssignment(instance, requestedState.name()));
          assignedPartitions.add(pId);
          LOG.debug(String.format(
              "Instance %s requested a state transition to %s for partition %s.", instance,
              requestedState, pName));
          continue;
        }

        switch (currState) {
        case RUNNING: {
          TaskPartitionState nextState = TaskPartitionState.RUNNING;
          if (jobState == TaskState.TIMING_OUT) {
            nextState = TaskPartitionState.TASK_ABORTED;
          } else if (jobTgtState == TargetState.STOP) {
            nextState = TaskPartitionState.STOPPED;
          }

          paMap.put(pId, new PartitionAssignment(instance, nextState.name()));
          assignedPartitions.add(pId);
          LOG.debug(String.format("Setting task partition %s state to %s on instance %s.", pName,
              nextState, instance));
        }
          break;
        case STOPPED: {
          TaskPartitionState nextState;
          if (jobTgtState == TargetState.START) {
            nextState = TaskPartitionState.RUNNING;
          } else {
            nextState = TaskPartitionState.STOPPED;
          }

          paMap.put(pId, new PartitionAssignment(instance, nextState.name()));
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
        case TASK_ABORTED:
        case ERROR: {
          donePartitions.add(pId); // The task may be rescheduled on a different instance.
          LOG.debug(String.format(
              "Task partition %s has error state %s with msg %s. Marking as such in rebalancer context.", pName,
              currState, jobCtx.getPartitionInfo(pId)));
          markPartitionError(jobCtx, pId, currState, true);
          // The error policy is to fail the task as soon a single partition fails for a specified
          // maximum number of attempts or task is in ABORTED state.
          // But notice that if job is TIMED_OUT, aborted task won't be treated as fail and won't cause job fail.
          // After all tasks are aborted, they will be dropped, because of job timeout.
          if (jobState != TaskState.TIMED_OUT && jobState != TaskState.TIMING_OUT) {
            if (jobCtx.getPartitionNumAttempts(pId) >= jobCfg.getMaxAttemptsPerTask()
                || currState.equals(TaskPartitionState.TASK_ABORTED)
                || currState.equals(TaskPartitionState.ERROR)) {
              // If we have some leeway for how many tasks we can fail, then we don't have
              // to fail the job immediately
              if (skippedPartitions.size() >= jobCfg.getFailureThreshold()) {
                markJobFailed(jobResource, jobCtx, workflowConfig, workflowCtx);
                _clusterStatusMonitor.updateJobCounters(jobCfg, TaskState.FAILED);
                markAllPartitionsError(jobCtx, currState, false);
                addAllPartitions(allPartitions, partitionsToDropFromIs);

                // remove IdealState of this job
                TaskUtil.cleanupJobIdealStateExtView(_manager.getHelixDataAccessor(), jobResource);
                return buildEmptyAssignment(jobResource, currStateOutput);
              } else {
                skippedPartitions.add(pId);
                partitionsToDropFromIs.add(pId);
              }

              LOG.debug("skippedPartitions:" + skippedPartitions);
            } else {
              // Mark the task to be started at some later time (if enabled)
              markPartitionDelayed(jobCfg, jobCtx, pId);
            }
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

    if (isJobComplete(jobCtx, allPartitions, jobCfg)) {
      markJobComplete(jobResource, jobCtx, workflowConfig, workflowCtx);
      _clusterStatusMonitor.updateJobCounters(jobCfg, TaskState.COMPLETED);
      _scheduledRebalancer.removeScheduledRebalance(jobResource);
      TaskUtil.cleanupJobIdealStateExtView(_manager.getHelixDataAccessor(), jobResource);
      return buildEmptyAssignment(jobResource, currStateOutput);
    }

    // If job is being timed out and no task is running (for whatever reason), idealState can be deleted and all tasks
    // can be dropped(note that Helix doesn't track whether the drop is success or not).
    if (jobState == TaskState.TIMING_OUT
        && isJobFinishTimingOut(jobCtx, jobResource, currStateOutput)) {
      jobCtx.setFinishTime(System.currentTimeMillis());
      workflowCtx.setJobState(jobResource, TaskState.TIMED_OUT);
      // Mark all INIT task to TASK_ABORTED
      for (int pId : jobCtx.getPartitionSet()) {
        if (jobCtx.getPartitionState(pId) == TaskPartitionState.INIT) {
          jobCtx.setPartitionState(pId, TaskPartitionState.TASK_ABORTED);
        }
      }
      _clusterStatusMonitor.updateJobCounters(jobCfg, TaskState.TIMED_OUT);
      _scheduledRebalancer.removeScheduledRebalance(jobResource);
      TaskUtil.cleanupJobIdealStateExtView(_manager.getHelixDataAccessor(), jobResource);
      return buildEmptyAssignment(jobResource, currStateOutput);
    }

    // Make additional task assignments if needed.
    if (jobState != TaskState.TIMING_OUT && jobState != TaskState.TIMED_OUT && jobTgtState == TargetState.START) {
      // Contains the set of task partitions that must be excluded from consideration when making
      // any new assignments.
      // This includes all completed, failed, delayed, and already assigned partitions.
      Set<Integer> excludeSet = Sets.newTreeSet(assignedPartitions);
      addCompletedTasks(excludeSet, jobCtx, allPartitions);
      addGiveupPartitions(excludeSet, jobCtx, allPartitions, jobCfg);
      excludeSet.addAll(skippedPartitions);
      excludeSet.addAll(getNonReadyPartitions(jobCtx, currentTime));
      // Get instance->[partition, ...] mappings for the target resource.
      Map<String, SortedSet<Integer>> tgtPartitionAssignments = taskAssignmentCal
          .getTaskAssignment(currStateOutput, prevAssignment, liveInstances, jobCfg, jobCtx,
              workflowConfig, workflowCtx, allPartitions, cache.getIdealStates());
      for (Map.Entry<String, SortedSet<Integer>> entry : taskAssignments.entrySet()) {
        String instance = entry.getKey();
        if (!tgtPartitionAssignments.containsKey(instance) || excludedInstances
            .contains(instance)) {
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
            paMap.put(pId, new PartitionAssignment(instance, TaskPartitionState.RUNNING.name()));
            excludeSet.add(pId);
            jobCtx.setAssignedParticipant(pId, instance);
            jobCtx.setPartitionState(pId, TaskPartitionState.INIT);
            jobCtx.setPartitionStartTime(pId, System.currentTimeMillis());
            LOG.debug(String.format("Setting task partition %s state to %s on instance %s.", pName,
                TaskPartitionState.RUNNING, instance));
          }
        }
      }
    }

    // Construct a ResourceAssignment object from the map of partition assignments.
    ResourceAssignment ra = new ResourceAssignment(jobResource);
    for (Map.Entry<Integer, PartitionAssignment> e : paMap.entrySet()) {
      PartitionAssignment pa = e.getValue();
      ra.addReplicaMap(new Partition(pName(jobResource, e.getKey())),
          ImmutableMap.of(pa._instance, pa._state));
    }

    return ra;
  }

  private TaskPartitionState updateJobContextAndGetTaskCurrentState(CurrentStateOutput currentStateOutput,
      String jobResource, Integer pId, String pName, String instance, JobContext jobCtx) {
    String currentStateString = currentStateOutput.getCurrentState(jobResource, new Partition(
        pName), instance);
    if (currentStateString == null) {
      // If the task is assigned but current state is null, it is equivalent to say its current state is INIT
      return TaskPartitionState.INIT;
    }
    TaskPartitionState currentState = TaskPartitionState.valueOf(currentStateString);
    jobCtx.setPartitionState(pId, currentState);
    String taskMsg = currentStateOutput.getInfo(jobResource, new Partition(
        pName), instance);
    if (taskMsg != null) {
      jobCtx.setPartitionInfo(pId, taskMsg);
    }
    return currentState;
  }

  private void processTaskWithPendingMessage(ResourceAssignment prevAssignment, Integer pId, String pName,
      String instance, Message pendingMessage, TaskState jobState, TaskPartitionState currState,
      Map<Integer, PartitionAssignment> paMap, Set<Integer> assignedPartitions) {

    Map<String, String> stateMap = prevAssignment.getReplicaMap(new Partition(pName));
    if (stateMap != null) {
      String prevState = stateMap.get(instance);
      if (!pendingMessage.getToState().equals(prevState)) {
        LOG.warn(String.format("Task pending to-state is %s while previous assigned state is %s. This should not"
            + "heppen.", pendingMessage.getToState(), prevState));
      }
      if (jobState == TaskState.TIMING_OUT
          && currState == TaskPartitionState.INIT
          && prevState.equals(TaskPartitionState.RUNNING.name())) {
        // While job is timing out, if the task is pending on INIT->RUNNING, set it back to INIT,
        // so that Helix will cancel the transition.
        paMap.put(pId, new PartitionAssignment(instance, TaskPartitionState.INIT.name()));
        assignedPartitions.add(pId);
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format(
              "Task partition %s has a pending state transition on instance %s INIT->RUNNING. "
                  + "Setting it back to INIT so that Helix can cancel the transition(if enabled).",
              pName, instance, prevState));
        }
      } else {
        // Otherwise, Just copy forward
        // the state assignment from the previous ideal state.
        paMap.put(pId, new PartitionAssignment(instance, prevState));
        assignedPartitions.add(pId);
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format(
              "Task partition %s has a pending state transition on instance %s. Using the previous ideal state which was %s.",
              pName, instance, prevState));
        }
      }
    }
  }

  private boolean isJobTimeout(JobContext jobContext, JobConfig jobConfig) {
    long jobTimeoutTime = computeJobTimeoutTime(jobContext, jobConfig);
    return jobTimeoutTime != jobConfig.DEFAULT_TIMEOUT_NEVER && jobTimeoutTime <= System.currentTimeMillis();
  }

  private boolean isJobFinishTimingOut(JobContext jobContext, String jobResource,
      CurrentStateOutput currentStateOutput) {
    for (int pId : jobContext.getPartitionSet()) {
      TaskPartitionState state = jobContext.getPartitionState(pId);
      Partition partition = new Partition(pName(jobResource, pId));
      String instance = jobContext.getAssignedParticipant(pId);
      Message pendingMessage = currentStateOutput.getPendingState(jobResource, partition, instance);
      // If state is INIT but is pending INIT->RUNNING, it's not yet safe to mark job as TIMEDOUT and drop the task
      if (state == TaskPartitionState.RUNNING || (state == TaskPartitionState.INIT && pendingMessage != null)) {
        return false;
      }
    }
    return true;
  }

  // Return jobConfig.DEFAULT_TIMEOUT_NEVER if job should never timeout.
  // job start time can't be -1 before calling this method.
  private long computeJobTimeoutTime(JobContext jobContext, JobConfig jobConfig) {
    return (jobConfig.getTimeout() == JobConfig.DEFAULT_TIMEOUT_NEVER
        || jobConfig.getTimeout() > Long.MAX_VALUE - jobContext.getStartTime()) // check long overflow
        ? jobConfig.DEFAULT_TIMEOUT_NEVER
        : jobContext.getStartTime() + jobConfig.getTimeout();
  }

  private void markJobComplete(String jobName, JobContext jobContext,
      WorkflowConfig workflowConfig, WorkflowContext workflowContext) {
    long currentTime = System.currentTimeMillis();
    workflowContext.setJobState(jobName, TaskState.COMPLETED);
    jobContext.setFinishTime(currentTime);
    if (isWorkflowFinished(workflowContext, workflowConfig)) {
      workflowContext.setFinishTime(currentTime);
    }
    scheduleJobCleanUp(jobName, workflowConfig, currentTime);
  }

  private void scheduleForNextTask(String job, JobContext jobCtx, long now) {
    // Figure out the earliest schedulable time in the future of a non-complete job
    boolean shouldSchedule = false;
    long earliestTime = Long.MAX_VALUE;
    for (int p : jobCtx.getPartitionSet()) {
      long retryTime = jobCtx.getNextRetryTime(p);
      TaskPartitionState state = jobCtx.getPartitionState(p);
      state = (state != null) ? state : TaskPartitionState.INIT;
      Set<TaskPartitionState> errorStates =
          Sets.newHashSet(TaskPartitionState.ERROR, TaskPartitionState.TASK_ERROR,
              TaskPartitionState.TIMED_OUT);
      if (errorStates.contains(state) && retryTime > now && retryTime < earliestTime) {
        earliestTime = retryTime;
        shouldSchedule = true;
      }
    }

    // If any was found, then schedule it
    if (shouldSchedule) {
      long scheduledTime = _scheduledRebalancer.getRebalanceTime(job);
      if (scheduledTime == -1 || earliestTime < scheduledTime) {
        _scheduledRebalancer.scheduleRebalance(_manager, job, earliestTime);
      }
    }
  }

  // Set job timeout rebalance, if the time is earlier than the current scheduled rebalance time
  // This needs to run for every rebalance because the scheduled rebalance could be removed in other places.
  private void scheduleRebalanceForJobTimeout(JobConfig jobCfg, JobContext jobCtx) {
    long jobTimeoutTime = computeJobTimeoutTime(jobCtx, jobCfg);
    if (jobTimeoutTime != JobConfig.DEFAULT_TIMEOUT_NEVER && jobTimeoutTime > System.currentTimeMillis()) {
      long nextRebalanceTime = _scheduledRebalancer.getRebalanceTime(jobCfg.getJobId());
      if (nextRebalanceTime == JobConfig.DEFAULT_TIMEOUT_NEVER || jobTimeoutTime < nextRebalanceTime) {
        _scheduledRebalancer.scheduleRebalance(_manager, jobCfg.getJobId(), jobTimeoutTime);
      }
    }
  }

  /**
   * Get the last task assignment for a given job
   *
   * @param resourceName the name of the job
   * @return {@link ResourceAssignment} instance, or null if no assignment is available
   */
  private ResourceAssignment getPrevResourceAssignment(String resourceName) {
    ZNRecord r = _manager.getHelixPropertyStore()
        .get(Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, resourceName, PREV_RA_NODE),
            null, AccessOption.PERSISTENT);
    return r != null ? new ResourceAssignment(r) : null;
  }

  /**
   * Set the last task assignment for a given job
   *
   * @param resourceName the name of the job
   * @param ra           {@link ResourceAssignment} containing the task assignment
   */
  private void setPrevResourceAssignment(String resourceName,
      ResourceAssignment ra) {
    _manager.getHelixPropertyStore()
        .set(Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, resourceName, PREV_RA_NODE),
            ra.getRecord(), AccessOption.PERSISTENT);
  }

  /**
   * Checks if the job has completed.
   * Look at states of all tasks of the job, there're 3 kind: completed, given up, not given up.
   * The job is completed if all tasks are completed or given up, and the number of given up tasks is within job
   * failure threshold.
   */
  private static boolean isJobComplete(JobContext ctx, Set<Integer> allPartitions, JobConfig cfg) {
    int numOfGivenUpTasks = 0;
    // Iterate through all tasks, if any one indicates the job has not completed, return false.
    for (Integer pId : allPartitions) {
      TaskPartitionState state = ctx.getPartitionState(pId);
      if (state != TaskPartitionState.COMPLETED) {
        if(!isTaskGivenup(ctx, cfg, pId)) {
          return false;
        }
        // If the task is given up, there's still chance the job has completed because of job failure threshold.
        numOfGivenUpTasks++;
      }
    }
    return numOfGivenUpTasks <= cfg.getFailureThreshold();
  }

  private static void addAllPartitions(Set<Integer> toAdd, Set<Integer> destination) {
    for (Integer pId : toAdd) {
      destination.add(pId);
    }
  }

  private static void addCompletedTasks(Set<Integer> set, JobContext ctx,
      Iterable<Integer> pIds) {
    for (Integer pId : pIds) {
      TaskPartitionState state = ctx.getPartitionState(pId);
      if (state == TaskPartitionState.COMPLETED) {
        set.add(pId);
      }
    }
  }

  private static boolean isTaskGivenup(JobContext ctx, JobConfig cfg, int pId) {
    TaskPartitionState state = ctx.getPartitionState(pId);
    if (state != null
        && (state.equals(TaskPartitionState.TASK_ABORTED) || state.equals(TaskPartitionState.ERROR))) {
      return true;
    }
    return ctx.getPartitionNumAttempts(pId) >= cfg.getMaxAttemptsPerTask();
  }

  // add all partitions that have been tried maxNumberAttempts
  private static void addGiveupPartitions(Set<Integer> set, JobContext ctx, Iterable<Integer> pIds,
      JobConfig cfg) {
    for (Integer pId : pIds) {
      if (isTaskGivenup(ctx, cfg, pId)) {
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
  private static Map<String, SortedSet<Integer>> getTaskPartitionAssignments(
      Iterable<String> instanceList, ResourceAssignment assignment, Set<Integer> includeSet) {
    Map<String, SortedSet<Integer>> result = new HashMap<String, SortedSet<Integer>>();
    for (String instance : instanceList) {
      result.put(instance, new TreeSet<Integer>());
    }

    for (Partition partition : assignment.getMappedPartitions()) {
      int pId = getPartitionId(partition.getPartitionName());
      if (includeSet.contains(pId)) {
        Map<String, String> replicaMap = assignment.getReplicaMap(partition);
        for (String instance : replicaMap.keySet()) {
          SortedSet<Integer> pList = result.get(instance);
          if (pList != null) {
            pList.add(pId);
          }
        }
      }
    }
    return result;
  }

  /* Extracts the partition id from the given partition name. */
  private static int getPartitionId(String pName) {
    int index = pName.lastIndexOf("_");
    if (index == -1) {
      throw new HelixException("Invalid partition name " + pName);
    }
    return Integer.valueOf(pName.substring(index + 1));
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

  private TaskAssignmentCalculator getAssignmentCalulator(JobConfig jobConfig) {
    Map<String, TaskConfig> taskConfigMap = jobConfig.getTaskConfigMap();
    if (taskConfigMap != null && !taskConfigMap.isEmpty()) {
      return genericTaskAssignmentCal;
    } else {
      return fixTaskAssignmentCal;
    }
  }

  /**
   * Check whether tasks are not in final states
   * @param jobContext The job context
   * @return           False if still tasks not in final state. Otherwise return true
   */
  private boolean checkJobStopped(JobContext jobContext) {
    for (int partition : jobContext.getPartitionSet()) {
      TaskPartitionState taskState = jobContext.getPartitionState(partition);
      if (taskState != null && taskState.equals(TaskPartitionState.RUNNING)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Computes the partition name given the resource name and partition id.
   */
  private String pName(String resource, int pId) {
    return resource + "_" + pId;
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
}
