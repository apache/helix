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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
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

import com.google.common.base.Joiner;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.controller.rebalancer.util.RebalanceScheduler;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom rebalancer implementation for the {@code Task} state model.
 */
/** This rebalancer is deprecated, left here only for back-compatible. **/
@Deprecated
public abstract class DeprecatedTaskRebalancer
    implements Rebalancer<WorkflowControllerDataProvider>,
    MappingCalculator<WorkflowControllerDataProvider> {
  private static final Logger LOG = LoggerFactory.getLogger(TaskRebalancer.class);

  // Management of already-scheduled rebalances across jobs
  private static final BiMap<String, Date> SCHEDULED_TIMES = HashBiMap.create();
  private static final ScheduledExecutorService SCHEDULED_EXECUTOR = Executors
      .newSingleThreadScheduledExecutor();
  public static final String PREV_RA_NODE = "PreviousResourceAssignment";

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
      WorkflowConfig workflowCfg, WorkflowContext workflowCtx, WorkflowControllerDataProvider cache);

  /**
   * Compute an assignment of tasks to instances
   * @param currStateOutput the current state of the instances
   * @param prevAssignment the previous task partition assignment
   * @param instances the instances
   * @param jobCfg the task configuration
   * @param jobContext the task context
   * @param workflowCfg the workflow configuration
   * @param workflowCtx the workflow context
   * @param partitionSet the partitions to assign
   * @param cache cluster snapshot
   * @return map of instances to set of partition numbers
   */
  public abstract Map<String, SortedSet<Integer>> getTaskAssignment(
      CurrentStateOutput currStateOutput, ResourceAssignment prevAssignment,
      Collection<String> instances, JobConfig jobCfg, JobContext jobContext,
      WorkflowConfig workflowCfg, WorkflowContext workflowCtx, Set<Integer> partitionSet,
      WorkflowControllerDataProvider cache);

  @Override
  public void init(HelixManager manager) {
    _manager = manager;
  }

  @Override
  public ResourceAssignment computeBestPossiblePartitionState(WorkflowControllerDataProvider clusterData,
      IdealState taskIs, Resource resource, CurrentStateOutput currStateOutput) {
    final String resourceName = resource.getResourceName();
    LOG.debug("Computer Best Partition for resource: " + resourceName);

    // Fetch job configuration
    JobConfig jobCfg = (JobConfig) clusterData.getResourceConfig(resourceName);
    if (jobCfg == null) {
      LOG.debug("Job configuration is NULL for " + resourceName);
      return emptyAssignment(resourceName, currStateOutput);
    }
    String workflowResource = jobCfg.getWorkflow();

    // Fetch workflow configuration and context
    WorkflowConfig workflowCfg = clusterData.getWorkflowConfig(workflowResource);
    if (workflowCfg == null) {
      LOG.debug("Workflow configuration is NULL for " + resourceName);
      return emptyAssignment(resourceName, currStateOutput);
    }
    WorkflowContext workflowCtx = clusterData.getWorkflowContext(workflowResource);

    // Initialize workflow context if needed
    if (workflowCtx == null) {
      workflowCtx = new WorkflowContext(new ZNRecord(TaskUtil.WORKFLOW_CONTEXT_KW));
      workflowCtx.setStartTime(System.currentTimeMillis());
      workflowCtx.setName(workflowResource);
      LOG.info("Workflow context for " + resourceName + " created!");
    }

    // check ancestor job status
    int notStartedCount = 0;
    int inCompleteCount = 0;
    for (String ancestor : workflowCfg.getJobDag().getAncestors(resourceName)) {
      TaskState jobState = workflowCtx.getJobState(ancestor);
      if (jobState == null || jobState == TaskState.NOT_STARTED) {
        ++notStartedCount;
      } else if (jobState == TaskState.IN_PROGRESS || jobState == TaskState.STOPPED) {
        ++inCompleteCount;
      }
    }

    if (notStartedCount > 0 || (workflowCfg.isJobQueue() && inCompleteCount >= workflowCfg
        .getParallelJobs())) {
      LOG.debug("Job is not ready to be scheduled due to pending dependent jobs " + resourceName);
      return emptyAssignment(resourceName, currStateOutput);
    }

    // Clean up if workflow marked for deletion
    TargetState targetState = workflowCfg.getTargetState();
    if (targetState == TargetState.DELETE) {
      LOG.info(
          "Workflow is marked as deleted " + workflowResource
              + " cleaning up the workflow context.");
      cleanup(_manager, resourceName, workflowCfg, workflowResource);
      return emptyAssignment(resourceName, currStateOutput);
    }

    // Check if this workflow has been finished past its expiry.
    if (workflowCtx.getFinishTime() != WorkflowContext.UNFINISHED
        && workflowCtx.getFinishTime() + workflowCfg.getExpiry() <= System.currentTimeMillis()) {
      LOG.info("Workflow " + workflowResource
          + " is completed and passed expiry time, cleaning up the workflow context.");
      markForDeletion(_manager, workflowResource);
      cleanup(_manager, resourceName, workflowCfg, workflowResource);
      return emptyAssignment(resourceName, currStateOutput);
    }

    // Fetch any existing context information from the property store.

    JobContext jobCtx = clusterData.getJobContext(resourceName);
    if (jobCtx == null) {
      jobCtx = new JobContext(new ZNRecord(TaskUtil.TASK_CONTEXT_KW));
      jobCtx.setStartTime(System.currentTimeMillis());
      jobCtx.setName(resourceName);
    }

    // Check for expired jobs for non-terminable workflows
    long jobFinishTime = jobCtx.getFinishTime();
    if (!workflowCfg.isTerminable() && jobFinishTime != WorkflowContext.UNFINISHED
        && jobFinishTime + workflowCfg.getExpiry() <= System.currentTimeMillis()) {
      LOG.info("Job " + resourceName
          + " is completed and passed expiry time, cleaning up the job context.");
      cleanup(_manager, resourceName, workflowCfg, workflowResource);
      return emptyAssignment(resourceName, currStateOutput);
    }

    // The job is already in a final state (completed/failed).
    if (workflowCtx.getJobState(resourceName) == TaskState.FAILED
        || workflowCtx.getJobState(resourceName) == TaskState.COMPLETED) {
      LOG.debug("Job " + resourceName + " is failed or already completed.");
      return emptyAssignment(resourceName, currStateOutput);
    }

    // Check for readiness, and stop processing if it's not ready
    boolean isReady =
        scheduleIfNotReady(workflowCfg, workflowCtx, workflowResource, resourceName, clusterData);
    if (!isReady) {
      LOG.debug("Job " + resourceName + " is not ready to be scheduled.");
      return emptyAssignment(resourceName, currStateOutput);
    }

    // Grab the old assignment, or an empty one if it doesn't exist
    ResourceAssignment prevAssignment = getPrevResourceAssignment(_manager, resourceName);
    if (prevAssignment == null) {
      prevAssignment = new ResourceAssignment(resourceName);
    }

    // Will contain the list of partitions that must be explicitly dropped from the ideal state that
    // is stored in zk.
    // Fetch the previous resource assignment from the property store. This is required because of
    // HELIX-230.
    Set<Integer> partitionsToDrop = new TreeSet<Integer>();

    ResourceAssignment newAssignment =
        computeResourceMapping(resourceName, workflowCfg, jobCfg, prevAssignment, clusterData
            .getLiveInstances().keySet(), currStateOutput, workflowCtx, jobCtx, partitionsToDrop,
            clusterData);

    if (!partitionsToDrop.isEmpty()) {
      for (Integer pId : partitionsToDrop) {
        taskIs.getRecord().getMapFields().remove(pName(resourceName, pId));
      }
      HelixDataAccessor accessor = _manager.getHelixDataAccessor();
      PropertyKey propertyKey = accessor.keyBuilder().idealStates(resourceName);
      accessor.setProperty(propertyKey, taskIs);
    }

    // Update Workflow and Job context in data cache and ZK.
    clusterData.updateJobContext(resourceName, jobCtx);
    clusterData
        .updateWorkflowContext(workflowResource, workflowCtx);

    setPrevResourceAssignment(_manager, resourceName, newAssignment);

    LOG.debug("Job " + resourceName + " new assignment " + Arrays
        .toString(newAssignment.getMappedPartitions().toArray()));

    return newAssignment;
  }

  /**
   * Get the last task assignment for a given job
   * @param manager a connection to Helix
   * @param resourceName the name of the job
   * @return {@link ResourceAssignment} instance, or null if no assignment is available
   */
  private ResourceAssignment getPrevResourceAssignment(HelixManager manager,
      String resourceName) {
    ZNRecord r =
        manager.getHelixPropertyStore().get(
            Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, resourceName, PREV_RA_NODE),
            null, AccessOption.PERSISTENT);
    return r != null ? new ResourceAssignment(r) : null;
  }

  /**
   * Set the last task assignment for a given job
   * @param manager a connection to Helix
   * @param resourceName the name of the job
   * @param ra {@link ResourceAssignment} containing the task assignment
   */
  public void setPrevResourceAssignment(HelixManager manager, String resourceName,
      ResourceAssignment ra) {
    manager.getHelixPropertyStore().set(
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, resourceName, PREV_RA_NODE),
        ra.getRecord(), AccessOption.PERSISTENT);
  }

  private Set<String> getInstancesAssignedToOtherJobs(String currentJobName,
      WorkflowConfig workflowCfg, WorkflowControllerDataProvider cache) {

    Set<String> ret = new HashSet<String>();

    for (String jobName : workflowCfg.getJobDag().getAllNodes()) {
      if (jobName.equals(currentJobName)) {
        continue;
      }

      JobContext jobContext = cache.getJobContext(jobName);
      if (jobContext == null) {
        continue;
      }
      for (int partition : jobContext.getPartitionSet()) {
        TaskPartitionState partitionState = jobContext.getPartitionState(partition);
        if (partitionState == TaskPartitionState.INIT ||
            partitionState == TaskPartitionState.RUNNING) {
          ret.add(jobContext.getAssignedParticipant(partition));
        }
      }
    }

    return ret;
  }

  private ResourceAssignment computeResourceMapping(String jobResource,
      WorkflowConfig workflowConfig, JobConfig jobCfg, ResourceAssignment prevAssignment,
      Collection<String> liveInstances, CurrentStateOutput currStateOutput,
      WorkflowContext workflowCtx, JobContext jobCtx, Set<Integer> partitionsToDropFromIs,
      WorkflowControllerDataProvider cache) {
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

    Set<String> excludedInstances =
        getInstancesAssignedToOtherJobs(jobResource, workflowConfig, cache);

    // Process all the current assignments of tasks.
    Set<Integer> allPartitions =
        getAllTaskPartitions(jobCfg, jobCtx, workflowConfig, workflowCtx, cache);
    Map<String, SortedSet<Integer>> taskAssignments =
        getTaskPartitionAssignments(liveInstances, prevAssignment, allPartitions);
    long currentTime = System.currentTimeMillis();
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

        // Check for pending state transitions on this (partition, instance).
        Message pendingMessage =
            currStateOutput.getPendingMessage(jobResource, new Partition(pName), instance);
        if (pendingMessage != null) {
          // There is a pending state transition for this (partition, instance). Just copy forward
          // the state assignment from the previous ideal state.
          Map<String, String> stateMap = prevAssignment.getReplicaMap(new Partition(pName));
          if (stateMap != null) {
            String prevState = stateMap.get(instance);
            paMap.put(pId, new PartitionAssignment(instance, prevState));
            assignedPartitions.add(pId);
            if (LOG.isDebugEnabled()) {
              LOG.debug(String
                  .format(
                      "Task partition %s has a pending state transition on instance %s. Using the previous ideal state which was %s.",
                      pName, instance, prevState));
            }
          }

          continue;
        }

        TaskPartitionState currState =
            TaskPartitionState.valueOf(currStateOutput.getCurrentState(jobResource, new Partition(
                pName), instance));
        jobCtx.setPartitionState(pId, currState);

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
        case RUNNING:
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

    if (isJobComplete(jobCtx, allPartitions, skippedPartitions, jobCfg)) {
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
      addGiveupPartitions(excludeSet, jobCtx, allPartitions, jobCfg);
      excludeSet.addAll(skippedPartitions);
      excludeSet.addAll(getNonReadyPartitions(jobCtx, currentTime));
      // Get instance->[partition, ...] mappings for the target resource.
      Map<String, SortedSet<Integer>> tgtPartitionAssignments =
          getTaskAssignment(currStateOutput, prevAssignment, liveInstances, jobCfg, jobCtx,
              workflowConfig, workflowCtx, allPartitions, cache);
      for (Map.Entry<String, SortedSet<Integer>> entry : taskAssignments.entrySet()) {
        String instance = entry.getKey();
        if (!tgtPartitionAssignments.containsKey(instance) || excludedInstances.contains(instance)) {
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
      String workflowResource, String jobResource, WorkflowControllerDataProvider cache) {
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
        LOG.debug("Remove schedule timer for " + jobResource + " time: " + SCHEDULED_TIMES.get(jobResource));
        SCHEDULED_TIMES.remove(workflowResource);
      }

      // Recurring workflows are just templates that spawn new workflows
      if (scheduleConfig.isRecurring()) {
        // Skip scheduling this workflow if it's not in a start state
        if (!workflowCfg.getTargetState().equals(TargetState.START)) {
          LOG.debug(
              "Skip scheduling since the workflow has not been started " + workflowResource);
          return false;
        }

        // Skip scheduling this workflow again if the previous run (if any) is still active
        String lastScheduled = workflowCtx.getLastScheduledSingleWorkflow();
        if (lastScheduled != null) {
          WorkflowContext lastWorkflowCtx = cache.getWorkflowContext(lastScheduled);
          if (lastWorkflowCtx != null
              && lastWorkflowCtx.getFinishTime() == WorkflowContext.UNFINISHED) {
            LOG.info("Skip scheduling since last schedule has not completed yet " + lastScheduled);
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
        DateFormat df = new SimpleDateFormat("yyyyMMdd'T'HHmmssZ");
        // Now clone the workflow if this clone has not yet been created
        String newWorkflowName = workflowResource + "_" + df.format(new java.util.Date(timeToSchedule));
        LOG.debug("Ready to start workflow " + newWorkflowName);
        if (!newWorkflowName.equals(lastScheduled)) {
          Workflow clonedWf =
              cloneWorkflow(_manager, workflowResource, newWorkflowName, new Date(timeToSchedule));
          TaskDriver driver = new TaskDriver(_manager);
          try {
            // Start the cloned workflow
            driver.start(clonedWf);
          } catch (Exception e) {
            LOG.error("Failed to schedule cloned workflow " + newWorkflowName, e);
          }
          // Persist workflow start regardless of success to avoid retrying and failing
          workflowCtx.setLastScheduledSingleWorkflow(newWorkflowName);
          cache.updateWorkflowContext(workflowResource, workflowCtx);
        }

        // Change the time to trigger the pipeline to that of the next run
        startTime = new Date(timeToSchedule + period);
        delayFromStart = startTime.getTime() - System.currentTimeMillis();
      } else {
        // This is a one-time workflow and is ready
        return true;
      }
    }

    scheduleRebalance(workflowResource, jobResource, startTime, delayFromStart);
    return false;
  }

  /**
   * Create a new workflow based on an existing one
   * @param manager connection to Helix
   * @param origWorkflowName the name of the existing workflow
   * @param newWorkflowName the name of the new workflow
   * @param newStartTime a provided start time that deviates from the desired start time
   * @return the cloned workflow, or null if there was a problem cloning the existing one
   */
  private Workflow cloneWorkflow(HelixManager manager, String origWorkflowName,
      String newWorkflowName, Date newStartTime) {
    // Read all resources, including the workflow and jobs of interest
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Map<String, HelixProperty> resourceConfigMap =
        accessor.getChildValuesMap(keyBuilder.resourceConfigs());
    if (!resourceConfigMap.containsKey(origWorkflowName)) {
      LOG.error("No such workflow named " + origWorkflowName);
      return null;
    }
    if (resourceConfigMap.containsKey(newWorkflowName)) {
      LOG.error("Workflow with name " + newWorkflowName + " already exists!");
      return null;
    }

    // Create a new workflow with a new name
    HelixProperty workflowConfig = resourceConfigMap.get(origWorkflowName);
    Map<String, String> wfSimpleFields = workflowConfig.getRecord().getSimpleFields();
    JobDag jobDag =
        JobDag.fromJson(wfSimpleFields.get(WorkflowConfig.WorkflowConfigProperty.Dag.name()));
    Map<String, Set<String>> parentsToChildren = jobDag.getParentsToChildren();
    Workflow.Builder builder = new Workflow.Builder(newWorkflowName);

    // Set the workflow expiry
    builder.setExpiry(
        Long.parseLong(wfSimpleFields.get(WorkflowConfig.WorkflowConfigProperty.Expiry.name())));

    // Set the schedule, if applicable
    ScheduleConfig scheduleConfig;
    if (newStartTime != null) {
      scheduleConfig = ScheduleConfig.oneTimeDelayedStart(newStartTime);
    } else {
      scheduleConfig = WorkflowConfig.parseScheduleFromConfigMap(wfSimpleFields);
    }
    if (scheduleConfig != null) {
      builder.setScheduleConfig(scheduleConfig);
    }

    // Add each job back as long as the original exists
    Set<String> namespacedJobs = jobDag.getAllNodes();
    for (String namespacedJob : namespacedJobs) {
      if (resourceConfigMap.containsKey(namespacedJob)) {
        // Copy over job-level and task-level configs
        String job = TaskUtil.getDenamespacedJobName(origWorkflowName, namespacedJob);
        HelixProperty jobConfig = resourceConfigMap.get(namespacedJob);
        Map<String, String> jobSimpleFields = jobConfig.getRecord().getSimpleFields();
        jobSimpleFields.put(JobConfig.JobConfigProperty.WorkflowID.name(), newWorkflowName); // overwrite workflow name
        for (Map.Entry<String, String> e : jobSimpleFields.entrySet()) {
          builder.addConfig(job, e.getKey(), e.getValue());
        }
        Map<String, Map<String, String>> rawTaskConfigMap = jobConfig.getRecord().getMapFields();
        List<TaskConfig> taskConfigs = Lists.newLinkedList();
        for (Map<String, String> rawTaskConfig : rawTaskConfigMap.values()) {
          TaskConfig taskConfig = TaskConfig.Builder.from(rawTaskConfig);
          taskConfigs.add(taskConfig);
        }
        builder.addTaskConfigs(job, taskConfigs);

        // Add dag dependencies
        Set<String> children = parentsToChildren.get(namespacedJob);
        if (children != null) {
          for (String namespacedChild : children) {
            String child = TaskUtil.getDenamespacedJobName(origWorkflowName, namespacedChild);
            builder.addParentChildDependency(job, child);
          }
        }
      }
    }
    return builder.build();
  }

  private void scheduleRebalance(String id, String jobResource, Date startTime, long delayFromStart) {
    // Do nothing if there is already a timer set for the this workflow with the same start time.
    if ((SCHEDULED_TIMES.containsKey(id) && SCHEDULED_TIMES.get(id).equals(startTime))
        || SCHEDULED_TIMES.inverse().containsKey(startTime)) {
      LOG.debug("Schedule timer for" + id + "and job: " + jobResource + " is up to date.");
      return;
    }
    LOG.info(
        "Schedule rebalance with id: " + id + "and job: " + jobResource + " at time: " + startTime
            + " delay from start: " + delayFromStart);

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
      LOG.debug(
          "Remove schedule timer for" + jobResource + " time: " + SCHEDULED_TIMES.get(jobResource));
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
      Set<Integer> skippedPartitions, JobConfig cfg) {
    for (Integer pId : allPartitions) {
      TaskPartitionState state = ctx.getPartitionState(pId);
      if (!skippedPartitions.contains(pId) && state != TaskPartitionState.COMPLETED
          && !isTaskGivenup(ctx, cfg, pId)) {
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
        WorkflowConfig.WorkflowConfigProperty.TargetState.name(), TargetState.DELETE.name());
  }

  /**
   * Cleans up all Helix state associated with this job, wiping workflow-level information if this
   * is the last remaining job in its workflow, and the workflow is terminable.
   */
  private static void cleanup(HelixManager mgr, final String resourceName, WorkflowConfig cfg,
      String workflowResource) {
    LOG.info("Cleaning up job: " + resourceName + " in workflow: " + workflowResource);
    HelixDataAccessor accessor = mgr.getHelixDataAccessor();

    // Remove any DAG references in workflow
    PropertyKey workflowKey = getConfigPropertyKey(accessor, workflowResource);
    DataUpdater<ZNRecord> dagRemover = new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        JobDag jobDag = JobDag
            .fromJson(currentData.getSimpleField(WorkflowConfig.WorkflowConfigProperty.Dag.name()));
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
          currentData
              .setSimpleField(WorkflowConfig.WorkflowConfigProperty.Dag.name(), jobDag.toJson());
        } catch (Exception e) {
          LOG.equals("Could not update DAG for job: " + resourceName);
        }
        return currentData;
      }
    };
    accessor.getBaseDataAccessor().update(workflowKey.getPath(), dagRemover,
        AccessOption.PERSISTENT);

    // Delete resource configs.
    PropertyKey cfgKey = getConfigPropertyKey(accessor, resourceName);
    if (!accessor.removeProperty(cfgKey)) {
      throw new RuntimeException(String.format(
          "Error occurred while trying to clean up job %s. Failed to remove node %s from Helix. Aborting further clean up steps.",
          resourceName,
          cfgKey));
    }

    // Delete property store information for this resource.
    // For recurring workflow, it's OK if the node doesn't exist.
    String propStoreKey = getRebalancerPropStoreKey(resourceName);
    mgr.getHelixPropertyStore().remove(propStoreKey, AccessOption.PERSISTENT);

    // Delete the ideal state itself.
    PropertyKey isKey = getISPropertyKey(accessor, resourceName);
    if (!accessor.removeProperty(isKey)) {
      throw new RuntimeException(String.format(
          "Error occurred while trying to clean up task %s. Failed to remove node %s from Helix.",
          resourceName, isKey));
    }

    // Delete dead external view
    // because job is already completed, there is no more current state change
    // thus dead external views removal will not be triggered
    PropertyKey evKey = accessor.keyBuilder().externalView(resourceName);
    accessor.removeProperty(evKey);

    LOG.info(String.format("Successfully cleaned up job resource %s.", resourceName));

    boolean lastInWorkflow = true;
    for (String job : cfg.getJobDag().getAllNodes()) {
      // check if property store information or resource configs exist for this job
      if (mgr.getHelixPropertyStore().exists(getRebalancerPropStoreKey(job),
          AccessOption.PERSISTENT)
          || accessor.getProperty(getConfigPropertyKey(accessor, job)) != null
          || accessor.getProperty(getISPropertyKey(accessor, job)) != null) {
        lastInWorkflow = false;
        break;
      }
    }

    // clean up workflow-level info if this was the last in workflow
    if (lastInWorkflow && (cfg.isTerminable() || cfg.getTargetState() == TargetState.DELETE)) {
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
      // Remove pending timer for this workflow if exists
      if (SCHEDULED_TIMES.containsKey(workflowResource)) {
        SCHEDULED_TIMES.remove(workflowResource);
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

  private static ResourceAssignment emptyAssignment(String name, CurrentStateOutput currStateOutput) {
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

  private static void addCompletedPartitions(Set<Integer> set, JobContext ctx,
      Iterable<Integer> pIds) {
    for (Integer pId : pIds) {
      TaskPartitionState state = ctx.getPartitionState(pId);
      if (state == TaskPartitionState.COMPLETED) {
        set.add(pId);
      }
    }
  }

  private static boolean isTaskGivenup(JobContext ctx, JobConfig cfg, int pId) {
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
      int pId = pId(partition.getPartitionName());
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

  @Override
  public IdealState computeNewIdealState(String resourceName, IdealState currentIdealState,
      CurrentStateOutput currentStateOutput, WorkflowControllerDataProvider clusterData) {
    // All of the heavy lifting is in the ResourceAssignment computation,
    // so this part can just be a no-op.
    return currentIdealState;
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
      RebalanceScheduler.invokeRebalance(_manager.getHelixDataAccessor(), _resource);
    }
  }
}
