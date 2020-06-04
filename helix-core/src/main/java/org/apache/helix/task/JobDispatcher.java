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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.task.assigner.ThreadCountBasedTaskAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobDispatcher extends AbstractTaskDispatcher {
  private static final Logger LOG = LoggerFactory.getLogger(JobDispatcher.class);

  // Intermediate states (meaning they are not terminal states) for workflows and jobs
  private static final Set<TaskState> INTERMEDIATE_STATES = new HashSet<>(Arrays
      .asList(TaskState.IN_PROGRESS, TaskState.NOT_STARTED, TaskState.STOPPING, TaskState.STOPPED));
  private WorkflowControllerDataProvider _dataProvider;

  public void updateCache(WorkflowControllerDataProvider cache) {
    _dataProvider = cache;
  }

  public ResourceAssignment processJobStatusUpdateAndAssignment(String jobName,
      CurrentStateOutput currStateOutput, WorkflowContext workflowCtx) {
    // Fetch job configuration
    final JobConfig jobCfg = _dataProvider.getJobConfig(jobName);
    if (jobCfg == null) {
      LOG.error("Job configuration is NULL for " + jobName);
      return buildEmptyAssignment(jobName, currStateOutput);
    }
    String workflowResource = jobCfg.getWorkflow();

    // Fetch workflow configuration and context
    final WorkflowConfig workflowCfg = _dataProvider.getWorkflowConfig(workflowResource);
    if (workflowCfg == null) {
      LOG.error("Workflow configuration is NULL for " + jobName);
      return buildEmptyAssignment(jobName, currStateOutput);
    }

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

    // Stop current run of the job if workflow or job is already in final state (failed or
    // completed)
    TaskState workflowState = workflowCtx.getWorkflowState();
    TaskState jobState = workflowCtx.getJobState(jobName);
    // The job is already in a final state (completed/failed).
    if (workflowState == TaskState.FAILED || workflowState == TaskState.COMPLETED
        || jobState == TaskState.FAILED || jobState == TaskState.COMPLETED) {
      LOG.info(String.format(
          "Workflow %s or job %s is already failed or completed, workflow state (%s), job state (%s), clean up job IS.",
          workflowResource, jobName, workflowState, jobState));
      finishJobInRuntimeJobDag(_dataProvider.getTaskDataCache(), workflowResource, jobName);
      TaskUtil.cleanupJobIdealStateExtView(_manager.getHelixDataAccessor(), jobName);
      _rebalanceScheduler.removeScheduledRebalance(jobName);
      return buildEmptyAssignment(jobName, currStateOutput);
    }

    if (!isWorkflowReadyForSchedule(workflowCfg)) {
      LOG.info("Job is not ready to be run since workflow is not ready " + jobName);
      return buildEmptyAssignment(jobName, currStateOutput);
    }

    if (!TaskUtil.isJobStarted(jobName, workflowCtx) && !isJobReadyToSchedule(jobName, workflowCfg,
        workflowCtx, TaskUtil.getInCompleteJobCount(workflowCfg, workflowCtx),
        _dataProvider.getJobConfigMap(), _dataProvider,
        _dataProvider.getAssignableInstanceManager())) {
      LOG.info("Job is not ready to run " + jobName);
      return buildEmptyAssignment(jobName, currStateOutput);
    }

    // Fetch any existing context information from the property store.
    JobContext jobCtx = _dataProvider.getJobContext(jobName);
    if (jobCtx == null) {
      jobCtx = new JobContext(new ZNRecord(TaskUtil.TASK_CONTEXT_KW));
      final long currentTimestamp = System.currentTimeMillis();
      jobCtx.setStartTime(currentTimestamp);
      jobCtx.setName(jobName);
      // This job's JobContext has not been created yet. Since we are creating a new JobContext
      // here, we must also create its UserContentStore
      TaskUtil.createUserContent(_manager.getHelixPropertyStore(), jobName,
          new ZNRecord(TaskUtil.USER_CONTENT_NODE));
      workflowCtx.setJobState(jobName, TaskState.IN_PROGRESS);

      // Since this job has been processed for the first time, we report SubmissionToProcessDelay
      // here asynchronously
      reportSubmissionToProcessDelay(_dataProvider, _clusterStatusMonitor, workflowCfg, jobCfg,
          currentTimestamp);
    }

    if (!TaskState.TIMED_OUT.equals(workflowCtx.getJobState(jobName))) {
      scheduleRebalanceForTimeout(jobCfg.getJobId(), jobCtx.getStartTime(), jobCfg.getTimeout());
    }

    // Grab the old assignment, or an empty one if it doesn't exist
    ResourceAssignment prevAssignment =
        _dataProvider.getTaskDataCache().getPreviousAssignment(jobName);
    if (prevAssignment == null) {
      prevAssignment = new ResourceAssignment(jobName);
    }

    // Will contain the list of partitions that must be explicitly dropped from the ideal state that
    // is stored in zk.
    // Fetch the previous resource assignment from the property store. This is required because of
    // HELIX-230.
    Set<String> liveInstances =
        jobCfg.getInstanceGroupTag() == null ? _dataProvider.getEnabledLiveInstances()
            : _dataProvider.getEnabledLiveInstancesWithTag(jobCfg.getInstanceGroupTag());

    if (liveInstances.isEmpty()) {
      LOG.error("No available instance found for job!");
    }

    TargetState jobTgtState = workflowCfg.getTargetState();
    jobState = workflowCtx.getJobState(jobName);
    workflowState = workflowCtx.getWorkflowState();

    if (INTERMEDIATE_STATES.contains(jobState)
        && (isTimeout(jobCtx.getStartTime(), jobCfg.getTimeout())
            || TaskState.TIMED_OUT.equals(workflowState))) {
      jobState = TaskState.TIMING_OUT;
      workflowCtx.setJobState(jobName, TaskState.TIMING_OUT);
    } else if (jobState != TaskState.TIMING_OUT && jobState != TaskState.FAILING) {
      // TIMING_OUT/FAILING/ABORTING job can't be stopped, because all tasks are being aborted
      // Update running status in workflow context
      if (jobTgtState == TargetState.STOP) {
        if (jobState != TaskState.NOT_STARTED && TaskUtil.checkJobStopped(jobCtx)) {
          workflowCtx.setJobState(jobName, TaskState.STOPPED);
        } else {
          workflowCtx.setJobState(jobName, TaskState.STOPPING);
        }
        // Workflow has been stopped if all in progress jobs are stopped
        if (isWorkflowStopped(workflowCtx, workflowCfg)) {
          workflowCtx.setWorkflowState(TaskState.STOPPED);
        } else {
          workflowCtx.setWorkflowState(TaskState.STOPPING);
        }
      } else {
        workflowCtx.setJobState(jobName, TaskState.IN_PROGRESS);
        // Workflow is in progress if any task is in progress
        workflowCtx.setWorkflowState(TaskState.IN_PROGRESS);
      }
    }

    Set<Integer> partitionsToDrop = new TreeSet<>();
    ResourceAssignment newAssignment =
        computeResourceMapping(jobName, workflowCfg, jobCfg, jobState, jobTgtState, liveInstances,
            currStateOutput, workflowCtx, jobCtx, partitionsToDrop, _dataProvider);

    // Update Workflow and Job context in data cache and ZK.
    _dataProvider.updateJobContext(jobName, jobCtx);
    _dataProvider.updateWorkflowContext(workflowResource, workflowCtx);
    _dataProvider.getTaskDataCache().setPreviousAssignment(jobName, newAssignment);

    LOG.debug("Job " + jobName + " new assignment "
        + Arrays.toString(newAssignment.getMappedPartitions().toArray()));
    return newAssignment;
  }

  private ResourceAssignment computeResourceMapping(String jobResource,
      WorkflowConfig workflowConfig, JobConfig jobCfg, TaskState jobState, TargetState jobTgtState,
      Collection<String> liveInstances, CurrentStateOutput currStateOutput,
      WorkflowContext workflowCtx, JobContext jobCtx, Set<Integer> partitionsToDropFromIs,
      WorkflowControllerDataProvider cache) {

    // Used to keep track of tasks that have already been assigned to instances.
    // InstanceName -> Set of task partitions assigned to that instance in this iteration
    Map<String, Set<Integer>> assignedPartitions = new HashMap<>();

    // Used to keep track of tasks that have failed, but whose failure is acceptable
    Set<Integer> skippedPartitions = new HashSet<>();

    // Keeps a mapping of (partition) -> (instance, state)
    Map<Integer, PartitionAssignment> paMap = new TreeMap<>();

    Set<String> excludedInstances =
        getExcludedInstances(jobResource, workflowConfig, workflowCtx, cache);

    // Process all the current assignments of tasks.
    TaskAssignmentCalculator taskAssignmentCal = getAssignmentCalculator(jobCfg, cache);
    Set<Integer> allPartitions = taskAssignmentCal.getAllTaskPartitions(jobCfg, jobCtx,
        workflowConfig, workflowCtx, cache.getIdealStates());

    if (allPartitions == null || allPartitions.isEmpty()) {
      // Empty target partitions, mark the job as FAILED.
      String failureMsg =
          "Empty task partition mapping for job " + jobResource + ", marked the job as FAILED!";
      LOG.info(failureMsg);
      jobCtx.setInfo(failureMsg);
      failJob(jobResource, workflowCtx, jobCtx, workflowConfig, cache.getJobConfigMap(), cache);
      markAllPartitionsError(jobCtx);
      return new ResourceAssignment(jobResource);
    }

    // This set contains all task pIds that need to be dropped because requestedState is DROPPED
    // Newer versions of Participants, upon connection reset, sets task requestedStates to DROPPED
    // These dropping transitions will be prioritized above all task state transition assignments
    Map<String, Set<Integer>> tasksToDrop = new HashMap<>();

    Map<String, SortedSet<Integer>> currentInstanceToTaskAssignments =
        getCurrentInstanceToTaskAssignments(liveInstances, currStateOutput, jobResource, tasksToDrop);

    updateInstanceToTaskAssignmentsFromContext(jobCtx, currentInstanceToTaskAssignments);

    long currentTime = System.currentTimeMillis();

    if (LOG.isDebugEnabled()) {
      LOG.debug("All partitions: " + allPartitions + " taskAssignment: "
          + currentInstanceToTaskAssignments + " excludedInstances: " + excludedInstances);
    }

    // Release resource for tasks in terminal state
    updatePreviousAssignedTasksStatus(currentInstanceToTaskAssignments, excludedInstances,
        jobResource, currStateOutput, jobCtx, jobCfg, jobState, assignedPartitions,
        partitionsToDropFromIs, paMap, jobTgtState, skippedPartitions, cache, tasksToDrop);

    addGiveupPartitions(skippedPartitions, jobCtx, allPartitions, jobCfg);

    if (jobState == TaskState.IN_PROGRESS && skippedPartitions.size() > jobCfg.getFailureThreshold()
        || (jobCfg.getTargetResource() != null
            && cache.getIdealState(jobCfg.getTargetResource()) != null
            && !cache.getIdealState(jobCfg.getTargetResource()).isEnabled())) {
      if (isJobFinished(jobCtx, jobResource, currStateOutput)) {
        failJob(jobResource, workflowCtx, jobCtx, workflowConfig, cache.getJobConfigMap(), cache);
        return buildEmptyAssignment(jobResource, currStateOutput);
      }
      workflowCtx.setJobState(jobResource, TaskState.FAILING);
      // Drop all assigned but not given-up tasks
      for (int pId : jobCtx.getPartitionSet()) {
        String instance = jobCtx.getAssignedParticipant(pId);
        if (jobCtx.getPartitionState(pId) != null && !isTaskGivenup(jobCtx, jobCfg, pId)) {
          paMap.put(pId, new PartitionAssignment(instance, TaskPartitionState.TASK_ABORTED.name()));
        }
        Partition partition = new Partition(pName(jobResource, pId));
        Message pendingMessage =
            currStateOutput.getPendingMessage(jobResource, partition, instance);
        // While job is failing, if the task is pending on INIT->RUNNING, set it back to INIT,
        // so that Helix will cancel the transition.
        if (jobCtx.getPartitionState(pId) == TaskPartitionState.INIT && pendingMessage != null) {
          paMap.put(pId, new PartitionAssignment(instance, TaskPartitionState.INIT.name()));
        }
      }

      return toResourceAssignment(jobResource, paMap);
    }

    if (jobState == TaskState.FAILING && isJobFinished(jobCtx, jobResource, currStateOutput)) {
      failJob(jobResource, workflowCtx, jobCtx, workflowConfig, cache.getJobConfigMap(), cache);
      return buildEmptyAssignment(jobResource, currStateOutput);
    }

    if (isJobComplete(jobCtx, allPartitions, jobCfg)) {
      markJobComplete(jobResource, jobCtx, workflowConfig, workflowCtx, cache.getJobConfigMap(),
          cache);
      _clusterStatusMonitor.updateJobCounters(jobCfg, TaskState.COMPLETED,
          jobCtx.getFinishTime() - jobCtx.getStartTime());
      _rebalanceScheduler.removeScheduledRebalance(jobResource);
      TaskUtil.cleanupJobIdealStateExtView(_manager.getHelixDataAccessor(), jobResource);
      return buildEmptyAssignment(jobResource, currStateOutput);
    }

    // If job is being timed out and no task is running (for whatever reason), idealState can be
    // deleted and all tasks
    // can be dropped(note that Helix doesn't track whether the drop is success or not).
    if (jobState == TaskState.TIMING_OUT && isJobFinished(jobCtx, jobResource, currStateOutput)) {
      handleJobTimeout(jobCtx, workflowCtx, jobResource, jobCfg);
      finishJobInRuntimeJobDag(cache.getTaskDataCache(), workflowConfig.getWorkflowId(),
          jobResource);
      return buildEmptyAssignment(jobResource, currStateOutput);
    }

    // For delayed tasks, trigger a rebalance event for the closest upcoming ready time
    scheduleForNextTask(jobResource, jobCtx, currentTime);

    // Make additional task assignments if needed.
    if (jobState != TaskState.TIMING_OUT && jobState != TaskState.TIMED_OUT
        && jobTgtState == TargetState.START) {
      handleAdditionalTaskAssignment(currentInstanceToTaskAssignments, excludedInstances,
          jobResource, currStateOutput, jobCtx, jobCfg, workflowConfig, workflowCtx, cache,
          assignedPartitions, paMap, skippedPartitions, taskAssignmentCal, allPartitions,
          currentTime, liveInstances);
    }

    return toResourceAssignment(jobResource, paMap);
  }

  private ResourceAssignment toResourceAssignment(String jobResource,
      Map<Integer, PartitionAssignment> paMap) {
    // Construct a ResourceAssignment object from the map of partition assignments.
    ResourceAssignment ra = new ResourceAssignment(jobResource);
    for (Map.Entry<Integer, PartitionAssignment> e : paMap.entrySet()) {
      PartitionAssignment pa = e.getValue();
      ra.addReplicaMap(new Partition(pName(jobResource, e.getKey())),
          ImmutableMap.of(pa._instance, pa._state));
    }
    return ra;
  }

  private boolean isJobFinished(JobContext jobContext, String jobResource,
      CurrentStateOutput currentStateOutput) {
    for (int pId : jobContext.getPartitionSet()) {
      TaskPartitionState state = jobContext.getPartitionState(pId);
      Partition partition = new Partition(pName(jobResource, pId));
      String instance = jobContext.getAssignedParticipant(pId);
      Message pendingMessage =
          currentStateOutput.getPendingMessage(jobResource, partition, instance);
      // If state is INIT but is pending INIT->RUNNING, it's not yet safe to say the job finished
      if (state == TaskPartitionState.RUNNING
          || (state == TaskPartitionState.INIT && pendingMessage != null)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if the job has completed. Look at states of all tasks of the job, there're 3 kind:
   * completed, given up, not given up. The job is completed if all tasks are completed or given up,
   * and the number of given up tasks is within job failure threshold.
   */
  private static boolean isJobComplete(JobContext ctx, Set<Integer> allPartitions, JobConfig cfg) {
    int numOfGivenUpTasks = 0;
    // Iterate through all tasks, if any one indicates the job has not completed, return false.
    for (Integer pId : allPartitions) {
      TaskPartitionState state = ctx.getPartitionState(pId);
      if (state != TaskPartitionState.COMPLETED) {
        if (!isTaskGivenup(ctx, cfg, pId)) {
          return false;
        }
        // If the task is given up, there's still chance the job has completed because of job
        // failure threshold.
        numOfGivenUpTasks++;
      }
    }
    return numOfGivenUpTasks <= cfg.getFailureThreshold();
  }

  /**
   * @param liveInstances
   * @param currStateOutput currentStates to make sure currentStates copied over expired sessions
   *          are accounted for
   * @param jobName job name
   * @param tasksToDrop instance -> pId's, to gather all pIds that need to be dropped
   * @return instance -> partitionIds from previous assignment, if the instance is still live
   */
  protected static Map<String, SortedSet<Integer>> getCurrentInstanceToTaskAssignments(
      Iterable<String> liveInstances, CurrentStateOutput currStateOutput, String jobName,
      Map<String, Set<Integer>> tasksToDrop) {
    Map<String, SortedSet<Integer>> result = new HashMap<>();
    for (String instance : liveInstances) {
      result.put(instance, new TreeSet<>());
    }

    // Generate currentInstanceToTaskAssignment with CurrentStateOutput as source of truth
    // Add all pIds existing in CurrentStateOutput
    // We need to add these pIds to result and update their states in JobContext in
    // updatePreviousAssignedTasksStatus method.
    Map<Partition, Map<String, String>> partitions = currStateOutput.getCurrentStateMap(jobName);
    for (Map.Entry<Partition, Map<String, String>> entry : partitions.entrySet()) {
      // Get all (instance -> currentState) mappings
      for (Map.Entry<String, String> instanceToCurrState : entry.getValue().entrySet()) {
        String instance = instanceToCurrState.getKey();
        String requestedState =
            currStateOutput.getRequestedState(jobName, entry.getKey(), instance);
        int pId = TaskUtil.getPartitionId(entry.getKey().getPartitionName());

        if (result.containsKey(instance)) {
          result.get(instance).add(pId);
          // Check if this task needs to be dropped. If so, we need to add to tasksToDrop no matter
          // what its current state is so that it will be dropped
          // This is trying to drop tasks on a reconnected instance with a new sessionId that have
          // all of their requestedState == DROPPED
          if (requestedState != null && requestedState.equals(TaskPartitionState.DROPPED.name())) {
            if (!tasksToDrop.containsKey(instance)) {
              tasksToDrop.put(instance, new HashSet<>());
            }
            tasksToDrop.get(instance).add(pId);
          }
        }
      }
    }
    return result;
  }

  /**
   * If partition is missing from prevInstanceToTaskAssignments (e.g. previous assignment is
   * deleted) it is added from context. Otherwise, the context won't be updated.
   * @param jobCtx Job Context
   * @param currentInstanceToTaskAssignments instance -> partitionIds from CurrentStateOutput
   */
  protected void updateInstanceToTaskAssignmentsFromContext(JobContext jobCtx,
      Map<String, SortedSet<Integer>> currentInstanceToTaskAssignments) {
    for (Integer partition : jobCtx.getPartitionSet()) {
      // We must add all active task pIds back here
      // The states other than Running and Init do not need to be added.
      // Logic in this function is similar to getPrevInstanceToTaskAssignments method
      if (jobCtx.getPartitionState(partition) == TaskPartitionState.RUNNING
          || jobCtx.getPartitionState(partition) == TaskPartitionState.INIT) {
        String instance = jobCtx.getAssignedParticipant(partition);
        if (instance != null) {
          if (currentInstanceToTaskAssignments.containsKey(instance)
              && !currentInstanceToTaskAssignments.get(instance).contains(partition)) {
            currentInstanceToTaskAssignments.get(instance).add(partition);
          }
        }
      }
    }
  }

  /**
   * If the job is a targeted job, use fixedTaskAssignmentCalculator. Otherwise, use
   * threadCountBasedTaskAssignmentCalculator. Both calculators support quota-based scheduling.
   * @param jobConfig
   * @param cache
   * @return
   */
  private TaskAssignmentCalculator getAssignmentCalculator(JobConfig jobConfig,
      WorkflowControllerDataProvider cache) {
    AssignableInstanceManager assignableInstanceManager = cache.getAssignableInstanceManager();
    if (TaskUtil.isGenericTaskJob(jobConfig)) {
      return new ThreadCountBasedTaskAssignmentCalculator(new ThreadCountBasedTaskAssigner(),
          assignableInstanceManager);
    }
    return new FixedTargetTaskAssignmentCalculator(assignableInstanceManager);
  }
}
