package org.apache.helix.task;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.rebalancer.util.RebalanceScheduler;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTaskDispatcher {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractTaskDispatcher.class);

  // For connection management
  protected HelixManager _manager;
  protected static RebalanceScheduler _rebalanceScheduler = new RebalanceScheduler();
  protected ClusterStatusMonitor _clusterStatusMonitor;

  public void init(HelixManager manager) {
    _manager = manager;
  }

  // Job Update related methods

  public void updatePreviousAssignedTasksStatus(
      Map<String, SortedSet<Integer>> prevInstanceToTaskAssignments, Set<String> excludedInstances,
      String jobResource, CurrentStateOutput currStateOutput, JobContext jobCtx, JobConfig jobCfg,
      ResourceAssignment prevTaskToInstanceStateAssignment, TaskState jobState,
      Set<Integer> assignedPartitions, Set<Integer> partitionsToDropFromIs,
      Map<Integer, PartitionAssignment> paMap, TargetState jobTgtState,
      Set<Integer> skippedPartitions) {
    // Iterate through all instances
    for (String instance : prevInstanceToTaskAssignments.keySet()) {
      if (excludedInstances.contains(instance)) {
        continue;
      }

      Set<Integer> pSet = prevInstanceToTaskAssignments.get(instance);
      // Used to keep track of partitions that are in one of the final states: COMPLETED, TIMED_OUT,
      // TASK_ERROR, ERROR.
      Set<Integer> donePartitions = new TreeSet<>();
      for (int pId : pSet) {
        final String pName = pName(jobResource, pId);
        TaskPartitionState currState = updateJobContextAndGetTaskCurrentState(currStateOutput, jobResource, pId, pName,
                instance, jobCtx);

        // Check for pending state transitions on this (partition, instance).
        Message pendingMessage =
            currStateOutput.getPendingState(jobResource, new Partition(pName), instance);
        if (pendingMessage != null && !pendingMessage.getToState().equals(currState.name())) {
          processTaskWithPendingMessage(prevTaskToInstanceStateAssignment, pId, pName, instance,
              pendingMessage, jobState, currState, paMap, assignedPartitions);
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
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format(
                "Instance %s requested a state transition to %s for partition %s.", instance,
                requestedState, pName));
          }
          continue;
        }

        switch (currState) {
        case RUNNING: {
          TaskPartitionState nextState = TaskPartitionState.RUNNING;
          if (jobState.equals(TaskState.TIMING_OUT)) {
            nextState = TaskPartitionState.TASK_ABORTED;
          } else if (jobTgtState.equals(TargetState.STOP)) {
            nextState = TaskPartitionState.STOPPED;
          }

          paMap.put(pId, new PartitionAssignment(instance, nextState.name()));
          assignedPartitions.add(pId);
          if (LOG.isDebugEnabled()) {
            LOG.debug(String
                .format("Setting task partition %s state to %s on instance %s.", pName, nextState,
                    instance));
          }
        }
        break;
        case STOPPED: {
          TaskPartitionState nextState;
          if (jobTgtState.equals(TargetState.START)) {
            nextState = TaskPartitionState.RUNNING;
          } else {
            nextState = TaskPartitionState.STOPPED;
          }

          paMap.put(pId, new JobRebalancer.PartitionAssignment(instance, nextState.name()));
          assignedPartitions.add(pId);
          if (LOG.isDebugEnabled()) {
            LOG.debug(String
                .format("Setting task partition %s state to %s on instance %s.", pName, nextState,
                    instance));
          }
        }
        break;
        case COMPLETED: {
          // The task has completed on this partition. Mark as such in the context object.
          donePartitions.add(pId);
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format(
                "Task partition %s has completed with state %s. Marking as such in rebalancer context.",
                pName, currState));
          }
          partitionsToDropFromIs.add(pId);
          markPartitionCompleted(jobCtx, pId);
        }
        break;
        case TIMED_OUT:
        case TASK_ERROR:
        case TASK_ABORTED:
        case ERROR: {
          donePartitions.add(pId); // The task may be rescheduled on a different instance.
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format(
                "Task partition %s has error state %s with msg %s. Marking as such in rebalancer context.",
                pName, currState, jobCtx.getPartitionInfo(pId)));
          }
          markPartitionError(jobCtx, pId, currState, true);
          // The error policy is to fail the task as soon a single partition fails for a specified
          // maximum number of attempts or task is in ABORTED state.
          // But notice that if job is TIMED_OUT, aborted task won't be treated as fail and won't cause job fail.
          // After all tasks are aborted, they will be dropped, because of job timeout.
          if (jobState != TaskState.TIMED_OUT && jobState != TaskState.TIMING_OUT) {
            if (jobCtx.getPartitionNumAttempts(pId) >= jobCfg.getMaxAttemptsPerTask()
                || currState.equals(TaskPartitionState.TASK_ABORTED)
                || currState.equals(TaskPartitionState.ERROR)) {
              skippedPartitions.add(pId);
              partitionsToDropFromIs.add(pId);
              if (LOG.isDebugEnabled()) {
                LOG.debug("skippedPartitions:" + skippedPartitions);
              }
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
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format(
                "Task partition %s has state %s. It will be dropped from the current ideal state.",
                pName, currState));
          }
        }
        break;
        default:
          throw new AssertionError("Unknown enum symbol: " + currState);
        }
      }

      // Remove the set of task partitions that are completed or in one of the error states.
      pSet.removeAll(donePartitions);
    }
  }

  /**
   * Computes the partition name given the resource name and partition id.
   */
  protected String pName(String resource, int pId) {
    return resource + "_" + pId;
  }

  /**
   * An (instance, state) pair.
   */
  protected static class PartitionAssignment {
    public final String _instance;
    public final String _state;

    PartitionAssignment(String instance, String state) {
      _instance = instance;
      _state = state;
    }
  }

  private TaskPartitionState updateJobContextAndGetTaskCurrentState(CurrentStateOutput currentStateOutput,
      String jobResource, Integer pId, String pName, String instance, JobContext jobCtx) {
    String currentStateString = currentStateOutput.getCurrentState(jobResource, new Partition(
        pName), instance);
    if (currentStateString == null) {
      // Task state is either DROPPED or INIT
      return jobCtx.getPartitionState(pId);
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
            + "happen.", pendingMessage.getToState(), prevState));
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

  protected static void markPartitionCompleted(JobContext ctx, int pId) {
    ctx.setPartitionState(pId, TaskPartitionState.COMPLETED);
    ctx.setPartitionFinishTime(pId, System.currentTimeMillis());
    ctx.incrementNumAttempts(pId);
  }

  protected static void markPartitionError(JobContext ctx, int pId, TaskPartitionState state,
      boolean incrementAttempts) {
    ctx.setPartitionState(pId, state);
    ctx.setPartitionFinishTime(pId, System.currentTimeMillis());
    if (incrementAttempts) {
      ctx.incrementNumAttempts(pId);
    }
  }

  protected static void markAllPartitionsError(JobContext ctx, TaskPartitionState state,
      boolean incrementAttempts) {
    for (int pId : ctx.getPartitionSet()) {
      markPartitionError(ctx, pId, state, incrementAttempts);
    }
  }

  protected static void markPartitionDelayed(JobConfig cfg, JobContext ctx, int p) {
    long delayInterval = cfg.getTaskRetryDelay();
    if (delayInterval <= 0) {
      return;
    }
    long nextStartTime = ctx.getPartitionFinishTime(p) + delayInterval;
    ctx.setNextRetryTime(p, nextStartTime);
  }

  protected void handleJobTimeout(JobContext jobCtx, WorkflowContext workflowCtx,
      String jobResource, JobConfig jobCfg) {
    jobCtx.setFinishTime(System.currentTimeMillis());
    workflowCtx.setJobState(jobResource, TaskState.TIMED_OUT);
    // Mark all INIT task to TASK_ABORTED
    for (int pId : jobCtx.getPartitionSet()) {
      if (jobCtx.getPartitionState(pId) == TaskPartitionState.INIT) {
        jobCtx.setPartitionState(pId, TaskPartitionState.TASK_ABORTED);
      }
    }
    _clusterStatusMonitor.updateJobCounters(jobCfg, TaskState.TIMED_OUT);
    _rebalanceScheduler.removeScheduledRebalance(jobResource);
    TaskUtil.cleanupJobIdealStateExtView(_manager.getHelixDataAccessor(), jobResource);
  }

  protected void failJob(String jobName, WorkflowContext workflowContext, JobContext jobContext,
      WorkflowConfig workflowConfig, Map<String, JobConfig> jobConfigMap) {
    markJobFailed(jobName, jobContext, workflowConfig, workflowContext, jobConfigMap);
    // Mark all INIT task to TASK_ABORTED
    for (int pId : jobContext.getPartitionSet()) {
      if (jobContext.getPartitionState(pId) == TaskPartitionState.INIT) {
        jobContext.setPartitionState(pId, TaskPartitionState.TASK_ABORTED);
      }
    }
    _clusterStatusMonitor.updateJobCounters(jobConfigMap.get(jobName), TaskState.FAILED);
    _rebalanceScheduler.removeScheduledRebalance(jobName);
    TaskUtil.cleanupJobIdealStateExtView(_manager.getHelixDataAccessor(), jobName);
  }

  // Compute real assignment from theoretical calculation with applied throttling or other logics
  protected void handleAdditionalTaskAssignment(
      Map<String, SortedSet<Integer>> prevInstanceToTaskAssignments, Set<String> excludedInstances,
      String jobResource, CurrentStateOutput currStateOutput, JobContext jobCtx, JobConfig jobCfg,
      WorkflowConfig workflowConfig, WorkflowContext workflowCtx, ClusterDataCache cache,
      ResourceAssignment prevTaskToInstanceStateAssignment, Set<Integer> assignedPartitions,
      Map<Integer, PartitionAssignment> paMap, Set<Integer> skippedPartitions,
      TaskAssignmentCalculator taskAssignmentCal, Set<Integer> allPartitions, long currentTime,
      Collection<String> liveInstances) {
    // The excludeSet contains the set of task partitions that must be excluded from consideration
    // when making any new assignments.
    // This includes all completed, failed, delayed, and already assigned partitions.
    Set<Integer> excludeSet = Sets.newTreeSet(assignedPartitions);
    addCompletedTasks(excludeSet, jobCtx, allPartitions);
    addGiveupPartitions(excludeSet, jobCtx, allPartitions, jobCfg);
    excludeSet.addAll(skippedPartitions);
    excludeSet.addAll(TaskUtil.getNonReadyPartitions(jobCtx, currentTime));
    // Get instance->[partition, ...] mappings for the target resource.
    Map<String, SortedSet<Integer>> tgtPartitionAssignments = taskAssignmentCal
        .getTaskAssignment(currStateOutput, prevTaskToInstanceStateAssignment, liveInstances,
            jobCfg, jobCtx, workflowConfig, workflowCtx, allPartitions, cache.getIdealStates());

    if (!TaskUtil.isGenericTaskJob(jobCfg) || jobCfg.isRebalanceRunningTask()) {
      dropRebalancedRunningTasks(tgtPartitionAssignments, prevInstanceToTaskAssignments, paMap,
          jobCtx);
    }

    for (Map.Entry<String, SortedSet<Integer>> entry : prevInstanceToTaskAssignments.entrySet()) {
      String instance = entry.getKey();
      if (!tgtPartitionAssignments.containsKey(instance) || excludedInstances.contains(instance)) {
        continue;
      }
      // 1. throttled by job configuration
      // Contains the set of task partitions currently assigned to the instance.
      Set<Integer> pSet = entry.getValue();
      int jobCfgLimitation = jobCfg.getNumConcurrentTasksPerInstance() - pSet.size();
      // 2. throttled by participant capacity
      int participantCapacity = cache.getInstanceConfigMap().get(instance).getMaxConcurrentTask();
      if (participantCapacity == InstanceConfig.MAX_CONCURRENT_TASK_NOT_SET) {
        participantCapacity = cache.getClusterConfig().getMaxConcurrentTaskPerInstance();
      }
      int participantLimitation =
          participantCapacity - cache.getParticipantActiveTaskCount(instance);
      // New tasks to be assigned
      int numToAssign = Math.min(jobCfgLimitation, participantLimitation);
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format(
            "Throttle tasks to be assigned to instance %s using limitation: Job Concurrent Task(%d), "
                + "Participant Max Task(%d). Remaining capacity %d.", instance, jobCfgLimitation,
            participantCapacity, numToAssign));
      }
      if (numToAssign > 0) {
        Set<Integer> throttledSet = new HashSet<Integer>();
        List<Integer> nextPartitions =
            getNextPartitions(tgtPartitionAssignments.get(instance), excludeSet, throttledSet,
                numToAssign);
        for (Integer pId : nextPartitions) {
          String pName = pName(jobResource, pId);
          paMap.put(pId, new PartitionAssignment(instance, TaskPartitionState.RUNNING.name()));
          excludeSet.add(pId);
          jobCtx.setAssignedParticipant(pId, instance);
          jobCtx.setPartitionState(pId, TaskPartitionState.INIT);
          jobCtx.setPartitionStartTime(pId, System.currentTimeMillis());
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Setting task partition %s state to %s on instance %s.", pName,
                TaskPartitionState.RUNNING, instance));
          }
        }
        cache.setParticipantActiveTaskCount(instance,
            cache.getParticipantActiveTaskCount(instance) + nextPartitions.size());
        if (!throttledSet.isEmpty()) {
          LOG.debug(
              throttledSet.size() + "tasks are ready but throttled when assigned to participant.");
        }
      }
    }
  }

  protected void scheduleForNextTask(String job, JobContext jobCtx, long now) {
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
      long scheduledTime = _rebalanceScheduler.getRebalanceTime(job);
      if (scheduledTime == -1 || earliestTime < scheduledTime) {
        _rebalanceScheduler.scheduleRebalance(_manager, job, earliestTime);
      }
    }
  }



  // add all partitions that have been tried maxNumberAttempts
  protected static void addGiveupPartitions(Set<Integer> set, JobContext ctx, Iterable<Integer> pIds,
      JobConfig cfg) {
    for (Integer pId : pIds) {
      if (isTaskGivenup(ctx, cfg, pId)) {
        set.add(pId);
      }
    }
  }

  private static List<Integer> getNextPartitions(SortedSet<Integer> candidatePartitions,
      Set<Integer> excluded, Set<Integer> throttled, int n) {
    List<Integer> result = new ArrayList<Integer>();
    for (Integer pId : candidatePartitions) {
      if (!excluded.contains(pId)) {
        if (result.size() < n) {
          result.add(pId);
        } else {
          throttled.add(pId);
        }
      }
    }
    return result;
  }

  private static void addCompletedTasks(Set<Integer> set, JobContext ctx, Iterable<Integer> pIds) {
    for (Integer pId : pIds) {
      TaskPartitionState state = ctx.getPartitionState(pId);
      if (state == TaskPartitionState.COMPLETED) {
        set.add(pId);
      }
    }
  }

  protected static boolean isTaskGivenup(JobContext ctx, JobConfig cfg, int pId) {
    TaskPartitionState state = ctx.getPartitionState(pId);
    if (state == TaskPartitionState.TASK_ABORTED || state == TaskPartitionState.ERROR) {
      return true;
    }
    if (state == TaskPartitionState.TIMED_OUT || state == TaskPartitionState.TASK_ERROR) {
      return ctx.getPartitionNumAttempts(pId) >= cfg.getMaxAttemptsPerTask();
    }
    return false;
  }


  /**
   * If assignment is different from previous assignment, drop the old running task if it's no
   * longer assigned to the same instance, but not removing it from excludeSet because the same task
   * should not be assigned to the new instance right away.
   */
  private void dropRebalancedRunningTasks(Map<String, SortedSet<Integer>> newAssignment,
      Map<String, SortedSet<Integer>> oldAssignment, Map<Integer, PartitionAssignment> paMap,
      JobContext jobContext) {
    for (String instance : oldAssignment.keySet()) {
      for (Integer pId : oldAssignment.get(instance)) {
        if (jobContext.getPartitionState(pId) == TaskPartitionState.RUNNING && !newAssignment
            .get(instance).contains(pId)) {
          paMap.put(pId, new PartitionAssignment(instance, TaskPartitionState.DROPPED.name()));
          jobContext.setPartitionState(pId, TaskPartitionState.DROPPED);
        }
      }
    }
  }

  protected void markJobComplete(String jobName, JobContext jobContext,
      WorkflowConfig workflowConfig, WorkflowContext workflowContext,
      Map<String, JobConfig> jobConfigMap) {
    long currentTime = System.currentTimeMillis();
    workflowContext.setJobState(jobName, TaskState.COMPLETED);
    jobContext.setFinishTime(currentTime);
    if (isWorkflowFinished(workflowContext, workflowConfig, jobConfigMap)) {
      workflowContext.setFinishTime(currentTime);
      updateWorkflowMonitor(workflowContext, workflowConfig);
    }
    scheduleJobCleanUp(jobConfigMap.get(jobName), workflowConfig, currentTime);
  }

  protected void markJobFailed(String jobName, JobContext jobContext, WorkflowConfig workflowConfig,
      WorkflowContext workflowContext, Map<String, JobConfig> jobConfigMap) {
    long currentTime = System.currentTimeMillis();
    workflowContext.setJobState(jobName, TaskState.FAILED);
    if (jobContext != null) {
      jobContext.setFinishTime(currentTime);
    }
    if (isWorkflowFinished(workflowContext, workflowConfig, jobConfigMap)) {
      workflowContext.setFinishTime(currentTime);
      updateWorkflowMonitor(workflowContext, workflowConfig);
    }
    scheduleJobCleanUp(jobConfigMap.get(jobName), workflowConfig, currentTime);
  }

  protected void scheduleJobCleanUp(JobConfig jobConfig, WorkflowConfig workflowConfig,
      long currentTime) {
    long currentScheduledTime =
        _rebalanceScheduler.getRebalanceTime(workflowConfig.getWorkflowId()) == -1
            ? Long.MAX_VALUE
            : _rebalanceScheduler.getRebalanceTime(workflowConfig.getWorkflowId());
    if (currentTime + jobConfig.getExpiry() < currentScheduledTime) {
      _rebalanceScheduler.scheduleRebalance(_manager, workflowConfig.getWorkflowId(),
          currentTime + jobConfig.getExpiry());
    }
  }




  // Workflow related methods

  /**
   * Checks if the workflow has finished (either completed or failed).
   * Set the state in workflow context properly.
   *
   * @param ctx Workflow context containing job states
   * @param cfg Workflow config containing set of jobs
   * @return returns true if the workflow
   *            1. completed (all tasks are {@link TaskState#COMPLETED})
   *            2. failed (any task is {@link TaskState#FAILED}
   *            3. workflow is {@link TaskState#TIMED_OUT}
   *         returns false otherwise.
   */
  protected boolean isWorkflowFinished(WorkflowContext ctx, WorkflowConfig cfg,
      Map<String, JobConfig> jobConfigMap) {
    boolean incomplete = false;

    TaskState workflowState = ctx.getWorkflowState();
    if (TaskState.TIMED_OUT.equals(workflowState)) {
      // We don't update job state here as JobRebalancer will do it
      return true;
    }

    // Check if failed job count is beyond threshold and if so, fail the workflow
    // and abort in-progress jobs
    int failedJobs = 0;
    for (String job : cfg.getJobDag().getAllNodes()) {
      TaskState jobState = ctx.getJobState(job);
      if (jobState == TaskState.FAILED || jobState == TaskState.TIMED_OUT) {
        failedJobs++;
        if (!cfg.isJobQueue() && failedJobs > cfg.getFailureThreshold()) {
          ctx.setWorkflowState(TaskState.FAILED);
          for (String jobToFail : cfg.getJobDag().getAllNodes()) {
            if (ctx.getJobState(jobToFail) == TaskState.IN_PROGRESS) {
              ctx.setJobState(jobToFail, TaskState.ABORTED);
              // Skip aborted jobs latency since they are not accurate latency for job running time
              if (_clusterStatusMonitor != null) {
                _clusterStatusMonitor
                    .updateJobCounters(jobConfigMap.get(jobToFail), TaskState.ABORTED);
              }
            }
          }
          return true;
        }
      }
      if (jobState != TaskState.COMPLETED && jobState != TaskState.FAILED
          && jobState != TaskState.TIMED_OUT) {
        incomplete = true;
      }
    }

    if (!incomplete && cfg.isTerminable()) {
      ctx.setWorkflowState(TaskState.COMPLETED);
      return true;
    }

    return false;
  }

  protected void updateWorkflowMonitor(WorkflowContext context,
      WorkflowConfig config) {
    if (_clusterStatusMonitor != null) {
      _clusterStatusMonitor.updateWorkflowCounters(config, context.getWorkflowState(),
          context.getFinishTime() - context.getStartTime());
    }
  }


  // Common methods

  protected Set<String> getExcludedInstances(String currentJobName, WorkflowConfig workflowCfg,
      ClusterDataCache cache) {
    Set<String> ret = new HashSet<String>();

    if (!workflowCfg.isAllowOverlapJobAssignment()) {
      // exclude all instances that has been assigned other jobs' tasks
      for (String jobName : workflowCfg.getJobDag().getAllNodes()) {
        if (jobName.equals(currentJobName)) {
          continue;
        }
        JobContext jobContext = cache.getJobContext(jobName);
        if (jobContext == null) {
          continue;
        }
        for (int pId : jobContext.getPartitionSet()) {
          TaskPartitionState partitionState = jobContext.getPartitionState(pId);
          if (partitionState == TaskPartitionState.INIT
              || partitionState == TaskPartitionState.RUNNING) {
            ret.add(jobContext.getAssignedParticipant(pId));
          }
        }
      }
    }
    return ret;
  }

  /**
   * Schedule the rebalancer timer for task framework elements
   * @param resourceId       The resource id
   * @param startTime        The resource start time
   * @param timeoutPeriod    The resource timeout period. Will be -1 if it is not set.
   */
  protected void scheduleRebalanceForTimeout(String resourceId, long startTime,
      long timeoutPeriod) {
    long nextTimeout = getTimeoutTime(startTime, timeoutPeriod);
    long nextRebalanceTime = _rebalanceScheduler.getRebalanceTime(resourceId);
    if (nextTimeout >= System.currentTimeMillis() && (
        nextRebalanceTime == TaskConstants.DEFAULT_NEVER_TIMEOUT
            || nextTimeout < nextRebalanceTime)) {
      _rebalanceScheduler.scheduleRebalance(_manager, resourceId, nextTimeout);
    }
  }

  /**
   * Basic function to check task framework resources, workflow and job, are timeout
   * @param startTime       Resources start time
   * @param timeoutPeriod   Resources timeout period. Will be -1 if it is not set.
   * @return
   */
  protected boolean isTimeout(long startTime, long timeoutPeriod) {
    long nextTimeout = getTimeoutTime(startTime, timeoutPeriod);
    return nextTimeout != TaskConstants.DEFAULT_NEVER_TIMEOUT && nextTimeout <= System
        .currentTimeMillis();
  }

  private long getTimeoutTime(long startTime, long timeoutPeriod) {
    return (timeoutPeriod == TaskConstants.DEFAULT_NEVER_TIMEOUT
        || timeoutPeriod > Long.MAX_VALUE - startTime) // check long overflow
        ? TaskConstants.DEFAULT_NEVER_TIMEOUT : startTime + timeoutPeriod;
  }


  /**
   * Set the ClusterStatusMonitor for metrics update
   */
  public void setClusterStatusMonitor(ClusterStatusMonitor clusterStatusMonitor) {
    _clusterStatusMonitor = clusterStatusMonitor;
  }
}
