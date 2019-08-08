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

import com.google.common.collect.Lists;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.common.caches.TaskDataCache;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.model.builder.IdealStateBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkflowDispatcher extends AbstractTaskDispatcher {
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowDispatcher.class);
  private static final Set<TaskState> finalStates = new HashSet<>(
      Arrays.asList(TaskState.COMPLETED, TaskState.FAILED, TaskState.ABORTED, TaskState.TIMED_OUT));
  private WorkflowControllerDataProvider _clusterDataCache;
  private JobDispatcher _jobDispatcher;

  public void updateCache(WorkflowControllerDataProvider cache) {
    _clusterDataCache = cache;
    if (_jobDispatcher == null) {
      _jobDispatcher = new JobDispatcher();
    }
    _jobDispatcher.init(_manager);
    _jobDispatcher.updateCache(cache);
    _jobDispatcher.setClusterStatusMonitor(_clusterStatusMonitor);
  }

  // Split it into status update and assign. But there are couple of data need
  // to pass around.
  public void updateWorkflowStatus(String workflow, WorkflowConfig workflowCfg,
      WorkflowContext workflowCtx, CurrentStateOutput currentStateOutput,
      BestPossibleStateOutput bestPossibleOutput) {

    // Fetch workflow configuration and context
    if (workflowCfg == null) {
      LOG.warn("Workflow configuration is NULL for " + workflow);
      return;
    }

    // Step 1: Check for deletion - if so, we don't need to go through further steps
    // Clean up if workflow marked for deletion
    TargetState targetState = workflowCfg.getTargetState();
    if (targetState == TargetState.DELETE) {
      LOG.info("Workflow is marked as deleted " + workflow + " cleaning up the workflow context.");
      cleanupWorkflow(workflow);
      return;
    }

    // Step 2: handle timeout, which should have higher priority than STOP
    // Only generic workflow get timeouted and schedule rebalance for timeout. Will skip the set if
    // the workflow already got timeouted. Job Queue will ignore the setup.
    if (!workflowCfg.isJobQueue() && !finalStates.contains(workflowCtx.getWorkflowState())) {
      // If timeout point has already been passed, it will not be scheduled
      scheduleRebalanceForTimeout(workflow, workflowCtx.getStartTime(), workflowCfg.getTimeout());

      if (!TaskState.TIMED_OUT.equals(workflowCtx.getWorkflowState())
          && isTimeout(workflowCtx.getStartTime(), workflowCfg.getTimeout())) {
        workflowCtx.setWorkflowState(TaskState.TIMED_OUT);
        _clusterDataCache.updateWorkflowContext(workflow, workflowCtx);
      }

      // We should not return after setting timeout, as in case the workflow is stopped already
      // marking it timeout will not trigger rebalance pipeline as we are not listening on
      // PropertyStore change, nor will we schedule rebalance for timeout as at this point,
      // workflow is already timed-out. We should let the code proceed and wait for schedule
      // future cleanup work
    }

    // Step 3: handle workflow that should STOP
    // For workflows that already reached final states, STOP should not take into effect
    if (!finalStates.contains(workflowCtx.getWorkflowState())
        && TargetState.STOP.equals(targetState)) {
      LOG.info("Workflow " + workflow + "is marked as stopped.");
      if (isWorkflowStopped(workflowCtx, workflowCfg)) {
        workflowCtx.setWorkflowState(TaskState.STOPPED);
        _clusterDataCache.updateWorkflowContext(workflow, workflowCtx);
      }
      return;
    }

    long currentTime = System.currentTimeMillis();

    // Step 4: Check and process finished workflow context (confusing,
    // but its inside isWorkflowFinished())
    // Check if workflow has been finished and mark it if it is. Also update cluster status
    // monitor if provided
    // Note that COMPLETE and FAILED will be marked in markJobComplete / markJobFailed
    // This is to handle TIMED_OUT only
    if (workflowCtx.getFinishTime() == WorkflowContext.UNFINISHED && isWorkflowFinished(workflowCtx,
        workflowCfg, _clusterDataCache.getJobConfigMap(), _clusterDataCache)) {
      workflowCtx.setFinishTime(currentTime);
      updateWorkflowMonitor(workflowCtx, workflowCfg);
      _clusterDataCache.updateWorkflowContext(workflow, workflowCtx);
    }

    // Step 5: Handle finished workflows
    if (workflowCtx.getFinishTime() != WorkflowContext.UNFINISHED) {
      LOG.info("Workflow " + workflow + " is finished.");
      long expiryTime = workflowCfg.getExpiry();
      // Check if this workflow has been finished past its expiry.
      if (workflowCtx.getFinishTime() + expiryTime <= currentTime) {
        LOG.info("Workflow " + workflow + " passed expiry time, cleaning up the workflow context.");
        cleanupWorkflow(workflow);
      } else {
        // schedule future cleanup work
        long cleanupTime = workflowCtx.getFinishTime() + expiryTime;
        _rebalanceScheduler.scheduleRebalance(_manager, workflow, cleanupTime);
      }
      return;
    }

    if (!workflowCfg.isTerminable() || workflowCfg.isJobQueue()) {
      Set<String> jobWithFinalStates = new HashSet<>(workflowCtx.getJobStates().keySet());
      jobWithFinalStates.removeAll(workflowCfg.getJobDag().getAllNodes());
      if (jobWithFinalStates.size() > 0) {
        workflowCtx.setLastJobPurgeTime(System.currentTimeMillis());
        workflowCtx.removeJobStates(jobWithFinalStates);
        workflowCtx.removeJobStartTime(jobWithFinalStates);
      }
    }

    // Update jobs already inflight
    RuntimeJobDag runtimeJobDag = _clusterDataCache.getTaskDataCache().getRuntimeJobDag(workflow);
    if (runtimeJobDag != null) {
      for (String inflightJob : runtimeJobDag.getInflightJobList()) {
        processJob(inflightJob, currentStateOutput, bestPossibleOutput, workflowCtx);
      }
    } else {
      LOG.warn(String.format(
          "Failed to find runtime job DAG for workflow %s, existing runtime jobs may not be processed correctly for it",
          workflow));
    }

    _clusterDataCache.updateWorkflowContext(workflow, workflowCtx);
  }

  public void assignWorkflow(String workflow, WorkflowConfig workflowCfg,
      WorkflowContext workflowCtx, CurrentStateOutput currentStateOutput,
      BestPossibleStateOutput bestPossibleOutput) {
    // Fetch workflow configuration and context
    if (workflowCfg == null) {
      // Already logged in status update.
      return;
    }

    if (!isWorkflowReadyForSchedule(workflowCfg)) {
      LOG.info("Workflow " + workflow + " is not ready to schedule");
      // set the timer to trigger future schedule
      _rebalanceScheduler.scheduleRebalance(_manager, workflow,
          workflowCfg.getStartTime().getTime());
      return;
    }

    // Check for readiness, and stop processing if it's not ready
    boolean isReady = scheduleWorkflowIfReady(workflow, workflowCfg, workflowCtx,
        _clusterDataCache.getTaskDataCache());
    if (isReady) {
      // Schedule jobs from this workflow.
      scheduleJobs(workflow, workflowCfg, workflowCtx, _clusterDataCache.getJobConfigMap(),
          _clusterDataCache, currentStateOutput, bestPossibleOutput);
    } else {
      LOG.debug("Workflow " + workflow + " is not ready to be scheduled.");
    }
    _clusterDataCache.updateWorkflowContext(workflow, workflowCtx);
  }

  public WorkflowContext getOrInitializeWorkflowContext(String workflowName, TaskDataCache cache) {
    WorkflowContext workflowCtx = cache.getWorkflowContext(workflowName);
    if (workflowCtx == null) {
      workflowCtx = new WorkflowContext(new ZNRecord(TaskUtil.WORKFLOW_CONTEXT_KW));
      workflowCtx.setStartTime(System.currentTimeMillis());
      workflowCtx.setName(workflowName);
      LOG.debug("Workflow context is created for " + workflowName);
    }
    return workflowCtx;
  }

  /**
   * Figure out whether the jobs in the workflow should be run,
   * and if it's ready, then just schedule it
   */
  private void scheduleJobs(String workflow, WorkflowConfig workflowCfg,
      WorkflowContext workflowCtx, Map<String, JobConfig> jobConfigMap,
      WorkflowControllerDataProvider clusterDataCache, CurrentStateOutput currentStateOutput,
      BestPossibleStateOutput bestPossibleOutput) {
    ScheduleConfig scheduleConfig = workflowCfg.getScheduleConfig();
    if (scheduleConfig != null && scheduleConfig.isRecurring()) {
      LOG.debug("Jobs from recurring workflow are not schedule-able");
      return;
    }

    int inCompleteAllJobCount = TaskUtil.getInCompleteJobCount(workflowCfg, workflowCtx);
    int scheduledJobs = 0;
    long timeToSchedule = Long.MAX_VALUE;
    JobDag jobDag = clusterDataCache.getTaskDataCache().getRuntimeJobDag(workflow);
    if (jobDag == null) {
      jobDag = workflowCfg.getJobDag();
    }

    String nextJob = jobDag.getNextJob();
    // Assign new jobs
    while (nextJob != null) {
      String job = nextJob;
      TaskState jobState = workflowCtx.getJobState(job);
      if (jobState != null && !jobState.equals(TaskState.NOT_STARTED)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Job " + job + " is already started or completed.");
        }
        processJob(job, currentStateOutput, bestPossibleOutput, workflowCtx);
        nextJob = jobDag.getNextJob();
        continue;
      }

      if (workflowCfg.isJobQueue() && scheduledJobs >= workflowCfg.getParallelJobs()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Workflow %s already have enough job in progress, "
              + "scheduledJobs(s)=%d, stop scheduling more jobs", workflow, scheduledJobs));
        }
        break;
      }

      // TODO: Part of isJobReadyToSchedule() is already done by RuntimeJobDag. Because there is
      // some duplicate logic, consider refactoring. The check here and the ready-list in
      // RuntimeJobDag may cause conflicts.
      // check ancestor job status
      if (isJobReadyToSchedule(job, workflowCfg, workflowCtx, inCompleteAllJobCount, jobConfigMap,
          clusterDataCache, clusterDataCache.getAssignableInstanceManager())) {
        JobConfig jobConfig = jobConfigMap.get(job);

        // Since the start time is calculated base on the time of completion of parent jobs for this
        // job, the calculated start time should only be calculate once. Persist the calculated time
        // in WorkflowContext znode.
        long calculatedStartTime = workflowCtx.getJobStartTime(job);
        if (calculatedStartTime < 0) {
          // Calculate the start time if it is not already calculated
          calculatedStartTime = System.currentTimeMillis();
          // If the start time is not calculated before, do the math.
          if (jobConfig.getExecutionDelay() >= 0) {
            calculatedStartTime += jobConfig.getExecutionDelay();
          }
          calculatedStartTime = Math.max(calculatedStartTime, jobConfig.getExecutionStart());
          workflowCtx.setJobStartTime(job, calculatedStartTime);
        }

        // Time is not ready. Set a trigger and update the start time.
        if (System.currentTimeMillis() < calculatedStartTime) {
          timeToSchedule = Math.min(timeToSchedule, calculatedStartTime);
        } else {
          scheduleSingleJob(job, jobConfig);
          workflowCtx.setJobState(job, TaskState.NOT_STARTED);
          processJob(job, currentStateOutput, bestPossibleOutput, workflowCtx);
          scheduledJobs++;
        }
      }
      nextJob = jobDag.getNextJob();
    }

    long currentScheduledTime =
        _rebalanceScheduler.getRebalanceTime(workflow) == -1 ? Long.MAX_VALUE
            : _rebalanceScheduler.getRebalanceTime(workflow);
    if (timeToSchedule < currentScheduledTime) {
      _rebalanceScheduler.scheduleRebalance(_manager, workflow, timeToSchedule);
    }
  }

  private void processJob(String job, CurrentStateOutput currentStateOutput,
      BestPossibleStateOutput bestPossibleOutput, WorkflowContext workflowCtx) {
    _clusterDataCache.getTaskDataCache().dispatchJob(job);
    try {
      ResourceAssignment resourceAssignment =
          _jobDispatcher.processJobStatusUpdateAndAssignment(job, currentStateOutput, workflowCtx);
      updateBestPossibleStateOutput(job, resourceAssignment, bestPossibleOutput);
    } catch (Exception e) {
      LogUtil.logWarn(LOG, _clusterDataCache.getClusterEventId(),
          String.format("Failed to compute job assignment for job %s", job));
    }
  }

  /**
   * Posts new job to cluster
   */
  private void scheduleSingleJob(String jobResource, JobConfig jobConfig) {
    HelixAdmin admin = _manager.getClusterManagmentTool();

    IdealState jobIS = admin.getResourceIdealState(_manager.getClusterName(), jobResource);
    if (jobIS != null) {
      LOG.info("Job " + jobResource + " idealstate already exists!");
      return;
    }

    // Set up job resource based on partitions from target resource

    // Create the UserContentStore for the job first
    TaskUtil.createUserContent(_manager.getHelixPropertyStore(), jobResource,
        new ZNRecord(TaskUtil.USER_CONTENT_NODE));

    int numPartitions = jobConfig.getTaskConfigMap().size();
    if (numPartitions == 0) {
      IdealState targetIs =
          admin.getResourceIdealState(_manager.getClusterName(), jobConfig.getTargetResource());
      if (targetIs == null) {
        LOG.warn("Target resource does not exist for job " + jobResource);
        // do not need to fail here, the job will be marked as failure immediately when job starts
        // running.
      } else {
        numPartitions = targetIs.getPartitionSet().size();
      }
    }

    admin.addResource(_manager.getClusterName(), jobResource, numPartitions,
        TaskConstants.STATE_MODEL_NAME);

    HelixDataAccessor accessor = _manager.getHelixDataAccessor();

    // Set the job configuration
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    HelixProperty resourceConfig = new HelixProperty(jobResource);
    resourceConfig.getRecord().getSimpleFields().putAll(jobConfig.getResourceConfigMap());
    Map<String, TaskConfig> taskConfigMap = jobConfig.getTaskConfigMap();
    if (taskConfigMap != null) {
      for (TaskConfig taskConfig : taskConfigMap.values()) {
        resourceConfig.getRecord().setMapField(taskConfig.getId(), taskConfig.getConfigMap());
      }
    }
    accessor.setProperty(keyBuilder.resourceConfig(jobResource), resourceConfig);

    // Push out new ideal state based on number of target partitions
    IdealStateBuilder builder = new CustomModeISBuilder(jobResource);
    builder.setRebalancerMode(IdealState.RebalanceMode.TASK);
    builder.setNumReplica(1);
    builder.setNumPartitions(numPartitions);
    builder.setStateModel(TaskConstants.STATE_MODEL_NAME);

    if (jobConfig.getInstanceGroupTag() != null) {
      builder.setNodeGroup(jobConfig.getInstanceGroupTag());
    }

    if (jobConfig.isDisableExternalView()) {
      builder.disableExternalView();
    }

    jobIS = builder.build();
    for (int i = 0; i < numPartitions; i++) {
      jobIS.getRecord().setListField(jobResource + "_" + i, new ArrayList<>());
      jobIS.getRecord().setMapField(jobResource + "_" + i, new HashMap<>());
    }
    jobIS.setRebalancerClassName(JobRebalancer.class.getName());
    admin.setResourceIdealState(_manager.getClusterName(), jobResource, jobIS);
  }

  /**
   * Check if a workflow is ready to schedule, and schedule a rebalance if it is not
   * @param workflow the Helix resource associated with the workflow
   * @param workflowCfg the workflow to check
   * @param workflowCtx the current workflow context
   * @return true if the workflow is ready for schedule, false if not ready
   */
  private boolean scheduleWorkflowIfReady(String workflow, WorkflowConfig workflowCfg,
      WorkflowContext workflowCtx, TaskDataCache cache) {
    // non-scheduled workflow is ready to run immediately.
    if (workflowCfg == null || workflowCfg.getScheduleConfig() == null) {
      return true;
    }

    // Figure out when this should be run, and if it's ready, then just run it
    ScheduleConfig scheduleConfig = workflowCfg.getScheduleConfig();
    Date startTime = scheduleConfig.getStartTime();
    long currentTime = new Date().getTime();
    long delayFromStart = startTime.getTime() - currentTime;

    if (delayFromStart <= 0) {
      // Recurring workflows are just templates that spawn new workflows
      if (scheduleConfig.isRecurring()) {
        // Skip scheduling this workflow if it's not in a start state
        if (!workflowCfg.getTargetState().equals(TargetState.START)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Skip scheduling since the workflow has not been started " + workflow);
          }
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
        DateFormat df = new SimpleDateFormat("yyyyMMdd'T'HHmmss");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        String newWorkflowName = workflow + "_" + df.format(new Date(timeToSchedule));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Ready to start workflow " + newWorkflowName);
        }
        if (!newWorkflowName.equals(lastScheduled)) {
          Workflow clonedWf =
              cloneWorkflow(_manager, workflow, newWorkflowName, new Date(timeToSchedule));
          TaskDriver driver = new TaskDriver(_manager);
          if (clonedWf != null) {
            try {
              // Start the cloned workflow
              driver.start(clonedWf);
            } catch (Exception e) {
              LOG.error("Failed to schedule cloned workflow " + newWorkflowName, e);
              _clusterStatusMonitor.updateWorkflowCounters(clonedWf.getWorkflowConfig(),
                  TaskState.FAILED);
            }
          }
          // Persist workflow start regardless of success to avoid retrying and failing
          workflowCtx.setLastScheduledSingleWorkflow(newWorkflowName);
        }

        // Change the time to trigger the pipeline to that of the next run
        _rebalanceScheduler.scheduleRebalance(_manager, workflow, (timeToSchedule + period));
      } else {
        // one time workflow.
        // Remove any timers that are past-time for this workflowg
        long scheduledTime = _rebalanceScheduler.getRebalanceTime(workflow);
        if (scheduledTime > 0 && currentTime > scheduledTime) {
          _rebalanceScheduler.removeScheduledRebalance(workflow);
        }
        return true;
      }
    } else {
      // set the timer to trigger future schedule
      _rebalanceScheduler.scheduleRebalance(_manager, workflow, startTime.getTime());
    }

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
  public static Workflow cloneWorkflow(HelixManager manager, String origWorkflowName,
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
    Map<String, String> workflowConfigsMap =
        resourceConfigMap.get(origWorkflowName).getRecord().getSimpleFields();
    WorkflowConfig.Builder workflowConfigBlder = WorkflowConfig.Builder.fromMap(workflowConfigsMap);

    // Set the schedule, if applicable
    if (newStartTime != null) {
      ScheduleConfig scheduleConfig = ScheduleConfig.oneTimeDelayedStart(newStartTime);
      workflowConfigBlder.setScheduleConfig(scheduleConfig);
    }
    workflowConfigBlder.setTerminable(true);

    WorkflowConfig workflowConfig = workflowConfigBlder.build();

    JobDag jobDag = workflowConfig.getJobDag();
    Map<String, Set<String>> parentsToChildren = jobDag.getParentsToChildren();

    Workflow.Builder workflowBuilder = new Workflow.Builder(newWorkflowName);
    workflowBuilder.setWorkflowConfig(workflowConfig);

    // Add each job back as long as the original exists
    Set<String> namespacedJobs = jobDag.getAllNodes();
    for (String namespacedJob : namespacedJobs) {
      if (resourceConfigMap.containsKey(namespacedJob)) {
        // Copy over job-level and task-level configs
        String job = TaskUtil.getDenamespacedJobName(origWorkflowName, namespacedJob);
        HelixProperty jobConfig = resourceConfigMap.get(namespacedJob);
        Map<String, String> jobSimpleFields = jobConfig.getRecord().getSimpleFields();

        JobConfig.Builder jobCfgBuilder = JobConfig.Builder.fromMap(jobSimpleFields);

        jobCfgBuilder.setWorkflow(newWorkflowName); // overwrite workflow name
        Map<String, Map<String, String>> rawTaskConfigMap = jobConfig.getRecord().getMapFields();
        List<TaskConfig> taskConfigs = Lists.newLinkedList();
        for (Map<String, String> rawTaskConfig : rawTaskConfigMap.values()) {
          TaskConfig taskConfig = TaskConfig.Builder.from(rawTaskConfig);
          taskConfigs.add(taskConfig);
        }
        jobCfgBuilder.addTaskConfigs(taskConfigs);
        workflowBuilder.addJob(job, jobCfgBuilder);

        // Add dag dependencies
        Set<String> children = parentsToChildren.get(namespacedJob);
        if (children != null) {
          for (String namespacedChild : children) {
            String child = TaskUtil.getDenamespacedJobName(origWorkflowName, namespacedChild);
            workflowBuilder.addParentChildDependency(job, child);
          }
        }
      }
    }
    return workflowBuilder.build();
  }

  /**
   * Clean up a workflow. This removes the workflow config, idealstate, externalview and workflow
   * contexts associated with this workflow, and all jobs information, including their configs,
   * context, IS and EV.
   */
  private void cleanupWorkflow(String workflow) {
    LOG.info("Cleaning up workflow: " + workflow);
    WorkflowConfig workflowcfg = _clusterDataCache.getWorkflowConfig(workflow);

    if (workflowcfg.isTerminable() || workflowcfg.getTargetState() == TargetState.DELETE) {
      Set<String> jobs = workflowcfg.getJobDag().getAllNodes();
      // Remove all pending timer tasks for this workflow if exists
      _rebalanceScheduler.removeScheduledRebalance(workflow);
      for (String job : jobs) {
        _rebalanceScheduler.removeScheduledRebalance(job);
      }
      if (!TaskUtil.removeWorkflow(_manager.getHelixDataAccessor(),
          _manager.getHelixPropertyStore(), workflow, jobs)) {
        LOG.warn("Failed to clean up workflow " + workflow);
      } else {
        // Only remove from cache when remove all workflow success. Otherwise, batch write will
        // clean all the contexts even if Configs and IdealStates are exists. Then all the workflows
        // and jobs will rescheduled again.
        removeContextsAndPreviousAssignment(workflow, jobs, _clusterDataCache.getTaskDataCache());
      }
    } else {
      LOG.info("Did not clean up workflow " + workflow
          + " because neither the workflow is non-terminable nor is set to DELETE.");
    }
  }

  private void removeContextsAndPreviousAssignment(String workflow, Set<String> jobs,
      TaskDataCache cache) {
    if (jobs != null) {
      for (String job : jobs) {
        cache.removeContext(job);
        cache.removePrevAssignment(job);
      }
    }
    cache.removeContext(workflow);
  }
}
