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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import com.google.common.collect.Lists;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.model.builder.IdealStateBuilder;
import org.apache.log4j.Logger;


/**
 * Custom rebalancer implementation for the {@code Workflow} in task state model.
 */
public class WorkflowRebalancer extends TaskRebalancer {
  private static final Logger LOG = Logger.getLogger(WorkflowRebalancer.class);

  @Override public ResourceAssignment computeBestPossiblePartitionState(
      ClusterDataCache clusterData, IdealState taskIs, Resource resource,
      CurrentStateOutput currStateOutput) {
    final String workflow = resource.getResourceName();
    LOG.debug("Computer Best Partition for workflow: " + workflow);

    // Fetch workflow configuration and context
    WorkflowConfig workflowCfg = TaskUtil.getWorkflowConfig(_manager, workflow);
    if (workflowCfg == null) {
      LOG.warn("Workflow configuration is NULL for " + workflow);
      return buildEmptyAssignment(workflow, currStateOutput);
    }

    WorkflowContext workflowCtx = TaskUtil.getWorkflowContext(_manager, workflow);
    // Initialize workflow context if needed
    if (workflowCtx == null) {
      workflowCtx = new WorkflowContext(new ZNRecord(TaskUtil.WORKFLOW_CONTEXT_KW));
      workflowCtx.setStartTime(System.currentTimeMillis());
      LOG.debug("Workflow context is created for " + workflow);
    }

    // Clean up if workflow marked for deletion
    TargetState targetState = workflowCfg.getTargetState();
    if (targetState == TargetState.DELETE) {
      LOG.info("Workflow is marked as deleted " + workflow + " cleaning up the workflow context.");
      cleanupWorkflow(workflow,  workflowCfg);
      return buildEmptyAssignment(workflow, currStateOutput);
    }

    if (targetState == TargetState.STOP) {
      LOG.info("Workflow " + workflow + "is marked as stopped.");
      if (isWorkflowStopped(workflowCtx, workflowCfg)) {
        workflowCtx.setWorkflowState(TaskState.STOPPED);
        TaskUtil.setWorkflowContext(_manager, workflow, workflowCtx);
      }
      return buildEmptyAssignment(workflow, currStateOutput);
    }

    long currentTime = System.currentTimeMillis();
    // Check if workflow has been finished and mark it if it is.
    if (workflowCtx.getFinishTime() == WorkflowContext.UNFINISHED && isWorkflowFinished(workflowCtx,
        workflowCfg)) {
      workflowCtx.setFinishTime(currentTime);
      TaskUtil.setWorkflowContext(_manager, workflow, workflowCtx);
    }

    if (workflowCtx.getFinishTime() != WorkflowContext.UNFINISHED) {
      LOG.info("Workflow " + workflow + " is finished.");
      long expiryTime = workflowCfg.getExpiry();
      // Check if this workflow has been finished past its expiry.
      if (workflowCtx.getFinishTime() + expiryTime <= currentTime) {
        LOG.info("Workflow " + workflow + " passed expiry time, cleaning up the workflow context.");
        cleanupWorkflow(workflow, workflowCfg);
      } else {
        // schedule future cleanup work
        long cleanupTime = workflowCtx.getFinishTime() + expiryTime;
        _scheduledRebalancer.scheduleRebalance(_manager, workflow, cleanupTime);
      }
      return buildEmptyAssignment(workflow, currStateOutput);
    }

    if (!isWorkflowReadyForSchedule(workflowCfg)) {
      LOG.info("Workflow " + workflow + " is not ready to schedule");
      // set the timer to trigger future schedule
      _scheduledRebalancer
          .scheduleRebalance(_manager, workflow, workflowCfg.getStartTime().getTime());
      return buildEmptyAssignment(workflow, currStateOutput);
    }

    // Check for readiness, and stop processing if it's not ready
    boolean isReady = scheduleWorkflowIfReady(workflow, workflowCfg, workflowCtx);
    if (isReady) {
      // Schedule jobs from this workflow.
      scheduleJobs(workflow, workflowCfg, workflowCtx);
    } else {
      LOG.debug("Workflow " + workflow + " is not ready to be scheduled.");
    }

    // clean up the expired jobs if it is a queue.
    if (!workflowCfg.isTerminable() || workflowCfg.isJobQueue()) {
      purgeExpiredJobs(workflow, workflowCfg, workflowCtx);
    }

    TaskUtil.setWorkflowContext(_manager, workflow, workflowCtx);
    return buildEmptyAssignment(workflow, currStateOutput);
  }

  /**
   * Figure out whether the jobs in the workflow should be run, and if it's ready, then just
   * schedule it
   */
  private void scheduleJobs(String workflow, WorkflowConfig workflowCfg,
      WorkflowContext workflowCtx) {
    ScheduleConfig scheduleConfig = workflowCfg.getScheduleConfig();
    if (scheduleConfig != null && scheduleConfig.isRecurring()) {
      LOG.debug("Jobs from recurring workflow are not schedule-able");
      return;
    }

    int scheduledJobs = 0;
    long timeToSchedule = Long.MAX_VALUE;
    for (String job : workflowCfg.getJobDag().getAllNodes()) {
      TaskState jobState = workflowCtx.getJobState(job);
      if (jobState != null && !jobState.equals(TaskState.NOT_STARTED)) {
        LOG.debug("Job " + job + " is already started or completed.");
        continue;
      }

      if (workflowCfg.isJobQueue() && scheduledJobs >= workflowCfg.getParallelJobs()) {
        LOG.debug(String.format("Workflow %s already have enough job in progress, "
            + "scheduledJobs(s)=%d, stop scheduling more jobs", workflow, scheduledJobs));
        break;
      }

      // check ancestor job status
      if (isJobReadyToSchedule(job, workflowCfg, workflowCtx)) {
        JobConfig jobConfig = TaskUtil.getJobConfig(_manager, job);

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
          scheduledJobs++;
        }
      }
    }
    long currentScheduledTime = _scheduledRebalancer.getRebalanceTime(workflow) == -1
        ? Long.MAX_VALUE
        : _scheduledRebalancer.getRebalanceTime(workflow);
    if (timeToSchedule < currentScheduledTime) {
      _scheduledRebalancer.scheduleRebalance(_manager, workflow, timeToSchedule);
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
    TaskUtil.createUserContent(_manager.getHelixPropertyStore(), jobResource,
        new ZNRecord(TaskUtil.USER_CONTENT_NODE));
    int numIndependentTasks = jobConfig.getTaskConfigMap().size();

    int numPartitions = numIndependentTasks;
    if (numPartitions == 0) {
      IdealState targetIs =
          admin.getResourceIdealState(_manager.getClusterName(), jobConfig.getTargetResource());
      if (targetIs == null) {
        LOG.warn("Target resource does not exist for job " + jobResource);
        // do not need to fail here, the job will be marked as failure immediately when job starts running.
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
      builder.setDisableExternalView(true);
    }

    jobIS = builder.build();
    for (int i = 0; i < numPartitions; i++) {
      jobIS.getRecord().setListField(jobResource + "_" + i, new ArrayList<String>());
      jobIS.getRecord().setMapField(jobResource + "_" + i, new HashMap<String, String>());
    }
    jobIS.setRebalancerClassName(JobRebalancer.class.getName());
    admin.setResourceIdealState(_manager.getClusterName(), jobResource, jobIS);
  }

  /**
   * Check if a workflow is ready to schedule, and schedule a rebalance if it is not
   *
   * @param workflow    the Helix resource associated with the workflow
   * @param workflowCfg the workflow to check
   * @param workflowCtx the current workflow context
   *
   * @return true if the workflow is ready for schedule, false if not ready
   */
  private boolean scheduleWorkflowIfReady(String workflow, WorkflowConfig workflowCfg,
      WorkflowContext workflowCtx) {
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
          LOG.debug("Skip scheduling since the workflow has not been started " + workflow);
          return false;
        }

        // Skip scheduling this workflow again if the previous run (if any) is still active
        String lastScheduled = workflowCtx.getLastScheduledSingleWorkflow();
        if (lastScheduled != null) {
          WorkflowContext lastWorkflowCtx = TaskUtil.getWorkflowContext(_manager, lastScheduled);
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
        LOG.debug("Ready to start workflow " + newWorkflowName);
        if (!newWorkflowName.equals(lastScheduled)) {
          Workflow clonedWf =
              cloneWorkflow(_manager, workflow, newWorkflowName, new Date(timeToSchedule));
          TaskDriver driver = new TaskDriver(_manager);
          try {
            // Start the cloned workflow
            driver.start(clonedWf);
          } catch (Exception e) {
            LOG.error("Failed to schedule cloned workflow " + newWorkflowName, e);
            _clusterStatusMonitor
                .updateWorkflowCounters(clonedWf.getWorkflowConfig(), TaskState.FAILED);
          }
          // Persist workflow start regardless of success to avoid retrying and failing
          workflowCtx.setLastScheduledSingleWorkflow(newWorkflowName);
        }

        // Change the time to trigger the pipeline to that of the next run
        _scheduledRebalancer.scheduleRebalance(_manager, workflow, (timeToSchedule + period));
      } else {
        // one time workflow.
        // Remove any timers that are past-time for this workflowg
        long scheduledTime = _scheduledRebalancer.getRebalanceTime(workflow);
        if (scheduledTime > 0 && currentTime > scheduledTime) {
          _scheduledRebalancer.removeScheduledRebalance(workflow);
        }
        return true;
      }
    } else {
      // set the timer to trigger future schedule
      _scheduledRebalancer.scheduleRebalance(_manager, workflow, startTime.getTime());
    }

    return false;
  }

  /**
   * Create a new workflow based on an existing one
   *
   * @param manager          connection to Helix
   * @param origWorkflowName the name of the existing workflow
   * @param newWorkflowName  the name of the new workflow
   * @param newStartTime     a provided start time that deviates from the desired start time
   *
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
<<<<<<< HEAD
   * Clean up a workflow. This removes the workflow config, idealstate, externalview and workflow
   * contexts associated with this workflow, and all jobs information, including their configs,
   * context, IS and EV.
=======
   * Cleans up workflow configs and workflow contexts associated with this workflow, including all
   * job-level configs and context, plus workflow-level information.
>>>>>>> Support configurable job purge interval for a queue.
   */
  private void cleanupWorkflow(String workflow, WorkflowConfig workflowcfg) {
    LOG.info("Cleaning up workflow: " + workflow);

    if (workflowcfg.isTerminable() || workflowcfg.getTargetState() == TargetState.DELETE) {
      Set<String> jobs = workflowcfg.getJobDag().getAllNodes();
      // Remove all pending timer tasks for this workflow if exists
      _scheduledRebalancer.removeScheduledRebalance(workflow);
      for (String job : jobs) {
        _scheduledRebalancer.removeScheduledRebalance(job);
      }
      if (!TaskUtil.removeWorkflow(_manager, workflow, jobs)) {
        LOG.warn("Failed to clean up workflow " + workflow);
      }
    } else {
      LOG.info("Did not clean up workflow " + workflow
          + " because neither the workflow is non-terminable nor is set to DELETE.");
    }
  }

  /**
   * Clean up all jobs that are COMPLETED and passes its expiry time.
   *
   * @param workflowConfig
   * @param workflowContext
   */
  // TODO: run this in a separate thread.
  // Get all jobConfigs & jobContext from ClusterCache.
  private void purgeExpiredJobs(String workflow, WorkflowConfig workflowConfig,
      WorkflowContext workflowContext) {
    long purgeInterval = workflowConfig.getJobPurgeInterval();
    long currentTime = System.currentTimeMillis();

    if (purgeInterval > 0 && workflowContext.getLastJobPurgeTime() + purgeInterval <= currentTime) {
      Set<String> expiredJobs = TaskUtil
          .getExpiredJobs(_manager.getHelixDataAccessor(), _manager.getHelixPropertyStore(),
              workflowConfig, workflowContext);

      if (expiredJobs.isEmpty()) {
        LOG.info("No job to purge for the queue " + workflow);
      } else {
        LOG.info("Purge jobs " + expiredJobs + " from queue " + workflow);
        for (String job : expiredJobs) {
          if (!TaskUtil
              .removeJob(_manager.getHelixDataAccessor(), _manager.getHelixPropertyStore(), job)) {
            LOG.warn("Failed to clean up expired and completed jobs from workflow " + workflow);
          }
          _scheduledRebalancer.removeScheduledRebalance(job);
        }
        if (!TaskUtil
            .removeJobsFromDag(_manager.getHelixDataAccessor(), workflow, expiredJobs, true)) {
          LOG.warn(
              "Error occurred while trying to remove jobs + " + expiredJobs + " from the workflow "
                  + workflow);
        }
        // remove job states in workflowContext.
        workflowContext.removeJobStates(expiredJobs);
        workflowContext.removeJobStartTime(expiredJobs);
      }
      workflowContext.setLastJobPurgeTime(currentTime);
    }

    setNextJobPurgeTime(workflow, currentTime, purgeInterval);
  }

  private void setNextJobPurgeTime(String workflow, long currentTime, long purgeInterval) {
    long nextPurgeTime = currentTime + purgeInterval;
    long currentScheduledTime = _scheduledRebalancer.getRebalanceTime(workflow);
    if (currentScheduledTime == -1 || currentScheduledTime > nextPurgeTime) {
      _scheduledRebalancer.scheduleRebalance(_manager, workflow, nextPurgeTime);
    }
  }

  @Override public IdealState computeNewIdealState(String resourceName,
      IdealState currentIdealState, CurrentStateOutput currentStateOutput,
      ClusterDataCache clusterData) {
    // Nothing to do here with workflow resource.
    return currentIdealState;
  }
}
