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

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.*;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.*;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Custom rebalancer implementation for the {@code Workflow} in task state model.
 */
public class WorkflowRebalancer extends TaskRebalancer {
  private static final Logger LOG = Logger.getLogger(WorkflowRebalancer.class);

  @Override
  public ResourceAssignment computeBestPossiblePartitionState(ClusterDataCache clusterData,
      IdealState taskIs, Resource resource, CurrentStateOutput currStateOutput) {
    final String workflow = resource.getResourceName();
    LOG.debug("Computer Best Partition for workflow: " + workflow);

    // Fetch workflow configuration and context
    WorkflowConfig workflowCfg = TaskUtil.getWorkflowCfg(_manager, workflow);
    if (workflowCfg == null) {
      LOG.warn("Workflow configuration is NULL for " + workflow);
      return buildEmptyAssignment(workflow, currStateOutput);
    }

    WorkflowContext workflowCtx = TaskUtil.getWorkflowContext(_manager, workflow);
    // Initialize workflow context if needed
    if (workflowCtx == null) {
      workflowCtx = new WorkflowContext(new ZNRecord("WorkflowContext"));
      workflowCtx.setStartTime(System.currentTimeMillis());
      LOG.debug("Workflow context is created for " + workflow);
    }

    // Clean up if workflow marked for deletion
    TargetState targetState = workflowCfg.getTargetState();
    if (targetState == TargetState.DELETE) {
      LOG.info("Workflow is marked as deleted " + workflow + " cleaning up the workflow context.");
      cleanupWorkflow(workflow, workflowCfg, workflowCtx);
      return buildEmptyAssignment(workflow, currStateOutput);
    }

    if (targetState == TargetState.STOP) {
      LOG.info("Workflow " + workflow + "is marked as stopped.");
      // Workflow has been stopped if all jobs are stopped
      // TODO: what should we do if workflowCtx is not set yet?
      if (workflowCtx != null && isWorkflowStopped(workflowCtx, workflowCfg)) {
        workflowCtx.setWorkflowState(TaskState.STOPPED);
      }
      return buildEmptyAssignment(workflow, currStateOutput);
    }

    long currentTime = System.currentTimeMillis();
    // Check if workflow has been finished and mark it if it is.
    if (workflowCtx.getFinishTime() == WorkflowContext.UNFINISHED
        && isWorkflowFinished(workflowCtx, workflowCfg)) {
      workflowCtx.setFinishTime(currentTime);
      TaskUtil.setWorkflowContext(_manager, workflow, workflowCtx);
    }

    if (workflowCtx.getFinishTime() != WorkflowContext.UNFINISHED) {
      LOG.info("Workflow " + workflow + " is finished.");
      long expiryTime = workflowCfg.getExpiry();
      // Check if this workflow has been finished past its expiry.
      if (workflowCtx.getFinishTime() + expiryTime <= currentTime) {
        LOG.info("Workflow " + workflow + " passed expiry time, cleaning up the workflow context.");
        cleanupWorkflow(workflow, workflowCfg, workflowCtx);
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
    boolean isReady =
        scheduleWorkflowIfReady(workflow, workflowCfg, workflowCtx);
    if (isReady) {
      // Schedule jobs from this workflow.
      scheduleJobs(workflowCfg, workflowCtx);
    } else {
      LOG.debug("Workflow " + workflow + " is not ready to be scheduled.");
    }

    TaskUtil.setWorkflowContext(_manager, workflow, workflowCtx);
    return buildEmptyAssignment(workflow, currStateOutput);
  }

  /**
   * Figure out whether the jobs in the workflow should be run,
   * and if it's ready, then just schedule it
   */
  private void scheduleJobs(WorkflowConfig workflowCfg, WorkflowContext workflowCtx) {
    ScheduleConfig scheduleConfig = workflowCfg.getScheduleConfig();
    if (scheduleConfig != null && scheduleConfig.isRecurring()) {
      LOG.debug("Jobs from recurring workflow are not schedule-able");
      return;
    }

    for (String job : workflowCfg.getJobDag().getAllNodes()) {
      TaskState jobState = workflowCtx.getJobState(job);
      if (jobState != null && !jobState.equals(TaskState.NOT_STARTED)) {
        LOG.debug("Job " + job + " is already started or completed.");
        continue;
      }
      // check ancestor job status
      if (isJobReadyToSchedule(job, workflowCfg, workflowCtx)) {
        JobConfig jobConfig = TaskUtil.getJobCfg(_manager, job);
        scheduleSingleJob(job, jobConfig);
      }
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
    CustomModeISBuilder builder = new CustomModeISBuilder(jobResource);
    builder.setRebalancerMode(IdealState.RebalanceMode.TASK);
    builder.setNumReplica(1);
    builder.setNumPartitions(numPartitions);
    builder.setStateModel(TaskConstants.STATE_MODEL_NAME);

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
   * @param workflow the Helix resource associated with the workflow
   * @param workflowCfg  the workflow to check
   * @param workflowCtx  the current workflow context
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
          Workflow clonedWf = TaskUtil
              .cloneWorkflow(_manager, workflow, newWorkflowName, new Date(timeToSchedule));
          TaskDriver driver = new TaskDriver(_manager);
          try {
            // Start the cloned workflow
            driver.start(clonedWf);
          } catch (Exception e) {
            LOG.error("Failed to schedule cloned workflow " + newWorkflowName, e);
          }
          // Persist workflow start regardless of success to avoid retrying and failing
          workflowCtx.setLastScheduledSingleWorkflow(newWorkflowName);
          TaskUtil.setWorkflowContext(_manager, workflow, workflowCtx);
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
   * Cleans up workflow configs and workflow contexts associated with this workflow,
   * including all job-level configs and context, plus workflow-level information.
   */
  private void cleanupWorkflow(String workflow, WorkflowConfig workflowcfg,
      WorkflowContext workflowCtx) {
    LOG.info("Cleaning up workflow: " + workflow);
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();

    /*
    if (workflowCtx != null && workflowCtx.getFinishTime() == WorkflowContext.UNFINISHED) {
      LOG.error("Workflow " + workflow + " has not completed, abort the clean up task.");
      return;
    }*/

    for (String job : workflowcfg.getJobDag().getAllNodes()) {
      cleanupJob(job, workflow);
    }

    // clean up workflow-level info if this was the last in workflow
    if (workflowcfg.isTerminable() || workflowcfg.getTargetState() == TargetState.DELETE) {
      // clean up IS & EV
      TaskUtil.cleanupIdealStateExtView(_manager.getHelixDataAccessor(), workflow);

      // delete workflow config
      PropertyKey workflowCfgKey = TaskUtil.getWorkflowConfigKey(accessor, workflow);
      if (accessor.getProperty(workflowCfgKey) != null) {
        if (!accessor.removeProperty(workflowCfgKey)) {
          LOG.error(String.format(
              "Error occurred while trying to clean up workflow %s. Failed to remove node %s from Helix.",
              workflow, workflowCfgKey));
        }
      }
      // Delete workflow context
      String workflowPropStoreKey = TaskUtil.getWorkflowContextKey(workflow);
      LOG.info("Removing workflow context: " + workflowPropStoreKey);
      if (!_manager.getHelixPropertyStore().remove(workflowPropStoreKey, AccessOption.PERSISTENT)) {
        LOG.error(String.format(
            "Error occurred while trying to clean up workflow %s. Failed to remove node %s from Helix. Aborting further clean up steps.",
            workflow, workflowPropStoreKey));
      }

      // Remove pending timer task for this workflow if exists
      _scheduledRebalancer.removeScheduledRebalance(workflow);
    }
  }


  /**
   * Cleans up workflow configs and workflow contexts associated with this workflow,
   * including all job-level configs and context, plus workflow-level information.
   */
  private void cleanupJob(final String job, String workflow) {
    LOG.info("Cleaning up job: " + job + " in workflow: " + workflow);
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();

    // Remove any idealstate and externalView.
    TaskUtil.cleanupIdealStateExtView(accessor, job);

    // Remove DAG references in workflow
    PropertyKey workflowKey = TaskUtil.getWorkflowConfigKey(accessor, workflow);
    DataUpdater<ZNRecord> dagRemover = new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        if (currentData != null) {
          JobDag jobDag = JobDag.fromJson(currentData.getSimpleField(WorkflowConfig.DAG));
          for (String child : jobDag.getDirectChildren(job)) {
            jobDag.getChildrenToParents().get(child).remove(job);
          }
          for (String parent : jobDag.getDirectParents(job)) {
            jobDag.getParentsToChildren().get(parent).remove(job);
          }
          jobDag.getChildrenToParents().remove(job);
          jobDag.getParentsToChildren().remove(job);
          jobDag.getAllNodes().remove(job);
          try {
            currentData.setSimpleField(WorkflowConfig.DAG, jobDag.toJson());
          } catch (Exception e) {
            LOG.error("Could not update DAG for job: " + job, e);
          }
        } else {
          LOG.error("Could not update DAG for job: " + job + " ZNRecord is null.");
        }
        return currentData;
      }
    };
    accessor.getBaseDataAccessor().update(workflowKey.getPath(), dagRemover,
        AccessOption.PERSISTENT);

    // Delete job configs.
    PropertyKey cfgKey = TaskUtil.getWorkflowConfigKey(accessor, job);
    if (accessor.getProperty(cfgKey) != null) {
      if (!accessor.removeProperty(cfgKey)) {
        LOG.error(String.format(
            "Error occurred while trying to clean up job %s. Failed to remove node %s from Helix.",
            job, cfgKey));
      }
    }

    // Delete job context
    // For recurring workflow, it's OK if the node doesn't exist.
    String propStoreKey = TaskUtil.getWorkflowContextKey(job);
    if (!_manager.getHelixPropertyStore().remove(propStoreKey, AccessOption.PERSISTENT)) {
      LOG.warn(String.format(
          "Error occurred while trying to clean up job %s. Failed to remove node %s from Helix.",
          job, propStoreKey));
    }

    LOG.info(String.format("Successfully cleaned up job context %s.", job));

    _scheduledRebalancer.removeScheduledRebalance(job);
  }

  @Override
  public IdealState computeNewIdealState(String resourceName, IdealState currentIdealState,
      CurrentStateOutput currentStateOutput, ClusterDataCache clusterData) {
    // Nothing to do here with workflow resource.
    return currentIdealState;
  }
}
