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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.util.RebalanceScheduler;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.log4j.Logger;

/**
 * CLI for scheduling/canceling workflows
 */
public class TaskDriver {

  public enum DriverCommand {
    start,
    stop,
    delete,
    resume,
    list,
    flush,
    clean
  }

  /** For logging */
  private static final Logger LOG = Logger.getLogger(TaskDriver.class);

  /** Default time out for monitoring workflow or job state */
  private final static int _defaultTimeout = 3 * 60 * 1000; /* 3 mins */

  private final HelixDataAccessor _accessor;
  private final HelixPropertyStore<ZNRecord> _propertyStore;
  private final HelixAdmin _admin;
  private final String _clusterName;

  public TaskDriver(HelixManager manager) {
    this(manager.getClusterManagmentTool(), manager.getHelixDataAccessor(),
        manager.getHelixPropertyStore(), manager.getClusterName());
  }

  public TaskDriver(ZkClient client, String clusterName) {
    this(client, new ZkBaseDataAccessor<ZNRecord>(client), clusterName);
  }

  public TaskDriver(ZkClient client, ZkBaseDataAccessor<ZNRecord> baseAccessor, String clusterName) {
    this(new ZKHelixAdmin(client), new ZKHelixDataAccessor(clusterName, baseAccessor),
        new ZkHelixPropertyStore<ZNRecord>(baseAccessor,
            PropertyPathBuilder.propertyStore(clusterName), null), clusterName);
  }

  @Deprecated
  public TaskDriver(HelixAdmin admin, HelixDataAccessor accessor, ConfigAccessor cfgAccessor,
      HelixPropertyStore<ZNRecord> propertyStore, String clusterName) {
    this(admin, accessor, propertyStore, clusterName);
  }

  public TaskDriver(HelixAdmin admin, HelixDataAccessor accessor,
      HelixPropertyStore<ZNRecord> propertyStore, String clusterName) {
    _admin = admin;
    _accessor = accessor;
    _propertyStore = propertyStore;
    _clusterName = clusterName;
  }


  /** Schedules a new workflow
   *
   * @param flow
   */
  public void start(Workflow flow) {
    // TODO: check that namespace for workflow is available
    LOG.info("Starting workflow " + flow.getName());
    flow.validate();

    WorkflowConfig newWorkflowConfig =
        new WorkflowConfig.Builder(flow.getWorkflowConfig()).setWorkflowId(flow.getName()).build();

    Map<String, String> jobTypes = new HashMap<String, String>();
    // add all job configs.
    for (String job : flow.getJobConfigs().keySet()) {
      JobConfig.Builder jobCfgBuilder = JobConfig.Builder.fromMap(flow.getJobConfigs().get(job));
      if (flow.getTaskConfigs() != null && flow.getTaskConfigs().containsKey(job)) {
        jobCfgBuilder.addTaskConfigs(flow.getTaskConfigs().get(job));
      }
      JobConfig jobCfg = jobCfgBuilder.build();
      if (jobCfg.getJobType() != null) {
        jobTypes.put(job, jobCfg.getJobType());
      }
      addJobConfig(job, jobCfg);
    }
    newWorkflowConfig.setJobTypes(jobTypes);

    // add workflow config.
    if (!TaskUtil.setWorkflowConfig(_accessor, flow.getName(), newWorkflowConfig)) {
      LOG.error("Failed to add workflow configuration for workflow " + flow.getName());
    }

    // Finally add workflow resource.
    addWorkflowResource(flow.getName());
  }

  /**
   * Update the configuration of a non-terminable workflow (queue).
   * The terminable workflow's configuration is not allowed
   * to change once created.
   * Note:
   * For recurrent workflow, the current running schedule will not be effected,
   * the new configuration will be applied to the next scheduled runs of the workflow.
   * For non-recurrent workflow, the new configuration may (or may not) be applied
   * on the current running jobs, but it will be applied on the following unscheduled jobs.
   *
   * Example:
   *
   * _driver = new TaskDriver ...
   * WorkflowConfig currentWorkflowConfig = _driver.getWorkflowCfg(_manager, workflow);
   * WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(currentWorkflowConfig);

   * // make needed changes to the config here
   * configBuilder.setXXX();
   *
   * // update workflow configuration
   * _driver.updateWorkflow(workflow, configBuilder.build());
   *
   * @param workflow
   * @param newWorkflowConfig
   */
  public void updateWorkflow(String workflow, WorkflowConfig newWorkflowConfig) {
    if (workflow == null || !workflow.equals(newWorkflowConfig.getWorkflowId())) {
      throw new HelixException(String
          .format("Workflow name {%s} does not match the workflow Id from WorkflowConfig {%s}",
              workflow, newWorkflowConfig.getWorkflowId()));
    }

    WorkflowConfig currentConfig =
        TaskUtil.getWorkflowConfig(_accessor, workflow);
    if (currentConfig == null) {
      throw new HelixException("Workflow " + workflow + " does not exist!");
    }

    if (currentConfig.isTerminable()) {
      throw new HelixException(
          "Workflow " + workflow + " is terminable, not allow to change its configuration!");
    }

    // Should not let user changing DAG in the workflow
    newWorkflowConfig.setJobDag(currentConfig.getJobDag());
    if (!TaskUtil.setWorkflowConfig(_accessor, workflow, newWorkflowConfig)) {
      LOG.error("Failed to update workflow configuration for workflow " + workflow);
    }

    RebalanceScheduler.invokeRebalance(_accessor, workflow);
  }

  /**
   * Creates a new named job queue (workflow)
   *
   * @param queue
   */
  public void createQueue(JobQueue queue) {
    start(queue);
  }

  /**
   * Remove all completed or failed jobs in a job queue
   * Same as {@link #cleanupQueue(String)}
   *
   * @param queue name of the queue
   * @throws Exception
   */
  public void flushQueue(String queue) {
    cleanupQueue(queue);
  }

  /**
   * Delete a job from an existing named queue,
   * the queue has to be stopped prior to this call
   *
   * @param queue queue name
   * @param job  job name
   */
  public void deleteJob(final String queue, final String job) {
    WorkflowConfig workflowCfg =
        TaskUtil.getWorkflowConfig(_accessor, queue);

    if (workflowCfg == null) {
      throw new IllegalArgumentException("Queue " + queue + " does not yet exist!");
    }
    if (workflowCfg.isTerminable()) {
      throw new IllegalArgumentException(queue + " is not a queue!");
    }

    boolean isRecurringWorkflow =
        (workflowCfg.getScheduleConfig() != null && workflowCfg.getScheduleConfig().isRecurring());

    if (isRecurringWorkflow) {
      // delete job from the last scheduled queue if there exists one.
      WorkflowContext wCtx = TaskUtil.getWorkflowContext(_propertyStore, queue);
      String lastScheduledQueue = null;
      if (wCtx != null) {
        lastScheduledQueue = wCtx.getLastScheduledSingleWorkflow();
      }
      if (lastScheduledQueue != null) {
        WorkflowConfig lastWorkflowCfg = TaskUtil.getWorkflowConfig(_accessor, lastScheduledQueue);
        if (lastWorkflowCfg != null) {
          deleteJobFromQueue(lastScheduledQueue, job);
        }
      }
    }

    deleteJobFromQueue(queue, job);
  }

    /**
     * delete a job from a scheduled (non-recurrent) queue.
     *
     * @param queue
     * @param job
     */

  private void deleteJobFromQueue(final String queue, final String job) {
    WorkflowContext workflowCtx = TaskUtil.getWorkflowContext(_propertyStore, queue);
    String workflowState = (workflowCtx != null)
        ? workflowCtx.getWorkflowState().name()
        : TaskState.NOT_STARTED.name();

    if (workflowState.equals(TaskState.IN_PROGRESS.name())) {
      throw new IllegalStateException("Queue " + queue + " is still running!");
    }

    if (workflowState.equals(TaskState.COMPLETED.name()) || workflowState.equals(
        TaskState.FAILED.name()) || workflowState.equals(TaskState.ABORTED.name())) {
      LOG.warn("Queue " + queue + " has already reached its final state, skip deleting job from it.");
      return;
    }

    String namespacedJobName = TaskUtil.getNamespacedJobName(queue, job);
    Set<String> jobs = new HashSet<String>(Arrays.asList(namespacedJobName));
    if (!TaskUtil.removeJobsFromWorkflow(_accessor, _propertyStore, queue, jobs, true)) {
      LOG.error("Failed to delete job " + job + " from queue " + queue);
      throw new HelixException("Failed to delete job " + job + " from queue " + queue);
    }
  }

  /**
   * Adds a new job to the end an existing named queue.
   *
   * @param queue
   * @param job
   * @param jobBuilder
   * @throws Exception
   */
  public void enqueueJob(final String queue, final String job,
      JobConfig.Builder jobBuilder) {
    // Get the job queue config and capacity
    WorkflowConfig workflowConfig = TaskUtil.getWorkflowConfig(_accessor, queue);
    if (workflowConfig == null) {
      throw new IllegalArgumentException("Queue " + queue + " config does not yet exist!");
    }
    if (workflowConfig.isTerminable()) {
      throw new IllegalArgumentException(queue + " is not a queue!");
    }

    final int capacity = workflowConfig.getCapacity();
    int queueSize = workflowConfig.getJobDag().size();
    if (capacity > 0 && queueSize >= capacity) {
      // if queue is full, Helix will try to clean up the expired job to free more space.
      WorkflowContext workflowContext = TaskUtil.getWorkflowContext(_propertyStore, queue);
      if (workflowContext != null) {
        Set<String> expiredJobs =
            TaskUtil.getExpiredJobs(_accessor, _propertyStore, workflowConfig, workflowContext);
        if (!TaskUtil.removeJobsFromWorkflow(_accessor, _propertyStore, queue, expiredJobs, true)) {
          LOG.warn("Failed to clean up expired and completed jobs from queue " + queue);
        }
      }
      workflowConfig = TaskUtil.getWorkflowConfig(_accessor, queue);
      if (workflowConfig.getJobDag().size() >= capacity) {
        throw new HelixException("Failed to enqueue a job, queue is full.");
      }
    }

    // Create the job to ensure that it validates
    JobConfig jobConfig = jobBuilder.setWorkflow(queue).build();
    final String namespacedJobName = TaskUtil.getNamespacedJobName(queue, job);

    // add job config first.
    addJobConfig(namespacedJobName, jobConfig);
    final String jobType = jobConfig.getJobType();

    // update the job dag to append the job to the end of the queue.
    DataUpdater<ZNRecord> updater = new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        // Add the node to the existing DAG
        JobDag jobDag = JobDag.fromJson(
            currentData.getSimpleField(WorkflowConfig.WorkflowConfigProperty.Dag.name()));
        Set<String> allNodes = jobDag.getAllNodes();
        if (capacity > 0 && allNodes.size() >= capacity) {
          throw new IllegalStateException(
              "Queue " + queue + " already reaches its max capacity, failed to add " + job);
        }
        if (allNodes.contains(namespacedJobName)) {
          throw new IllegalStateException(
              "Could not add to queue " + queue + ", job " + job + " already exists");
        }
        jobDag.addNode(namespacedJobName);

        // Add the node to the end of the queue
        String candidate = null;
        for (String node : allNodes) {
          if (!node.equals(namespacedJobName) && jobDag.getDirectChildren(node).isEmpty()) {
            candidate = node;
            break;
          }
        }
        if (candidate != null) {
          jobDag.addParentToChild(candidate, namespacedJobName);
        }

        // Add job type if job type is not null
        if (jobType != null) {
          Map<String, String> jobTypes =
              currentData.getMapField(WorkflowConfig.WorkflowConfigProperty.JobTypes.name());
          if (jobTypes == null) {
            jobTypes = new HashMap<String, String>();
          }
          jobTypes.put(queue, jobType);
          currentData.setMapField(WorkflowConfig.WorkflowConfigProperty.JobTypes.name(), jobTypes);
        }

        // Save the updated DAG
        try {
          currentData
              .setSimpleField(WorkflowConfig.WorkflowConfigProperty.Dag.name(), jobDag.toJson());
        } catch (Exception e) {
          throw new IllegalStateException("Could not add job " + job + " to queue " + queue,
              e);
        }
        return currentData;
      }
    };
    String path = _accessor.keyBuilder().resourceConfig(queue).getPath();
    boolean status = _accessor.getBaseDataAccessor().update(path, updater, AccessOption.PERSISTENT);
    if (!status) {
      throw new HelixException("Failed to enqueue job");
    }

    // This is to make it back-compatible with old Helix task driver.
    addWorkflowResourceIfNecessary(queue);

    // Schedule the job
    RebalanceScheduler.invokeRebalance(_accessor, queue);
  }

  /**
   * Remove all jobs that are in final states (ABORTED, FAILED, COMPLETED) from the job queue. The
   * job config, job context will be removed from Zookeeper.
   *
   * @param queue The name of job queue
   */
  public void cleanupQueue(String queue) {
    WorkflowConfig workflowConfig = TaskUtil.getWorkflowConfig(_accessor, queue);

    if (workflowConfig == null) {
      throw new IllegalArgumentException("Queue " + queue + " does not yet exist!");
    }

    boolean isTerminable = workflowConfig.isTerminable();
    if (isTerminable) {
      throw new IllegalArgumentException(queue + " is not a queue!");
    }

    WorkflowContext wCtx = TaskUtil.getWorkflowContext(_propertyStore, queue);
    if (wCtx == null || wCtx.getWorkflowState() == null) {
      throw new IllegalStateException("Queue " + queue + " does not have a valid work state!");
    }

    Set<String> jobs = new HashSet<String>();
    for (String jobNode : workflowConfig.getJobDag().getAllNodes()) {
      TaskState curState = wCtx.getJobState(jobNode);
      if (curState != null && (curState == TaskState.ABORTED || curState == TaskState.COMPLETED
          || curState == TaskState.FAILED)) {
        jobs.add(jobNode);
      }
    }

    TaskUtil.removeJobsFromWorkflow(_accessor, _propertyStore, queue, jobs, true);
  }

  /** Posts new workflow resource to cluster */
  private void addWorkflowResource(String workflow) {
    // Add workflow resource
    _admin.addResource(_clusterName, workflow, 1, TaskConstants.STATE_MODEL_NAME);

    IdealState is = buildWorkflowIdealState(workflow);
    TaskUtil
        .createUserContent(_propertyStore, workflow, new ZNRecord(TaskUtil.USER_CONTENT_NODE));

    _admin.setResourceIdealState(_clusterName, workflow, is);
  }

  /**
   * Posts new workflow resource to cluster if it does not exist
   */
  private void addWorkflowResourceIfNecessary(String workflow) {
    IdealState is = _admin.getResourceIdealState(_clusterName, workflow);
    if (is == null) {
      addWorkflowResource(workflow);
    }
  }

  private IdealState buildWorkflowIdealState(String workflow) {
    CustomModeISBuilder IsBuilder = new CustomModeISBuilder(workflow);
    IsBuilder.setRebalancerMode(IdealState.RebalanceMode.TASK).setNumReplica(1)
        .setNumPartitions(1).setStateModel(TaskConstants.STATE_MODEL_NAME).disableExternalView();

    IdealState is = IsBuilder.build();
    is.getRecord().setListField(workflow, new ArrayList<String>());
    is.getRecord().setMapField(workflow, new HashMap<String, String>());
    is.setRebalancerClassName(WorkflowRebalancer.class.getName());

    return is;
  }

  /**
   * Add new job config to cluster
   */
  private void addJobConfig(String job, JobConfig jobConfig) {
    LOG.info("Add job configuration " + job);

    // Set the job configuration
    JobConfig newJobCfg = new JobConfig(job, jobConfig);
    if (!TaskUtil.setJobConfig(_accessor, job, newJobCfg)) {
      throw new HelixException("Failed to add job configuration for job " + job);
    }
  }

  /**
   * Public method to resume a workflow/queue.
   *
   * @param workflow
   */
  public void resume(String workflow) {
    setWorkflowTargetState(workflow, TargetState.START);
  }

  /**
   * Public async method to stop a workflow/queue.
   *
   * This call only send STOP command to Helix, it does not check
   * whether the workflow (all jobs) has been stopped yet.
   *
   * @param workflow
   */
  public void stop(String workflow) throws InterruptedException {
    setWorkflowTargetState(workflow, TargetState.STOP);
  }

  /**
   * Public sync method to stop a workflow/queue with timeout
   *
   * Basically the workflow and all of its jobs has been stopped if this method return success.
   *
   * @param workflow  The workflow name
   * @param timeout   The timeout for stopping workflow/queue in milisecond
   */
  public void waitToStop(String workflow, long timeout) throws InterruptedException {
    setWorkflowTargetState(workflow, TargetState.STOP);
    long endTime = System.currentTimeMillis() + timeout;

    while (System.currentTimeMillis() <= endTime) {
      WorkflowContext workflowContext = getWorkflowContext(workflow);

      if (workflowContext == null || !workflowContext.getWorkflowState()
          .equals(TaskState.STOPPED)) {
        Thread.sleep(1000);
      } else {
        // Successfully stopped
        return;
      }
    }

    // Failed to stop with timeout
    throw new HelixException(String
        .format("Fail to stop the workflow/queue %s with in %d milliseconds.", workflow, timeout));
  }

  /**
   * Public method to delete a workflow/queue.
   *
   * @param workflow
   */
  public void delete(String workflow) {
    // After set DELETE state, rebalancer may remove the workflow instantly.
    // So record context before set DELETE state.
    WorkflowContext wCtx = TaskUtil.getWorkflowContext(_propertyStore, workflow);

    setWorkflowTargetState(workflow, TargetState.DELETE);

    // Delete all finished scheduled workflows.
    if (wCtx != null && wCtx.getScheduledWorkflows() != null) {
      for (String scheduledWorkflow : wCtx.getScheduledWorkflows()) {
        WorkflowContext scheduledWorkflowCtx = TaskUtil.getWorkflowContext(_propertyStore, scheduledWorkflow);
        if (scheduledWorkflowCtx != null && scheduledWorkflowCtx.getFinishTime() != WorkflowContext.UNFINISHED) {
          Set<String> jobSet = new HashSet<String>();
          // Note that even WorkflowConfig is null, if WorkflowContext exists, still need to remove workflow
          WorkflowConfig wCfg = TaskUtil.getWorkflowConfig(_accessor, scheduledWorkflow);
          if (wCfg != null) {
            jobSet.addAll(wCfg.getJobDag().getAllNodes());
          }
          TaskUtil.removeWorkflow(_accessor, _propertyStore, scheduledWorkflow, jobSet);
        }
      }
    }
  }

  /**
   * Helper function to change target state for a given workflow
   */
  private void setWorkflowTargetState(String workflow, TargetState state) {
    setSingleWorkflowTargetState(workflow, state);

    // For recurring schedules, last scheduled incomplete workflow must also be handled
    WorkflowContext wCtx = TaskUtil.getWorkflowContext(_propertyStore, workflow);
    if (wCtx != null) {
      String lastScheduledWorkflow = wCtx.getLastScheduledSingleWorkflow();
      if (lastScheduledWorkflow != null) {
        setSingleWorkflowTargetState(lastScheduledWorkflow, state);
      }
    }
  }

  /**
   * Helper function to change target state for a given workflow
   */
  private void setSingleWorkflowTargetState(String workflow, final TargetState state) {
    LOG.info("Set " + workflow + " to target state " + state);

    WorkflowConfig workflowConfig = TaskUtil.getWorkflowConfig(_accessor, workflow);
    if (workflowConfig == null) {
      LOG.warn("WorkflowConfig for " + workflow + " not found!");
      return;
    }

    WorkflowContext workflowContext = TaskUtil.getWorkflowContext(_propertyStore, workflow);
    if (state != TargetState.DELETE && workflowContext != null &&
        workflowContext.getFinishTime() != WorkflowContext.UNFINISHED) {
      // Should not update target state for completed workflow
      LOG.info("Workflow " + workflow + " is already completed, skip to update its target state "
          + state);
      return;
    }

    DataUpdater<ZNRecord> updater = new DataUpdater<ZNRecord>() {
      @Override public ZNRecord update(ZNRecord currentData) {
        if (currentData != null) {
          currentData.setSimpleField(WorkflowConfig.WorkflowConfigProperty.TargetState.name(),
              state.name());
        } else {
          LOG.warn("TargetState DataUpdater: Fails to update target state. CurrentData is "
              + currentData);
        }
        return currentData;
      }
    };

    PropertyKey workflowConfigKey = TaskUtil.getWorkflowConfigKey(_accessor, workflow);
    _accessor.getBaseDataAccessor()
        .update(workflowConfigKey.getPath(), updater, AccessOption.PERSISTENT);
    RebalanceScheduler.invokeRebalance(_accessor, workflow);
  }

  public WorkflowConfig getWorkflowConfig(String workflow) {
    return TaskUtil.getWorkflowConfig(_accessor, workflow);
  }

  public WorkflowContext getWorkflowContext(String workflow) {
    return TaskUtil.getWorkflowContext(_propertyStore, workflow);
  }

  public JobConfig getJobConfig(String job) {
    return TaskUtil.getJobConfig(_accessor, job);
  }

  public JobContext getJobContext(String job) {
    return TaskUtil.getJobContext(_propertyStore, job);
  }

  public static JobContext getJobContext(HelixManager manager, String job) {
    return TaskUtil.getJobContext(manager, job);
  }

  public static WorkflowConfig getWorkflowConfig(HelixManager manager, String workflow) {
    return TaskUtil.getWorkflowConfig(manager, workflow);
  }

  public static WorkflowContext getWorkflowContext(HelixManager manager, String workflow) {
    return TaskUtil.getWorkflowContext(manager, workflow);
  }

  public static JobConfig getJobConfig(HelixManager manager, String job) {
    return TaskUtil.getJobConfig(manager, job);
  }

  /**
   * Batch get the configurations of all workflows in this cluster.
   *
   * @return
   */
  public Map<String, WorkflowConfig> getWorkflows() {
    Map<String, WorkflowConfig> workflowConfigMap = new HashMap<String, WorkflowConfig>();
    Map<String, ResourceConfig> resourceConfigMap =
        _accessor.getChildValuesMap(_accessor.keyBuilder().resourceConfigs());

    for (Map.Entry<String, ResourceConfig> resource : resourceConfigMap.entrySet()) {
      try {
        WorkflowConfig config = WorkflowConfig.fromHelixProperty(resource.getValue());
        workflowConfigMap.put(resource.getKey(), config);
      } catch (IllegalArgumentException ex) {
        // ignore if it is not a workflow config.
      }
    }

    return workflowConfigMap;
  }

  /**
   * This call will be blocked until either workflow reaches to one of the state specified
   * in the arguments, or timeout happens. If timeout happens, then it will throw a HelixException
   * Otherwise, it will return current workflow state
   *
   * @param workflowName The workflow to be monitored
   * @param timeout A long integer presents the time out, in milliseconds
   * @param targetStates Specified states that user would like to stop monitoring
   * @return A TaskState, which is current workflow state
   * @throws InterruptedException
   */
  public TaskState pollForWorkflowState(String workflowName, long timeout,
      TaskState... targetStates) throws InterruptedException {
    // Wait for completion.
    long st = System.currentTimeMillis();
    WorkflowContext ctx;
    Set<TaskState> allowedStates = new HashSet<TaskState>(Arrays.asList(targetStates));

    long timeToSleep = timeout > 100L ? 100L : timeout;
    do {
      Thread.sleep(timeToSleep);
      ctx = getWorkflowContext(workflowName);
    } while ((ctx == null || ctx.getWorkflowState() == null || !allowedStates
        .contains(ctx.getWorkflowState())) && System.currentTimeMillis() < st + timeout);

    if (ctx == null || !allowedStates.contains(ctx.getWorkflowState())) {
      throw new HelixException(String
          .format("Workflow \"%s\" context is empty or not in states: \"%s\"", workflowName,
              targetStates));
    }

    return ctx.getWorkflowState();
  }

  /**
   * This is a wrapper function that set default time out for monitoring workflow in 2 MINUTES.
   * If timeout happens, then it will throw a HelixException, Otherwise, it will return
   * current job state.
   *
   * @param workflowName The workflow to be monitored
   * @param targetStates Specified states that user would like to stop monitoring
   * @return A TaskState, which is current workflow state
   * @throws InterruptedException
   */
  public TaskState pollForWorkflowState(String workflowName, TaskState... targetStates)
      throws InterruptedException {
    return pollForWorkflowState(workflowName, _defaultTimeout, targetStates);
  }

  /**
   * This call will be blocked until either specified job reaches to one of the state
   * in the arguments, or timeout happens. If timeout happens, then it will throw a HelixException
   * Otherwise, it will return current job state
   *
   * @param workflowName The workflow that contains the job to monitor
   * @param jobName The specified job to monitor
   * @param timeout A long integer presents the time out, in milliseconds
   * @param states Specified states that user would like to stop monitoring
   * @return A TaskState, which is current job state
   * @throws Exception
   */
  public TaskState pollForJobState(String workflowName, String jobName, long timeout,
      TaskState... states) throws InterruptedException {
    // Get workflow config
    WorkflowConfig workflowConfig = getWorkflowConfig(workflowName);

    if (workflowConfig == null) {
      throw new HelixException(String.format("Workflow \"%s\" does not exists!", workflowName));
    }

    long timeToSleep = timeout > 100L ? 100L : timeout;

    WorkflowContext ctx;
    if (workflowConfig.isRecurring()) {
      // if it's recurring, need to reconstruct workflow and job name
      do {
        Thread.sleep(timeToSleep);
        ctx = getWorkflowContext(workflowName);
      } while ((ctx == null || ctx.getLastScheduledSingleWorkflow() == null));

      jobName = jobName.substring(workflowName.length() + 1);
      workflowName = ctx.getLastScheduledSingleWorkflow();
      jobName = TaskUtil.getNamespacedJobName(workflowName, jobName);
    }

    Set<TaskState> allowedStates = new HashSet<TaskState>(Arrays.asList(states));
    // Wait for state
    long st = System.currentTimeMillis();
    do {
      Thread.sleep(timeToSleep);
      ctx = getWorkflowContext(workflowName);
    } while ((ctx == null || ctx.getJobState(jobName) == null || !allowedStates
        .contains(ctx.getJobState(jobName))) && System.currentTimeMillis() < st + timeout);

    if (ctx == null || !allowedStates.contains(ctx.getJobState(jobName))) {
      throw new HelixException(
          String.format("Workflow \"%s\" context is null or job \"%s\" is not in states: %s",
              workflowName, jobName, allowedStates));
    }

    return ctx.getJobState(jobName);
  }

  /**
   * This is a wrapper function for monitoring job state with default timeout 2 MINUTES.
   * If timeout happens, then it will throw a HelixException, Otherwise, it will return
   * current job state
   *
   * @param workflowName The workflow that contains the job to monitor
   * @param jobName The specified job to monitor
   * @param states Specified states that user would like to stop monitoring
   * @return A TaskState, which is current job state
   * @throws Exception
   */
  public TaskState pollForJobState(String workflowName, String jobName, TaskState... states)
      throws InterruptedException {
    return pollForJobState(workflowName, jobName, _defaultTimeout, states);
  }
}
