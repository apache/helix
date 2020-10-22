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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(TaskDriver.class);

  /** Default time out for monitoring workflow or job state */
  private final static int DEFAULT_TIMEOUT = 5 * 60 * 1000; /* 5 mins */

  /** Default sleep time for requests */
  private final static long DEFAULT_SLEEP = 1000L; /* 1 second */

  /** The illegal job states for job to accept new tasks */
  private final static Set<TaskState> ILLEGAL_JOB_STATES_FOR_TASK_MODIFICATION = new HashSet<>(
      Arrays.asList(TaskState.TIMING_OUT, TaskState.TIMED_OUT, TaskState.FAILING, TaskState.FAILED,
          TaskState.ABORTED, TaskState.COMPLETED, TaskState.STOPPING, TaskState.STOPPED));

  // HELIX-619 This is a temporary solution for too many ZK nodes issue.
  // Limit workflows/jobs creation to prevent the problem.
  //
  // Note this limitation should be smaller than ZK capacity. If current nodes count already exceeds
  // the CAP, the verification method will not throw exception since the getChildNames() call will
  // return empty list.
  //
  // TODO Implement or configure the limitation in ZK server.
  private final static long DEFAULT_CONFIGS_LIMITATION =
      HelixUtil.getSystemPropertyAsLong(SystemPropertyKeys.TASK_CONFIG_LIMITATION, 100000L);
  private final static String TASK_START_TIME_KEY = "START_TIME";
  protected long _configsLimitation = DEFAULT_CONFIGS_LIMITATION;

  private final HelixDataAccessor _accessor;
  private final HelixPropertyStore<ZNRecord> _propertyStore;
  private final HelixAdmin _admin;
  private final String _clusterName;

  public TaskDriver(HelixManager manager) {
    this(manager.getClusterManagmentTool(), manager.getHelixDataAccessor(),
        manager.getHelixPropertyStore(), manager.getClusterName());
  }

  @Deprecated
  public TaskDriver(RealmAwareZkClient client, String clusterName) {
    this(client, new ZkBaseDataAccessor<>(client), clusterName);
  }

  @Deprecated
  public TaskDriver(RealmAwareZkClient client, ZkBaseDataAccessor<ZNRecord> baseAccessor,
      String clusterName) {
    this(new ZKHelixAdmin(client), new ZKHelixDataAccessor(clusterName, baseAccessor),
        new ZkHelixPropertyStore<>(baseAccessor, PropertyPathBuilder.propertyStore(clusterName),
            null),
        clusterName);
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

  /**
   * Schedules a new workflow
   * @param flow
   */
  public void start(Workflow flow) {
    LOG.info("Starting workflow " + flow.getName());
    flow.validate();

    validateZKNodeLimitation(flow.getJobConfigs().keySet().size() + 1);

    WorkflowConfig newWorkflowConfig =
        new WorkflowConfig.Builder(flow.getWorkflowConfig()).setWorkflowId(flow.getName()).build();

    Map<String, String> jobTypes = new HashMap<>();
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
    if (!TaskUtil.createWorkflowConfig(_accessor, flow.getName(), newWorkflowConfig)) {
      // workflow config creation failed; try to delete job configs back
      Set<String> failedJobRemoval = new HashSet<>();
      for (String job : flow.getJobConfigs().keySet()) {
        if (!TaskUtil.removeJobConfig(_accessor, job)) {
          failedJobRemoval.add(job);
        }
      }
      throw new HelixException(String.format(
          "Failed to add workflow configuration for workflow %s. It's possible that a workflow of the same name already exists or there was a connection issue. JobConfig deletion attempted but failed for the following jobs: %s",
          flow.getName(), failedJobRemoval));
    }
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
   * Example:
   * _driver = new TaskDriver ...
   * WorkflowConfig currentWorkflowConfig = _driver.getWorkflowCfg(_manager, workflow);
   * WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(currentWorkflowConfig);
   * // make needed changes to the config here
   * configBuilder.setXXX();
   * // update workflow configuration
   * _driver.updateWorkflow(workflow, configBuilder.build());
   * @param workflow
   * @param newWorkflowConfig
   */
  public void updateWorkflow(String workflow, WorkflowConfig newWorkflowConfig) {
    if (newWorkflowConfig.getWorkflowId() == null || newWorkflowConfig.getWorkflowId().isEmpty()) {
      newWorkflowConfig.getRecord()
          .setSimpleField(WorkflowConfig.WorkflowConfigProperty.WorkflowID.name(), workflow);
    }
    if (workflow == null || !workflow.equals(newWorkflowConfig.getWorkflowId())) {
      throw new HelixException(String.format(
          "Workflow name {%s} does not match the workflow Id from WorkflowConfig {%s}", workflow,
          newWorkflowConfig.getWorkflowId()));
    }

    WorkflowConfig currentConfig = TaskUtil.getWorkflowConfig(_accessor, workflow);
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
  }

  /**
   * Creates a new named job queue (workflow)
   * @param queue
   */
  public void createQueue(JobQueue queue) {
    start(queue);
  }

  /**
   * Remove all completed or failed jobs in a job queue
   * Same as {@link #cleanupQueue(String)}
   * @param queue name of the queue
   * @throws Exception
   */
  public void flushQueue(String queue) {
    cleanupQueue(queue);
  }

  /**
   * Delete a job from an existing named queue,
   * the queue has to be stopped prior to this call
   * @param queue queue name
   * @param job job name, denamespaced
   */
  public void deleteJob(final String queue, final String job) {
    deleteNamespacedJob(queue, TaskUtil.getNamespacedJobName(queue, job), false);
  }

  /**
   * Delete a job from an existing named queue,
   * the queue has to be stopped prior to this call
   * @param queue queue name
   * @param job job name, denamespaced
   * @param forceDelete CAUTION: if set true, all job's related zk nodes will
   *          be clean up from zookeeper even if its workflow information can not be found.
   */
  public void deleteJob(final String queue, final String job, boolean forceDelete) {
    deleteNamespacedJob(queue, TaskUtil.getNamespacedJobName(queue, job), forceDelete);
  }

  /**
   * Delete a job from an existing named queue,
   * the queue has to be stopped prior to this call
   * @param queue queue name
   * @param job job name: namespaced job name
   * @param forceDelete CAUTION: if set true, all job's related zk nodes will
   *          be removed from zookeeper even if its JobQueue information can not be found.
   */
  public void deleteNamespacedJob(final String queue, final String job, boolean forceDelete) {
    WorkflowConfig jobQueueConfig = TaskUtil.getWorkflowConfig(_accessor, queue);
    boolean isRecurringWorkflow;

    // Force deletion of a job
    if (forceDelete) {
      // remove all job-related ZNodes
      LOG.info("Forcefully removing job: {} from queue: {}", job, queue);
      if (!TaskUtil.removeJob(_accessor, _propertyStore, job)) {
        LOG.info("Failed to delete job: {} from queue: {}", job, queue);
        throw new HelixException("Failed to delete job: " + job + " from queue: " + queue);
      }
      // In case this was a recurrent workflow, remove it from last scheduled queue as well
      if (jobQueueConfig != null) {
        isRecurringWorkflow = jobQueueConfig.getScheduleConfig() != null
            && jobQueueConfig.getScheduleConfig().isRecurring();
        if (isRecurringWorkflow) {
          deleteJobFromLastScheduledQueue(queue, TaskUtil.getDenamespacedJobName(queue, job));
        }
      }
      return;
    }

    // Regular, non-force, deletion of a job
    if (jobQueueConfig == null) {
      throw new IllegalArgumentException(
          String.format("JobQueue %s's config is not found!", queue));
    }
    if (!jobQueueConfig.isJobQueue()) {
      throw new IllegalArgumentException(String.format("%s is not a queue!", queue));
    }
    isRecurringWorkflow = jobQueueConfig.getScheduleConfig() != null
        && jobQueueConfig.getScheduleConfig().isRecurring();
    String denamespacedJob = TaskUtil.getDenamespacedJobName(queue, job);
    if (isRecurringWorkflow) {
      deleteJobFromLastScheduledQueue(queue, denamespacedJob);
    }
    deleteJobFromQueue(queue, denamespacedJob);
  }

  /**
   * Delete the given job from the last-scheduled queue for recurrent workflows.
   * @param queue
   * @param denamespacedJob
   */
  private void deleteJobFromLastScheduledQueue(String queue, String denamespacedJob) {
    // delete job from the last scheduled queue if there exists one.
    WorkflowContext wCtx = TaskUtil.getWorkflowContext(_propertyStore, queue);
    String lastScheduledQueue = null;
    if (wCtx != null) {
      lastScheduledQueue = wCtx.getLastScheduledSingleWorkflow();
    }
    if (lastScheduledQueue != null) {
      WorkflowConfig lastWorkflowCfg = TaskUtil.getWorkflowConfig(_accessor, lastScheduledQueue);
      if (lastWorkflowCfg != null) {
        deleteJobFromQueue(lastScheduledQueue, denamespacedJob);
      }
    }
  }

  /**
   * Delete a job from a scheduled (non-recurrent) queue.
   * @param queue
   * @param job this must be a namespaced job name
   */
  private void deleteJobFromQueue(final String queue, final String job) {
    WorkflowContext workflowCtx = TaskUtil.getWorkflowContext(_propertyStore, queue);
    String workflowState = (workflowCtx != null) ? workflowCtx.getWorkflowState().name()
        : TaskState.NOT_STARTED.name();

    if (workflowState.equals(TaskState.IN_PROGRESS.name())) {
      throw new IllegalStateException("Queue " + queue + " is still running!");
    }

    if (workflowState.equals(TaskState.COMPLETED.name())
        || workflowState.equals(TaskState.FAILED.name())
        || workflowState.equals(TaskState.ABORTED.name())) {
      LOG.warn(
          "Queue " + queue + " has already reached its final state, skip deleting job from it.");
      return;
    }

    if (!TaskUtil.removeJobsFromWorkflow(_accessor, _propertyStore, queue,
        Collections.singleton(TaskUtil.getNamespacedJobName(queue, job)), true)) {
      LOG.error("Failed to delete job " + job + " from queue " + queue);
      throw new HelixException("Failed to delete job " + job + " from queue " + queue);
    }
  }

  /**
   * Adds a new job to the end an existing named queue.
   * @param queue
   * @param job
   * @param jobBuilder
   * @throws Exception
   */
  public void enqueueJob(final String queue, final String job, JobConfig.Builder jobBuilder) {
    enqueueJobs(queue, Collections.singletonList(job), Collections.singletonList(jobBuilder));
  }

  /**
   * Batch add jobs to queues that garantee
   * @param queue
   * @param jobs
   * @param jobBuilders
   */
  public void enqueueJobs(final String queue, final List<String> jobs,
      final List<JobConfig.Builder> jobBuilders) {

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

    // Fail the operation if adding new jobs will cause the queue to reach its capacity limit
    workflowConfig = TaskUtil.getWorkflowConfig(_accessor, queue);
    if (workflowConfig.getJobDag().size() + jobs.size() >= capacity) {
      throw new IllegalStateException(
          String.format("Queue %s already reaches its max capacity %d, failed to add %s", queue,
              capacity, jobs.toString()));
    }

    validateZKNodeLimitation(1);
    final List<JobConfig> jobConfigs = new ArrayList<>();
    final List<String> namespacedJobNames = new ArrayList<>();
    final List<String> jobTypeList = new ArrayList<>();

    try {
      for (int i = 0; i < jobBuilders.size(); i++) {
        // Create the job to ensure that it validates
        JobConfig jobConfig = jobBuilders.get(i).setWorkflow(queue).build();
        String namespacedJobName = TaskUtil.getNamespacedJobName(queue, jobs.get(i));

        // add job config first.
        addJobConfig(namespacedJobName, jobConfig);
        jobConfigs.add(jobConfig);
        namespacedJobNames.add(namespacedJobName);
        jobTypeList.add(jobConfig.getJobType());
      }
    } catch (HelixException e) {
      LOG.error("Failed to add job configs {}. Remove them all!", jobs.toString());
      for (String job : jobs) {
        String namespacedJobName = TaskUtil.getNamespacedJobName(queue, job);
        TaskUtil.removeJobConfig(_accessor, namespacedJobName);
      }
    }

    // update the job dag to append the job to the end of the queue.
    DataUpdater<ZNRecord> updater = currentData -> {
      if (currentData == null) {
        // For some reason, the WorkflowConfig for this JobQueue doesn't exist
        // In this case, we cannot proceed and must alert the user
        throw new HelixException(
            String.format("enqueueJobs DataUpdater: JobQueue %s config is not found!", queue));
      }

      // Add the node to the existing DAG
      JobDag jobDag = JobDag
          .fromJson(currentData.getSimpleField(WorkflowConfig.WorkflowConfigProperty.Dag.name()));
      Set<String> allNodes = jobDag.getAllNodes();
      if (capacity > 0 && allNodes.size() + jobConfigs.size() >= capacity) {
        // Remove previously added jobConfigs if adding new jobs will cause exceeding capacity
        // limit. Removing the job configs is necessary to avoid multiple threads adding jobs at the
        // same time and cause overcapacity queue
        for (String job : jobs) {
          String namespacedJobName = TaskUtil.getNamespacedJobName(queue, job);
          TaskUtil.removeJobConfig(_accessor, namespacedJobName);
        }
        throw new IllegalStateException(
            String.format("Queue %s already reaches its max capacity %d, failed to add %s", queue,
                capacity, jobs.toString()));
      }

      String lastNodeName = null;
      for (int i = 0; i < namespacedJobNames.size(); i++) {
        String namespacedJobName = namespacedJobNames.get(i);
        if (allNodes.contains(namespacedJobName)) {
          throw new IllegalStateException(String
              .format("Could not add to queue %s, job %s already exists", queue, jobs.get(i)));
        }
        jobDag.addNode(namespacedJobName);

        // Add the node to the end of the queue
        String candidate = null;
        if (lastNodeName == null) {
          for (String node : allNodes) {
            if (!node.equals(namespacedJobName) && jobDag.getDirectChildren(node).isEmpty()) {
              candidate = node;
              break;
            }
          }
        } else {
          candidate = lastNodeName;
        }
        if (candidate != null) {
          jobDag.addParentToChild(candidate, namespacedJobName);
          lastNodeName = namespacedJobName;
        }
      }

      // Add job type if job type is not null
      Map<String, String> jobTypes =
          currentData.getMapField(WorkflowConfig.WorkflowConfigProperty.JobTypes.name());
      for (String jobType : jobTypeList) {
        if (jobType != null) {
          if (jobTypes == null) {
            jobTypes = new HashMap<>();
          }
          jobTypes.put(queue, jobType);
        }
      }

      if (jobTypes != null) {
        currentData.setMapField(WorkflowConfig.WorkflowConfigProperty.JobTypes.name(), jobTypes);
      }
      // Save the updated DAG
      try {
        currentData.setSimpleField(WorkflowConfig.WorkflowConfigProperty.Dag.name(),
            jobDag.toJson());
      } catch (Exception e) {
        throw new IllegalStateException(
            String.format("Could not add jobs %s to queue %s", jobs.toString(), queue), e);
      }
      return currentData;
    };

    String path = _accessor.keyBuilder().resourceConfig(queue).getPath();
    boolean status = _accessor.getBaseDataAccessor().update(path, updater, AccessOption.PERSISTENT);
    if (!status) {
      LOG.error("Failed to update WorkflowConfig, remove all jobs {}", jobs.toString());
      for (String job : jobs) {
        TaskUtil.removeJobConfig(_accessor, job);
      }
      throw new HelixException("Failed to enqueue job");
    }
  }

  /**
   * Add task to a running (IN-PROGRESS) job or a job which has not started yet. Timeout for this
   * operation is the default timeout which is 5 minutes. {@link TaskDriver#DEFAULT_TIMEOUT}
   * Note1: Task cannot be added if the job is in an illegal state. A job can accept
   * new task if the job is in-progress or it has not started yet.
   * Note2: The tasks can only be added to non-targeted jobs.
   * Note3: The taskID for the new task should be unique. If not, this API throws an exception.
   * Note4: In case of timeout exception, it is the user's responsibility to check whether the task
   * has been successfully added or not.
   * @param workflowName
   * @param jobName
   * @param taskConfig
   * @throws TimeoutException if the outcome of the task addition is unknown and cannot be verified
   * @throws IllegalArgumentException if the inputs are invalid
   * @throws HelixException if the job is not in the states to accept a new task or if there is any
   *           issue in updating jobConfig.
   */
  public void addTask(String workflowName, String jobName, TaskConfig taskConfig)
      throws TimeoutException, InterruptedException {
    addTask(workflowName, jobName, taskConfig, DEFAULT_TIMEOUT);
  }

  /**
   * Add task to a running (IN-PROGRESS) job or a job which has not started yet
   * Note1: Task cannot be added if the job is in an illegal state. A job can accept
   * new task if the job is in-progress or it has not started yet.
   * Note2: The tasks can only be added to non-targeted jobs.
   * Note3: The taskID for the new task should be unique. If not, this API throws an exception.
   * Note4: In case of timeout exception, it is the user's responsibility to check whether the task
   * has been successfully added or not.
   * Note5: timeout is the time that this API checks whether the task has been successfully added or
   * not.
   * @param workflowName
   * @param jobName
   * @param taskConfig
   * @param timeoutMs
   * @throws TimeoutException if the outcome of the task addition is unknown and cannot be verified
   * @throws IllegalArgumentException if the inputs are invalid
   * @throws HelixException if the job is not in the states to accept a new task or if there is any
   *           issue in updating jobConfig.
   */
  public void addTask(String workflowName, String jobName, TaskConfig taskConfig, long timeoutMs)
      throws TimeoutException, InterruptedException {

    if (timeoutMs < DEFAULT_SLEEP) {
      throw new IllegalArgumentException(
          String.format("Timeout is less than the minimum acceptable timeout value which is %s ms",
              DEFAULT_SLEEP));
    }

    long endTime = System.currentTimeMillis() + timeoutMs;

    validateConfigsForTaskModifications(workflowName, jobName, taskConfig);

    String nameSpaceJobName = TaskUtil.getNamespacedJobName(workflowName, jobName);
    JobConfig jobConfig = TaskUtil.getJobConfig(_accessor, nameSpaceJobName);
    for (String taskEntry : jobConfig.getMapConfigs().keySet()) {
      if (taskEntry.equals(taskConfig.getId())) {
        throw new HelixException(
            "Task cannot be added because another task with the same ID already exists!");
      }
    }

    WorkflowContext workflowContext = getWorkflowContext(workflowName);
    JobContext jobContext = getJobContext(nameSpaceJobName);
    // If workflow context or job context is null. It means job has not been started. Hence task can
    // be added to the job
    if (workflowContext != null && jobContext != null) {
      TaskState jobState = workflowContext.getJobState(nameSpaceJobName);
      if (jobState != null && ILLEGAL_JOB_STATES_FOR_TASK_MODIFICATION.contains(jobState)) {
        throw new HelixException("Job " + nameSpaceJobName
            + " is in illegal state for task addition. Job State is " + jobState);
      }
    }
    addTaskToJobConfig(workflowName, jobName, taskConfig, endTime);
  }

  /**
   * Delete an existing task from a running (IN-PROGRESS) job or a job which has not started yet.
   * Timeout for this operation is the default timeout which is 5 minutes.
   * {@link TaskDriver#DEFAULT_TIMEOUT}
   * Note1: Task cannot be deleted from the job which is in an illegal state. Task can be deleted
   * from the job if the job is in-progress or it has not started yet.
   * Note2: The tasks can only be deleted from non-targeted jobs.
   * Note3: In case of timeout exception, it is the user's responsibility to check whether the task
   * has been successfully deleted or not.
   * Note4: timeout is the time that this API checks whether the task has been successfully deleted
   * or not.
   * @param workflowName
   * @param jobName
   * @param taskID
   * @throws TimeoutException if the outcome of the task deletion is unknown and cannot be verified
   * @throws IllegalArgumentException if the inputs are invalid
   * @throws HelixException if the job is not in the states to accept a new task or if there is any
   *           issue in updating jobConfig.
   */
  public void deleteTask(String workflowName, String jobName, String taskID)
      throws TimeoutException, InterruptedException {
    deleteTask(workflowName, jobName, taskID, DEFAULT_TIMEOUT);
  }

  /**
   * Delete an existing task from a running (IN-PROGRESS) job or a job which has not started yet.
   * Note1: Task cannot be deleted from the job which is in an illegal state. Task can be deleted
   * from the job if the job is in-progress or it has not started yet.
   * Note2: The tasks can only be deleted from non-targeted jobs.
   * Note3: In case of timeout exception, it is the user's responsibility to check whether the task
   * has been successfully deleted or not.
   * Note4: timeout is the time that this API checks whether the task has been successfully deleted
   * or not.
   * @param workflowName
   * @param jobName
   * @param taskID
   * @param timeoutMs
   * @throws TimeoutException if the outcome of the task deletion is unknown and cannot be verified
   * @throws IllegalArgumentException if the inputs are invalid
   * @throws HelixException if the job is not in the states to accept a new task or if there is any
   *           issue in updating jobConfig.
   */
  public void deleteTask(String workflowName, String jobName, String taskID, long timeoutMs)
      throws TimeoutException, InterruptedException {
    long endTime = System.currentTimeMillis() + timeoutMs;

    String nameSpaceJobName = TaskUtil.getNamespacedJobName(workflowName, jobName);
    JobConfig jobConfig = getJobConfig(nameSpaceJobName);
    if (jobConfig == null) {
      throw new IllegalArgumentException("Job " + nameSpaceJobName + " config does not exist!");
    }

    TaskConfig taskConfig = null;
    Map<String, TaskConfig> allTaskConfigs = jobConfig.getTaskConfigMap();
    for (Map.Entry<String, TaskConfig> entry : allTaskConfigs.entrySet()) {
      if (entry.getKey().equals(taskID)) {
        taskConfig = entry.getValue();
      }
    }

    validateConfigsForTaskModifications(workflowName, jobName, taskConfig);

    WorkflowContext workflowContext = getWorkflowContext(workflowName);
    JobContext jobContext = getJobContext(nameSpaceJobName);
    // If workflow context or job context is null. It means job has not been started. Hence task can
    // be deleted from the job
    if (workflowContext != null && jobContext != null) {
      TaskState jobState = workflowContext.getJobState(nameSpaceJobName);
      if (jobState != null && ILLEGAL_JOB_STATES_FOR_TASK_MODIFICATION.contains(jobState)) {
        throw new HelixException("Job " + nameSpaceJobName
            + " is in illegal state for task deletion. Job State is " + jobState);
      }
    }
    deleteTaskFromJobConfig(workflowName, jobName, taskID, endTime);
  }

  /**
   * The helper method which check the workflow, job and task configs to determine if new task can
   * be added/deleted to/from the job
   * @param workflowName
   * @param jobName
   * @param taskConfig
   */
  private void validateConfigsForTaskModifications(String workflowName, String jobName,
      TaskConfig taskConfig) {
    WorkflowConfig workflowConfig = TaskUtil.getWorkflowConfig(_accessor, workflowName);
    String nameSpaceJobName = TaskUtil.getNamespacedJobName(workflowName, jobName);
    JobConfig jobConfig = TaskUtil.getJobConfig(_accessor, nameSpaceJobName);

    if (workflowConfig == null) {
      throw new IllegalArgumentException(
          String.format("Workflow config for workflow %s does not exist!", workflowName));
    }

    if (jobConfig == null) {
      throw new IllegalArgumentException(
          String.format("Job config for job %s does not exist!", nameSpaceJobName));
    }

    if (taskConfig == null) {
      throw new IllegalArgumentException("TaskConfig is null!");
    }

    if (taskConfig.getId() == null) {
      throw new HelixException("Task cannot be added or deleted because taskID is null!");
    }

    if (jobConfig.getTargetResource() != null) {
      throw new HelixException(String.format(
          "Job %s is a targeted job. New task cannot be added/deleted to/from this job!",
          nameSpaceJobName));
    }

    if ((taskConfig.getCommand() == null) == (jobConfig.getCommand() == null)) {
      throw new HelixException("Command must exist in either job or task, not both!");
    }
  }

  /**
   * A helper method which adds a new task config to the job config and verifies if task is added to
   * the context by the controller.
   * @param workflowName
   * @param jobName
   * @param taskConfig
   * @param endTime
   * @throws InterruptedException
   * @throws TimeoutException
   */
  private void addTaskToJobConfig(String workflowName, String jobName, TaskConfig taskConfig,
      long endTime) throws InterruptedException, TimeoutException {
    String nameSpaceJobName = TaskUtil.getNamespacedJobName(workflowName, jobName);
    DataUpdater<ZNRecord> updater = currentData -> {
      if (currentData != null) {
        currentData.setMapField(taskConfig.getId(), taskConfig.getConfigMap());
      } else {
        LOG.error("JobConfig DataUpdater: Fails to update JobConfig. CurrentData is null.");
      }
      return currentData;
    };

    String path = _accessor.keyBuilder().resourceConfig(nameSpaceJobName).getPath();
    boolean status = _accessor.getBaseDataAccessor().update(path, updater, AccessOption.PERSISTENT);
    if (!status) {
      LOG.error("Failed to add task to the job {}", nameSpaceJobName);
      throw new HelixException("Failed to add task to the job!");
    }

    WorkflowContext workflowContext =
        _accessor.getProperty(_accessor.keyBuilder().workflowContextZNode(workflowName));
    JobContext jobContext =
        _accessor.getProperty(_accessor.keyBuilder().jobContextZNode(workflowName, jobName));

    if (workflowContext == null || jobContext == null) {
      return;
    }

    String taskID = taskConfig.getId();
    while (System.currentTimeMillis() <= endTime) {
      jobContext =
          _accessor.getProperty(_accessor.keyBuilder().jobContextZNode(workflowName, jobName));
      workflowContext =
          _accessor.getProperty(_accessor.keyBuilder().workflowContextZNode(workflowName));
      if (jobContext.getTaskIdPartitionMap().containsKey(taskID)
          && workflowContext.getJobState(nameSpaceJobName) == TaskState.IN_PROGRESS) {
        return;
      }

      Thread.sleep(DEFAULT_SLEEP);
    }
    throw new TimeoutException("An unexpected issue happened while task being added to the job!");
  }

  /**
   * A helper method which deletes an existing task from the job config and verifies if task is
   * deleted from the context by the controller.
   * @param workflowName
   * @param jobName
   * @param taskID
   * @param endTime
   * @throws InterruptedException
   * @throws TimeoutException
   */
  private void deleteTaskFromJobConfig(String workflowName, String jobName, String taskID,
      long endTime) throws InterruptedException, TimeoutException {
    String nameSpaceJobName = TaskUtil.getNamespacedJobName(workflowName, jobName);
    DataUpdater<ZNRecord> taskRemover = new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        if (currentData != null) {
          Map<String, Map<String, String>> taskMap = currentData.getMapFields();
          if (taskMap == null) {
            LOG.warn("Could not update the jobConfig: " + jobName + " Znode MapField is null.");
            return null;
          }
          Map<String, Map<String, String>> newTaskMap = new HashMap<String, Map<String, String>>();
          for (Map.Entry<String, Map<String, String>> entry : taskMap.entrySet()) {
            if (!entry.getKey().equals(taskID)) {
              newTaskMap.put(entry.getKey(), entry.getValue());
            }
          }
          currentData.setMapFields(newTaskMap);
        }
        return currentData;
      }
    };

    String path = _accessor.keyBuilder().resourceConfig(nameSpaceJobName).getPath();
    boolean status =
        _accessor.getBaseDataAccessor().update(path, taskRemover, AccessOption.PERSISTENT);
    if (!status) {
      LOG.error("Failed to delete task from the job {}", nameSpaceJobName);
      throw new HelixException("Failed to delete task from the job");
    }

    WorkflowContext workflowContext =
        _accessor.getProperty(_accessor.keyBuilder().workflowContextZNode(workflowName));
    JobContext jobContext =
        _accessor.getProperty(_accessor.keyBuilder().jobContextZNode(workflowName, jobName));

    if (workflowContext == null || jobContext == null) {
      return;
    }

    while (System.currentTimeMillis() <= endTime) {
      jobContext =
          _accessor.getProperty(_accessor.keyBuilder().jobContextZNode(workflowName, jobName));
      workflowContext =
          _accessor.getProperty(_accessor.keyBuilder().workflowContextZNode(workflowName));
      if (!jobContext.getTaskIdPartitionMap().containsKey(taskID)
          && workflowContext.getJobState(nameSpaceJobName) == TaskState.IN_PROGRESS) {
        return;
      }
      Thread.sleep(DEFAULT_SLEEP);
    }
    throw new TimeoutException(
        "An unexpected issue happened while task being deleted from the job!");
  }

  /**
   * Keep the old name of API for backward compatibility
   * @param queue
   */
  @Deprecated
  public void cleanupJobQueue(String queue) {
    cleanupQueue(queue);
  }

  /**
   * Remove all jobs that are in final states (ABORTED, FAILED, COMPLETED) from the job queue. The
   * job config, job context will be removed from Zookeeper.
   * @param queue The name of job queue
   */
  public void cleanupQueue(String queue) {
    WorkflowConfig workflowConfig = TaskUtil.getWorkflowConfig(_accessor, queue);

    if (workflowConfig == null) {
      throw new IllegalArgumentException("Queue " + queue + " does not yet exist!");
    }

    if (!workflowConfig.isJobQueue() || workflowConfig.isTerminable()) {
      throw new IllegalArgumentException(queue + " is not a queue!");
    }

    WorkflowContext wCtx = TaskUtil.getWorkflowContext(_propertyStore, queue);
    if (wCtx == null || wCtx.getWorkflowState() == null) {
      throw new IllegalStateException("Queue " + queue + " does not have a valid work state!");
    }

    Set<String> jobs = new HashSet<>();
    for (String jobNode : workflowConfig.getJobDag().getAllNodes()) {
      TaskState curState = wCtx.getJobState(jobNode);
      if (curState != null && curState == TaskState.ABORTED || curState == TaskState.COMPLETED
          || curState == TaskState.FAILED) {
        jobs.add(jobNode);
      }
    }

    TaskUtil.removeJobsFromWorkflow(_accessor, _propertyStore, queue, jobs, true);
  }

  /**
   * Add new job config to cluster by way of create
   */
  private void addJobConfig(String job, JobConfig jobConfig) {
    LOG.info("Add job configuration " + job);

    // Set the job configuration
    JobConfig newJobCfg = new JobConfig(job, jobConfig);
    if (!TaskUtil.createJobConfig(_accessor, job, newJobCfg)) {
      throw new HelixException("Failed to add job configuration for job " + job
          + ". It's possible that a job of the same name already exists or there was a connection issue");
    }
  }

  /**
   * Public method to resume a workflow/queue.
   * @param workflow
   */
  public void resume(String workflow) {
    setWorkflowTargetState(workflow, TargetState.START);
  }

  /**
   * Public async method to stop a workflow/queue.
   * This call only send STOP command to Helix, it does not check
   * whether the workflow (all jobs) has been stopped yet.
   * @param workflow
   */
  public void stop(String workflow) {
    setWorkflowTargetState(workflow, TargetState.STOP);
  }

  /**
   * Public sync method to stop a workflow/queue with timeout
   * Basically the workflow and all of its jobs has been stopped if this method return success.
   * @param workflow The workflow name
   * @param timeout The timeout for stopping workflow/queue in milisecond
   */
  public void waitToStop(String workflow, long timeout) throws InterruptedException {
    setWorkflowTargetState(workflow, TargetState.STOP);
    long endTime = System.currentTimeMillis() + timeout;

    while (System.currentTimeMillis() <= endTime) {
      WorkflowContext workflowContext = getWorkflowContext(workflow);

      if (workflowContext == null
          || !TaskState.STOPPED.equals(workflowContext.getWorkflowState())) {
        Thread.sleep(DEFAULT_SLEEP);
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
   * @param workflow
   */
  public void delete(String workflow) {
    delete(workflow, false);
  }

  /**
   * Public method to delete a workflow/queue.
   * @param workflow
   * @param forceDelete, CAUTION: if set true, workflow and all of its jobs' related zk nodes will
   *          be clean up immediately from zookeeper, no matter whether there are jobs
   *          are running or not.
   *          Enabling this option can cause a ZooKeeper delete failure as Helix might
   *          inadvertently try to write the deleted ZNodes back to ZooKeeper. Also this option
   *          might corrupt Task Framework cache. At any rate, it shouldn't be used while the
   *          controller is running or the cluster is active.
   */
  public void delete(String workflow, boolean forceDelete) {
    WorkflowContext wCtx = TaskUtil.getWorkflowContext(_propertyStore, workflow);
    if (forceDelete) {
      // if forceDelete, remove the workflow and its jobs immediately from zookeeper.
      LOG.info("Forcefully removing workflow: " + workflow);
      removeWorkflowFromZK(workflow);
    } else {
      // Set the workflow target state to DELETE, and let Helix controller to remove the workflow.
      // Controller may remove the workflow instantly, so record context before set DELETE state.
      setWorkflowTargetState(workflow, TargetState.DELETE);
    }

    // Delete all previously scheduled workflows.
    if (wCtx != null && wCtx.getScheduledWorkflows() != null) {
      for (String scheduledWorkflow : wCtx.getScheduledWorkflows()) {
        if (forceDelete) {
          // Only directly delete it if it's force delete. Otherwise, it will cause a race condition
          // where contexts would get written back to ZK from cache
          WorkflowContext scheduledWorkflowCtx =
              TaskUtil.getWorkflowContext(_propertyStore, scheduledWorkflow);
          if (scheduledWorkflowCtx != null
              && scheduledWorkflowCtx.getFinishTime() != WorkflowContext.UNFINISHED) {
            removeWorkflowFromZK(scheduledWorkflow);
          }
        } else {
          setWorkflowTargetState(scheduledWorkflow, TargetState.DELETE);
        }
      }
    }
  }

  private void removeWorkflowFromZK(String workflow) {
    Set<String> jobSet = new HashSet<>();
    // Note that even WorkflowConfig is null, if WorkflowContext exists, still need to remove
    // workflow
    WorkflowConfig wCfg = TaskUtil.getWorkflowConfig(_accessor, workflow);
    if (wCfg != null) {
      jobSet.addAll(wCfg.getJobDag().getAllNodes());
    }
    boolean success = TaskUtil.removeWorkflow(_accessor, _propertyStore, workflow, jobSet);
    if (!success) {
      LOG.info("Failed to delete the workflow " + workflow);
      throw new HelixException("Failed to delete the workflow " + workflow);
    }
  }

  /**
   * TODO: IdealStates are no longer used by Task Framework. This function deletes IdealStates for
   * TODO: backward compatability purpose; this behavior will be removed later.
   * Public synchronized method to wait for a delete operation to fully complete with timeout.
   * When this method returns, it means that a queue (workflow) has been completely deleted, meaning
   * its IdealState, WorkflowConfig, and WorkflowContext have all been deleted.
   * @param workflow workflow/jobqueue name
   * @param timeout duration to give to delete operation to completion
   */
  public void deleteAndWaitForCompletion(String workflow, long timeout)
      throws InterruptedException {
    delete(workflow);
    long endTime = System.currentTimeMillis() + timeout;

    // For checking whether delete completed
    BaseDataAccessor baseDataAccessor = _accessor.getBaseDataAccessor();
    PropertyKey.Builder keyBuilder = _accessor.keyBuilder();

    String idealStatePath = keyBuilder.idealStates(workflow).getPath();
    String workflowConfigPath = keyBuilder.resourceConfig(workflow).getPath();
    String workflowContextPath = keyBuilder.workflowContext(workflow).getPath();

    while (System.currentTimeMillis() <= endTime) {
      if (baseDataAccessor.exists(idealStatePath, AccessOption.PERSISTENT)
          || baseDataAccessor.exists(workflowConfigPath, AccessOption.PERSISTENT)
          || baseDataAccessor.exists(workflowContextPath, AccessOption.PERSISTENT)) {
        Thread.sleep(DEFAULT_SLEEP);
      } else {
        return;
      }
    }

    // Deletion failed: check which step of deletion failed to complete and create an error message
    StringBuilder failed = new StringBuilder();
    if (baseDataAccessor.exists(idealStatePath, AccessOption.PERSISTENT)) {
      failed.append("IdealState ");
    }
    if (baseDataAccessor.exists(workflowConfigPath, AccessOption.PERSISTENT)) {
      failed.append("WorkflowConfig ");
    }
    if (baseDataAccessor.exists(workflowContextPath, AccessOption.PERSISTENT)) {
      failed.append("WorkflowContext ");
    }
    throw new HelixException(
        String.format(
            "Failed to delete the workflow/queue %s within %d milliseconds. "
                + "The following components still remain: %s",
            workflow, timeout, failed.toString()));
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
    if (state != TargetState.DELETE && workflowContext != null
        && workflowContext.getFinishTime() != WorkflowContext.UNFINISHED) {
      // Should not update target state for completed workflow
      LOG.info("Workflow " + workflow + " is already completed, skip to update its target state "
          + state);
      return;
    }

    DataUpdater<ZNRecord> updater = currentData -> {
      if (currentData != null) {
        currentData.setSimpleField(WorkflowConfig.WorkflowConfigProperty.TargetState.name(),
            state.name());
      } else {
        LOG.warn("TargetState DataUpdater: Fails to update target state. CurrentData is null.");
      }
      return currentData;
    };

    PropertyKey workflowConfigKey = TaskUtil.getWorkflowConfigKey(_accessor, workflow);
    _accessor.getBaseDataAccessor().update(workflowConfigKey.getPath(), updater,
        AccessOption.PERSISTENT);
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
   * @return
   */
  public Map<String, WorkflowConfig> getWorkflows() {
    Map<String, WorkflowConfig> workflowConfigMap = new HashMap<>();
    Map<String, ResourceConfig> resourceConfigMap =
        _accessor.getChildValuesMap(_accessor.keyBuilder().resourceConfigs(), true);

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
    Set<TaskState> allowedStates = new HashSet<>(Arrays.asList(targetStates));

    long timeToSleep = timeout > 100L ? 100L : timeout;
    do {
      Thread.sleep(timeToSleep);
      ctx = getWorkflowContext(workflowName);
    } while ((ctx == null || ctx.getWorkflowState() == null
        || !allowedStates.contains(ctx.getWorkflowState()))
        && System.currentTimeMillis() < st + timeout);

    if (ctx == null || !allowedStates.contains(ctx.getWorkflowState())) {
      throw new HelixException(String.format(
          "Workflow \"%s\" context is empty or not in states: \"%s\", current state: \"%s\"",
          workflowName, Arrays.asList(targetStates),
          ctx == null ? "null" : ctx.getWorkflowState().toString()));
    }

    return ctx.getWorkflowState();
  }

  /**
   * This is a wrapper function that set default time out for monitoring workflow in 2 MINUTES.
   * If timeout happens, then it will throw a HelixException, Otherwise, it will return
   * current job state.
   * @param workflowName The workflow to be monitored
   * @param targetStates Specified states that user would like to stop monitoring
   * @return A TaskState, which is current workflow state
   * @throws InterruptedException
   */
  public TaskState pollForWorkflowState(String workflowName, TaskState... targetStates)
      throws InterruptedException {
    return pollForWorkflowState(workflowName, DEFAULT_TIMEOUT, targetStates);
  }

  /**
   * This call will be blocked until either specified job reaches to one of the state
   * in the arguments, or timeout happens. If timeout happens, then it will throw a HelixException
   * Otherwise, it will return current job state
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

    long timeToSleep = timeout > 50L ? 50L : timeout;

    WorkflowContext ctx;
    if (workflowConfig.isRecurring()) {
      // if it's recurring, need to reconstruct workflow and job name
      do {
        Thread.sleep(timeToSleep);
        ctx = getWorkflowContext(workflowName);
      } while ((ctx == null || ctx.getLastScheduledSingleWorkflow() == null));

      jobName = jobName.substring(workflowName.length() + 1);
      workflowName = ctx.getLastScheduledSingleWorkflow();
    }

    Set<TaskState> allowedStates = new HashSet<>(Arrays.asList(states));
    // Wait for state
    long st = System.currentTimeMillis();
    do {
      Thread.sleep(timeToSleep);
      ctx = getWorkflowContext(workflowName);
    } while ((ctx == null || ctx.getJobState(jobName) == null
        || !allowedStates.contains(ctx.getJobState(jobName)))
        && System.currentTimeMillis() < st + timeout);

    if (ctx == null || !allowedStates.contains(ctx.getJobState(jobName))) {
      WorkflowConfig wfcfg = getWorkflowConfig(workflowName);
      JobConfig jobConfig = getJobConfig(jobName);
      JobContext jbCtx = getJobContext(jobName);
      throw new HelixException(String.format(
          "Workflow \"%s\" context is null or job \"%s\" is not in states: %s; ctx is %s, jobState is %s, wf cfg %s, jobcfg %s, jbctx %s",
          workflowName, jobName, allowedStates, ctx == null ? "null" : ctx,
          ctx != null ? ctx.getJobState(jobName) : "null", wfcfg, jobConfig, jbCtx));

    }

    return ctx.getJobState(jobName);
  }

  /**
   * This is a wrapper function for monitoring job state with default timeout 2 MINUTES.
   * If timeout happens, then it will throw a HelixException, Otherwise, it will return
   * current job state
   * @param workflowName The workflow that contains the job to monitor
   * @param jobName The specified job to monitor
   * @param states Specified states that user would like to stop monitoring
   * @return A TaskState, which is current job state
   * @throws Exception
   */
  public TaskState pollForJobState(String workflowName, String jobName, TaskState... states)
      throws InterruptedException {
    return pollForJobState(workflowName, jobName, DEFAULT_TIMEOUT, states);
  }

  /**
   * This function returns the timestamp of the very last task that was scheduled. It is provided to
   * help determine
   * whether a given Workflow/Job/Task is stuck.
   * @param workflowName The name of the workflow
   * @return timestamp of the most recent job scheduled.
   *         -1L if timestamp is not set (either nothing is scheduled or no start time recorded).
   */
  public long getLastScheduledTaskTimestamp(String workflowName) {
    return getLastScheduledTaskExecutionInfo(workflowName).getStartTimeStamp();
  }

  public TaskExecutionInfo getLastScheduledTaskExecutionInfo(String workflowName) {
    long lastScheduledTaskTimestamp = TaskExecutionInfo.TIMESTAMP_NOT_SET;
    String jobName = null;
    Integer taskPartitionIndex = null;
    TaskPartitionState state = null;

    WorkflowContext workflowContext = getWorkflowContext(workflowName);
    if (workflowContext != null) {
      Map<String, TaskState> allJobStates = workflowContext.getJobStates();
      for (Map.Entry<String, TaskState> jobState : allJobStates.entrySet()) {
        if (!jobState.getValue().equals(TaskState.NOT_STARTED)) {
          JobContext jobContext = getJobContext(jobState.getKey());
          if (jobContext != null) {
            Set<Integer> allPartitions = jobContext.getPartitionSet();
            for (Integer partition : allPartitions) {
              String startTime = jobContext.getMapField(partition).get(TASK_START_TIME_KEY);
              if (startTime != null) {
                long startTimeLong = Long.parseLong(startTime);
                if (startTimeLong > lastScheduledTaskTimestamp) {
                  lastScheduledTaskTimestamp = startTimeLong;
                  jobName = jobState.getKey();
                  taskPartitionIndex = partition;
                  state = jobContext.getPartitionState(partition);
                }
              }
            }
          }
        }
      }
    }
    return new TaskExecutionInfo(jobName, taskPartitionIndex, state, lastScheduledTaskTimestamp);
  }

  /**
   * Returns the lookup of UserContentStore by key.
   * @param key key used at write time by a task implementing UserContentStore
   * @param scope scope used at write time
   * @param workflowName name of workflow. Must be supplied
   * @param jobName name of job. Optional if scope is WORKFLOW
   * @param taskName name of task. Optional if scope is WORKFLOW or JOB
   * @return null if key-value pair not found or this content store does not exist. Otherwise,
   *         return a String
   * @deprecated use the following equivalents: {@link #getWorkflowUserContentMap(String)},
   *             {@link #getJobUserContentMap(String, String)},
   * @{{@link #getTaskContentMap(String, String, String)}}
   */
  @Deprecated
  public String getUserContent(String key, UserContentStore.Scope scope, String workflowName,
      String jobName, String taskName) {
    return TaskUtil.getUserContent(_propertyStore, key, scope, workflowName, jobName, taskName);
  }

  /**
   * Return the full user content map for workflow
   * @param workflowName workflow name
   * @return user content map
   */
  public Map<String, String> getWorkflowUserContentMap(String workflowName) {
    return TaskUtil.getWorkflowJobUserContentMap(_propertyStore, workflowName);
  }

  /**
   * Return full user content map for job
   * @param workflowName workflow name
   * @param jobName Un-namespaced job name
   * @return user content map
   */
  public Map<String, String> getJobUserContentMap(String workflowName, String jobName) {
    String namespacedJobName = TaskUtil.getNamespacedJobName(workflowName, jobName);
    return TaskUtil.getWorkflowJobUserContentMap(_propertyStore, namespacedJobName);
  }

  /**
   * Return full user content map for task
   * @param workflowName workflow name
   * @param jobName Un-namespaced job name
   * @param taskPartitionId task partition id
   * @return user content map
   */
  public Map<String, String> getTaskUserContentMap(String workflowName, String jobName,
      String taskPartitionId) {
    String namespacedJobName = TaskUtil.getNamespacedJobName(workflowName, jobName);
    String namespacedTaskName = TaskUtil.getNamespacedTaskName(namespacedJobName, taskPartitionId);
    return TaskUtil.getTaskUserContentMap(_propertyStore, namespacedJobName, namespacedTaskName);
  }

  /**
   * Add or update workflow user content with the given map - new keys will be added, and old
   * keys will be updated
   * @param workflowName workflow name
   * @param contentToAddOrUpdate map containing items to add or update
   */
  public void addOrUpdateWorkflowUserContentMap(String workflowName,
      final Map<String, String> contentToAddOrUpdate) {
    TaskUtil.addOrUpdateWorkflowJobUserContentMap(_propertyStore, workflowName,
        contentToAddOrUpdate);
  }

  /**
   * Add or update job user content with the given map - new keys will be added, and old keys will
   * be updated
   * @param workflowName workflow name
   * @param jobName Un-namespaced job name
   * @param contentToAddOrUpdate map containing items to add or update
   */
  public void addOrUpdateJobUserContentMap(String workflowName, String jobName,
      final Map<String, String> contentToAddOrUpdate) {
    String namespacedJobName = TaskUtil.getNamespacedJobName(workflowName, jobName);
    TaskUtil.addOrUpdateWorkflowJobUserContentMap(_propertyStore, namespacedJobName,
        contentToAddOrUpdate);
  }

  /**
   * Add or update task user content with the given map - new keys will be added, and old keys
   * will be updated
   * @param workflowName workflow name
   * @param jobName Un-namespaced job name
   * @param taskPartitionId task partition id
   * @param contentToAddOrUpdate map containing items to add or update
   */
  public void addOrUpdateTaskUserContentMap(String workflowName, String jobName,
      String taskPartitionId, final Map<String, String> contentToAddOrUpdate) {
    String namespacedJobName = TaskUtil.getNamespacedJobName(workflowName, jobName);
    String namespacedTaskName = TaskUtil.getNamespacedTaskName(namespacedJobName, taskPartitionId);
    TaskUtil.addOrUpdateTaskUserContentMap(_propertyStore, namespacedJobName, namespacedTaskName,
        contentToAddOrUpdate);
  }

  /**
   * Throw Exception if children nodes will exceed limitation after adding newNodesCount children.
   * @param newConfigNodeCount
   */
  private void validateZKNodeLimitation(int newConfigNodeCount) {
    List<String> resourceConfigs =
        _accessor.getChildNames(_accessor.keyBuilder().resourceConfigs());
    if (resourceConfigs.size() + newConfigNodeCount > _configsLimitation) {
      throw new HelixException(
          "Cannot create more workflows or jobs because there are already too many items created in the path CONFIGS.");
    }
  }

  /**
   * Get the target task thread pool size of an instance, a value that's used to construct the task
   * thread pool and is created by users.
   * @param instanceName - name of the instance
   * @return the target task thread pool size of the instance
   */
  public int getTargetTaskThreadPoolSize(String instanceName) {
    InstanceConfig instanceConfig = getInstanceConfig(instanceName);
    return instanceConfig.getTargetTaskThreadPoolSize();
  }

  /**
   * Set the target task thread pool size of an instance. The target task thread pool size goes to
   * InstanceConfig, and is used to construct the task thread pool. The newly-set target task
   * thread pool size will take effect upon a JVM restart.
   * @param instanceName - name of the instance
   * @param targetTaskThreadPoolSize - the target task thread pool size of the instance
   */
  public void setTargetTaskThreadPoolSize(String instanceName, int targetTaskThreadPoolSize) {
    InstanceConfig instanceConfig = getInstanceConfig(instanceName);
    instanceConfig.setTargetTaskThreadPoolSize(targetTaskThreadPoolSize);
    _accessor.setProperty(_accessor.keyBuilder().instanceConfig(instanceName), instanceConfig);
  }

  private InstanceConfig getInstanceConfig(String instanceName) {
    InstanceConfig instanceConfig =
        _accessor.getProperty(_accessor.keyBuilder().instanceConfig(instanceName));
    if (instanceConfig == null) {
      throw new IllegalArgumentException(
          "Failed to find InstanceConfig with provided instance name " + instanceName + "!");
    }
    return instanceConfig;
  }

  /**
   * Get the global target task thread pool size of the cluster, a value that's used to construct
   * task thread pools for the cluster's instances and is created by users.
   * @return the global target task thread pool size of the cluster
   */
  public int getGlobalTargetTaskThreadPoolSize() {
    ClusterConfig clusterConfig = getClusterConfig();
    return clusterConfig.getGlobalTargetTaskThreadPoolSize();
  }

  /**
   * Set the global target task thread pool size of the cluster. The global target task thread pool
   * size goes to ClusterConfig, and is applied to all instances of the cluster. If an instance
   * doesn't specify its target thread pool size in InstanceConfig, then this value in ClusterConfig
   * will be used to construct its task thread pool. The newly-set target task thread pool size will
   * take effect upon a JVM restart. If none of the global and per-instance target thread pool sizes
   * are set, a default size will be used.
   * @param globalTargetTaskThreadPoolSize - the global target task thread pool size of the cluster
   */
  public void setGlobalTargetTaskThreadPoolSize(int globalTargetTaskThreadPoolSize) {
    ClusterConfig clusterConfig = getClusterConfig();
    clusterConfig.setGlobalTargetTaskThreadPoolSize(globalTargetTaskThreadPoolSize);
    _accessor.setProperty(_accessor.keyBuilder().clusterConfig(), clusterConfig);
  }

  private ClusterConfig getClusterConfig() {
    ClusterConfig clusterConfig = _accessor.getProperty(_accessor.keyBuilder().clusterConfig());
    if (clusterConfig == null) {
      throw new IllegalStateException(
          "Failed to find ClusterConfig for cluster " + _clusterName + "!");
    }
    return clusterConfig;
  }

  /**
   * Get the current target task thread pool size of an instance. This value reflects the current
   * task thread pool size that's already created on the instance, and may be different from the
   * target thread pool size.
   * @param instanceName - name of the instance
   * @return the current task thread pool size of the instance
   */
  public int getCurrentTaskThreadPoolSize(String instanceName) {
    LiveInstance liveInstance =
        _accessor.getProperty(_accessor.keyBuilder().liveInstance(instanceName));
    if (liveInstance == null) {
      throw new IllegalArgumentException(
          "Failed to find LiveInstance with provided instance name " + instanceName + "!");
    }

    return liveInstance.getCurrentTaskThreadPoolSize();
  }
}
