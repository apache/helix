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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixProperty;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.task.beans.JobBean;
import org.apache.helix.task.beans.TaskBean;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Provides a typed interface to job configurations.
 */
public class JobConfig extends ResourceConfig {

  /**
   * Do not use this value directly, always use the get/set methods in JobConfig and
   * JobConfig.Builder.
   */
  protected enum JobConfigProperty {
    /**
     * The name of the workflow to which the job belongs.
     */
    WorkflowID,
    /**
     * The name of the job
     */
    JobID,
    /**
     * The assignment strategy of this job
     */
    AssignmentStrategy,
    /**
     * The name of the target resource.
     */
    TargetResource,
    /**
     * The set of the target partition states. The value must be a comma-separated list of partition
     * states.
     */
    TargetPartitionStates,
    /**
     * The set of the target partition ids. The value must be a comma-separated list of partition
     * ids.
     */
    TargetPartitions,
    /**
     * The command that is to be run by participants in the case of identical tasks.
     */
    Command,
    /**
     * The command configuration to be used by the tasks.
     */
    JobCommandConfig,
    /**
     * The allowed execution time of the job.
     */
    Timeout,
    /**
     * The timeout for a task.
     */
    TimeoutPerPartition,
    /**
     * The maximum number of times the task rebalancer may attempt to execute a task.
     */
    MaxAttemptsPerTask,
    @Deprecated
    /**
     * The maximum number of times Helix will intentionally move a failing task
     */
        MaxForcedReassignmentsPerTask,
    /**
     * The number of concurrent tasks that are allowed to run on an instance.
     */
    ConcurrentTasksPerInstance,
    /**
     * The number of tasks within the job that are allowed to fail.
     */
    FailureThreshold,
    /**
     * The amount of time in ms to wait before retrying a task
     */
    TaskRetryDelay,
    /**
     * Whether failure of directly dependent jobs should fail this job.
     */
    IgnoreDependentJobFailure,

    /**
     * The individual task configurations, if any *
     */
    TaskConfigs,

    /**
     * Disable external view (not showing) for this job resource
     */
    DisableExternalView,

    /**
     * The type of the job
     */
    JobType,

    /**
     * The instance group that task assign to
     */
    InstanceGroupTag,

    /**
     * The job execution delay time
     */
    DelayTime,

    /**
     * The job execution start time
     */
    StartTime,

    /**
     * The expiration time for the job
     */
    Expiry,

    /**
     * Whether or not enable running task rebalance
     */
    RebalanceRunningTask,
  }

  // Default property values
  public static final long DEFAULT_TIMEOUT_PER_TASK = 60 * 60 * 1000; // 1 hr.
  public static final long DEFAULT_TASK_RETRY_DELAY = -1; // no delay
  public static final int DEFAULT_MAX_ATTEMPTS_PER_TASK = 10;
  public static final int DEFAULT_NUM_CONCURRENT_TASKS_PER_INSTANCE = 1;
  public static final int DEFAULT_FAILURE_THRESHOLD = 0;
  public static final int DEFAULT_MAX_FORCED_REASSIGNMENTS_PER_TASK = 0;
  public static final boolean DEFAULT_DISABLE_EXTERNALVIEW = false;
  public static final boolean DEFAULT_IGNORE_DEPENDENT_JOB_FAILURE = false;
  public static final int DEFAULT_NUMBER_OF_TASKS = 0;
  public static final long DEFAULT_JOB_EXECUTION_START_TIME = -1L;
  public static final long DEFAULT_Job_EXECUTION_DELAY_TIME = -1L;
  public static final boolean DEFAULT_REBALANCE_RUNNING_TASK = false;

  // Cache TaskConfig objects for targeted jobs' tasks to reduce object creation/GC overload
  private Map<String, TaskConfig> _targetedTaskConfigMap = new HashMap<>();

  public JobConfig(HelixProperty property) {
    super(property.getRecord());
  }

  public JobConfig(String jobId, JobConfig jobConfig) {
    this(jobConfig.getWorkflow(), jobConfig.getTargetResource(), jobConfig.getTargetPartitions(),
        jobConfig.getTargetPartitionStates(), jobConfig.getCommand(),
        jobConfig.getJobCommandConfigMap(), jobConfig.getTimeout(), jobConfig.getTimeoutPerTask(),
        jobConfig.getNumConcurrentTasksPerInstance(), jobConfig.getMaxAttemptsPerTask(),
        jobConfig.getMaxAttemptsPerTask(), jobConfig.getFailureThreshold(),
        jobConfig.getTaskRetryDelay(), jobConfig.isDisableExternalView(),
        jobConfig.isIgnoreDependentJobFailure(), jobConfig.getTaskConfigMap(),
        jobConfig.getJobType(), jobConfig.getInstanceGroupTag(), jobConfig.getExecutionDelay(),
        jobConfig.getExecutionStart(), jobId, jobConfig.getExpiry(),
        jobConfig.isRebalanceRunningTask());
  }

  private JobConfig(String workflow, String targetResource, List<String> targetPartitions,
      Set<String> targetPartitionStates, String command, Map<String, String> jobCommandConfigMap,
      long timeout, long timeoutPerTask, int numConcurrentTasksPerInstance, int maxAttemptsPerTask,
      int maxForcedReassignmentsPerTask, int failureThreshold, long retryDelay,
      boolean disableExternalView, boolean ignoreDependentJobFailure,
      Map<String, TaskConfig> taskConfigMap, String jobType, String instanceGroupTag,
      long executionDelay, long executionStart, String jobId, long expiry,
      boolean rebalanceRunningTask) {
    super(jobId);
    putSimpleConfig(JobConfigProperty.WorkflowID.name(), workflow);
    putSimpleConfig(JobConfigProperty.JobID.name(), jobId);
    if (command != null) {
      putSimpleConfig(JobConfigProperty.Command.name(), command);
    }
    if (jobCommandConfigMap != null) {
      String serializedConfig = TaskUtil.serializeJobCommandConfigMap(jobCommandConfigMap);
      if (serializedConfig != null) {
        putSimpleConfig(JobConfigProperty.JobCommandConfig.name(), serializedConfig);
      }
    }
    if (targetResource != null) {
      putSimpleConfig(JobConfigProperty.TargetResource.name(), targetResource);
    }
    if (targetPartitionStates != null) {
      putSimpleConfig(JobConfigProperty.TargetPartitionStates.name(),
          Joiner.on(",").join(targetPartitionStates));
    }
    if (targetPartitions != null) {
      putSimpleConfig(JobConfigProperty.TargetPartitions.name(),
          Joiner.on(",").join(targetPartitions));
    }
    if (retryDelay > 0) {
      getRecord().setLongField(JobConfigProperty.TaskRetryDelay.name(), retryDelay);
    }
    if (executionDelay > 0) {
      getRecord().setLongField(JobConfigProperty.DelayTime.name(), executionDelay);
    }
    if (executionStart > 0) {
      getRecord().setLongField(JobConfigProperty.StartTime.name(), executionStart);
    }
    if (timeout > TaskConstants.DEFAULT_NEVER_TIMEOUT) {
      getRecord().setLongField(JobConfigProperty.Timeout.name(), timeout);
    }
    getRecord().setLongField(JobConfigProperty.TimeoutPerPartition.name(), timeoutPerTask);
    getRecord().setIntField(JobConfigProperty.MaxAttemptsPerTask.name(), maxAttemptsPerTask);
    getRecord().setIntField(JobConfigProperty.MaxForcedReassignmentsPerTask.name(),
        maxForcedReassignmentsPerTask);
    getRecord().setIntField(JobConfigProperty.FailureThreshold.name(), failureThreshold);
    getRecord().setBooleanField(JobConfigProperty.DisableExternalView.name(), disableExternalView);
    getRecord().setIntField(JobConfigProperty.ConcurrentTasksPerInstance.name(),
        numConcurrentTasksPerInstance);
    getRecord().setBooleanField(JobConfigProperty.IgnoreDependentJobFailure.name(),
        ignoreDependentJobFailure);
    if (jobType != null) {
      putSimpleConfig(JobConfigProperty.JobType.name(), jobType);
    }
    if (instanceGroupTag != null) {
      putSimpleConfig(JobConfigProperty.InstanceGroupTag.name(), instanceGroupTag);
    }
    if (taskConfigMap != null) {
      for (TaskConfig taskConfig : taskConfigMap.values()) {
        putMapConfig(taskConfig.getId(), taskConfig.getConfigMap());
      }
    }
    if (expiry > 0) {
      getRecord().setLongField(JobConfigProperty.Expiry.name(), expiry);
    }
    putSimpleConfig(ResourceConfigProperty.MONITORING_DISABLED.toString(),
        String.valueOf(WorkflowConfig.DEFAULT_MONITOR_DISABLE));
    getRecord().setBooleanField(JobConfigProperty.RebalanceRunningTask.name(),
        rebalanceRunningTask);
  }

  public String getWorkflow() {
    return simpleConfigContains(JobConfigProperty.WorkflowID.name())
        ? getSimpleConfig(JobConfigProperty.WorkflowID.name())
        : Workflow.UNSPECIFIED;
  }

  public String getJobId() {
    return getSimpleConfig(JobConfigProperty.JobID.name());
  }

  public String getTargetResource() {
    return getSimpleConfig(JobConfigProperty.TargetResource.name());
  }

  public List<String> getTargetPartitions() {
    return simpleConfigContains(JobConfigProperty.TargetPartitions.name())
        ? Arrays.asList(getSimpleConfig(JobConfigProperty.TargetPartitions.name()).split(","))
        : null;
  }

  public Set<String> getTargetPartitionStates() {
    if (simpleConfigContains(JobConfigProperty.TargetPartitionStates.name())) {
      return new HashSet<>(Arrays
          .asList(getSimpleConfig(JobConfigProperty.TargetPartitionStates.name()).split(",")));
    }
    return null;
  }

  public String getCommand() {
    return getSimpleConfig(JobConfigProperty.Command.name());
  }

  public Map<String, String> getJobCommandConfigMap() {
    return simpleConfigContains(JobConfigProperty.JobCommandConfig.name()) ? TaskUtil
        .deserializeJobCommandConfigMap(getSimpleConfig(JobConfigProperty.JobCommandConfig.name()))
        : null;
  }

  public long getTimeout() {
    return getRecord().getLongField(JobConfigProperty.Timeout.name(),
        TaskConstants.DEFAULT_NEVER_TIMEOUT);
  }

  public long getTimeoutPerTask() {
    return getRecord().getLongField(JobConfigProperty.TimeoutPerPartition.name(),
        DEFAULT_TIMEOUT_PER_TASK);
  }

  public int getNumConcurrentTasksPerInstance() {
    return getRecord().getIntField(JobConfigProperty.ConcurrentTasksPerInstance.name(),
        DEFAULT_NUM_CONCURRENT_TASKS_PER_INSTANCE);
  }

  public int getMaxAttemptsPerTask() {
    return getRecord().getIntField(JobConfigProperty.MaxAttemptsPerTask.name(),
        DEFAULT_MAX_ATTEMPTS_PER_TASK);
  }

  public int getFailureThreshold() {
    return getRecord().getIntField(JobConfigProperty.FailureThreshold.name(),
        DEFAULT_FAILURE_THRESHOLD);
  }

  public long getTaskRetryDelay() {
    return getRecord().getLongField(JobConfigProperty.TaskRetryDelay.name(),
        DEFAULT_TASK_RETRY_DELAY);
  }

  // Execution delay time will be ignored when it is negative number
  public long getExecutionDelay() {
    return getRecord().getLongField(JobConfigProperty.DelayTime.name(),
        DEFAULT_Job_EXECUTION_DELAY_TIME);
  }

  public long getExecutionStart() {
    return getRecord().getLongField(JobConfigProperty.StartTime.name(),
        DEFAULT_JOB_EXECUTION_START_TIME);
  }

  public boolean isDisableExternalView() {
    return getRecord().getBooleanField(JobConfigProperty.DisableExternalView.name(),
        DEFAULT_DISABLE_EXTERNALVIEW);
  }

  public boolean isIgnoreDependentJobFailure() {
    return getRecord().getBooleanField(JobConfigProperty.IgnoreDependentJobFailure.name(),
        DEFAULT_IGNORE_DEPENDENT_JOB_FAILURE);
  }

  /**
   * Returns taskConfigMap. If it's targeted, then return a cached targetedTaskConfigMap.
   * @return
   */
  public Map<String, TaskConfig> getTaskConfigMap() {
    String targetResource = getSimpleConfig(JobConfigProperty.TargetResource.name());
    if (targetResource != null) {
      return _targetedTaskConfigMap;
    }
    Map<String, TaskConfig> taskConfigMap = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : getMapConfigs().entrySet()) {
      taskConfigMap.put(entry.getKey(),
          new TaskConfig(null, entry.getValue(), entry.getKey(), null));
    }
    return taskConfigMap;
  }

  /**
   * If the job is targeted, try to get it from the cached targetedTaskConfigMap first. If not,
   * create a TaskConfig on the fly.
   * @param id pName for targeted tasks
   * @return a TaskConfig object
   */
  public TaskConfig getTaskConfig(String id) {
    String targetResource = getSimpleConfig(JobConfigProperty.TargetResource.name());
    if (targetResource != null) {
      // This is a targeted task. For targeted tasks, id is pName
      if (!_targetedTaskConfigMap.containsKey(id)) {
        return new TaskConfig(null, null, id, null);
      }
      return _targetedTaskConfigMap.get(id);
    }
    return new TaskConfig(null, getMapConfig(id), id, null);
  }

  /**
   * When a targeted task is assigned for the first time, cache it in JobConfig so that it could be
   * retrieved later for release.
   * @param pName a concatenation of job name + "_" + task partition number
   */
  public void setTaskConfig(String pName, TaskConfig taskConfig) {
    _targetedTaskConfigMap.put(pName, taskConfig);
  }

  public Map<String, String> getResourceConfigMap() {
    return getSimpleConfigs();
  }

  /**
   * Returns the job type for this job. This type will be used as a quota type for quota-based
   * scheduling.
   * @return quota type. null if quota type is not set
   */
  public String getJobType() {
    return getSimpleConfig(JobConfigProperty.JobType.name());
  }

  public String getInstanceGroupTag() {
    return getSimpleConfig(JobConfigProperty.InstanceGroupTag.name());
  }

  public Long getExpiry() {
    return getRecord().getLongField(JobConfigProperty.Expiry.name(), WorkflowConfig.DEFAULT_EXPIRY);
  }

  public boolean isRebalanceRunningTask() {
    return getRecord().getBooleanField(JobConfigProperty.RebalanceRunningTask.name(),
        DEFAULT_REBALANCE_RUNNING_TASK);
  }

  public static JobConfig fromHelixProperty(HelixProperty property)
      throws IllegalArgumentException {
    Map<String, String> configs = property.getRecord().getSimpleFields();
    return Builder.fromMap(configs).build();
  }

  /**
   * A builder for {@link JobConfig}. Validates the configurations.
   */
  public static class Builder {
    private String _workflow;
    private String _jobId;
    private String _targetResource;
    private String _jobType;
    private String _instanceGroupTag;
    private List<String> _targetPartitions;
    private Set<String> _targetPartitionStates;
    private String _command;
    private Map<String, String> _commandConfig;
    private Map<String, TaskConfig> _taskConfigMap = Maps.newHashMap();
    private long _timeout = TaskConstants.DEFAULT_NEVER_TIMEOUT;
    private long _timeoutPerTask = DEFAULT_TIMEOUT_PER_TASK;
    private int _numConcurrentTasksPerInstance = DEFAULT_NUM_CONCURRENT_TASKS_PER_INSTANCE;
    private int _maxAttemptsPerTask = DEFAULT_MAX_ATTEMPTS_PER_TASK;
    private int _maxForcedReassignmentsPerTask = DEFAULT_MAX_FORCED_REASSIGNMENTS_PER_TASK;
    private int _failureThreshold = DEFAULT_FAILURE_THRESHOLD;
    private long _retryDelay = DEFAULT_TASK_RETRY_DELAY;
    private long _executionStart = DEFAULT_JOB_EXECUTION_START_TIME;
    private long _executionDelay = DEFAULT_Job_EXECUTION_DELAY_TIME;
    private long _expiry = WorkflowConfig.DEFAULT_EXPIRY;
    private boolean _disableExternalView = DEFAULT_DISABLE_EXTERNALVIEW;
    private boolean _ignoreDependentJobFailure = DEFAULT_IGNORE_DEPENDENT_JOB_FAILURE;
    private int _numberOfTasks = DEFAULT_NUMBER_OF_TASKS;
    private boolean _rebalanceRunningTask = DEFAULT_REBALANCE_RUNNING_TASK;

    public JobConfig build() {
      if (_targetResource == null && _taskConfigMap.isEmpty()) {
        for (int i = 0; i < _numberOfTasks; i++) {
          TaskConfig taskConfig = new TaskConfig(null, null);
          _taskConfigMap.put(taskConfig.getId(), taskConfig);
        }
      }
      if (_jobId == null) {
        _jobId = "";
      }

      validate();

      return new JobConfig(_workflow, _targetResource, _targetPartitions, _targetPartitionStates,
          _command, _commandConfig, _timeout, _timeoutPerTask, _numConcurrentTasksPerInstance,
          _maxAttemptsPerTask, _maxForcedReassignmentsPerTask, _failureThreshold, _retryDelay,
          _disableExternalView, _ignoreDependentJobFailure, _taskConfigMap, _jobType,
          _instanceGroupTag, _executionDelay, _executionStart, _jobId, _expiry,
          _rebalanceRunningTask);
    }

    /**
     * Convenience method to build a {@link JobConfig} from a {@code Map&lt;String, String&gt;}.
     * @param cfg A map of property names to their string representations.
     * @return A {@link Builder}.
     */
    public static Builder fromMap(Map<String, String> cfg) {
      Builder b = new Builder();
      if (cfg.containsKey(JobConfigProperty.WorkflowID.name())) {
        b.setWorkflow(cfg.get(JobConfigProperty.WorkflowID.name()));
      }
      if (cfg.containsKey(JobConfigProperty.JobID.name())) {
        b.setJobId(cfg.get(JobConfigProperty.JobID.name()));
      }
      if (cfg.containsKey(JobConfigProperty.TargetResource.name())) {
        b.setTargetResource(cfg.get(JobConfigProperty.TargetResource.name()));
      }
      if (cfg.containsKey(JobConfigProperty.TargetPartitions.name())) {
        b.setTargetPartitions(csvToStringList(cfg.get(JobConfigProperty.TargetPartitions.name())));
      }
      if (cfg.containsKey(JobConfigProperty.TargetPartitionStates.name())) {
        b.setTargetPartitionStates(new HashSet<>(
            Arrays.asList(cfg.get(JobConfigProperty.TargetPartitionStates.name()).split(","))));
      }
      if (cfg.containsKey(JobConfigProperty.Command.name())) {
        b.setCommand(cfg.get(JobConfigProperty.Command.name()));
      }
      if (cfg.containsKey(JobConfigProperty.JobCommandConfig.name())) {
        Map<String, String> commandConfigMap = TaskUtil
            .deserializeJobCommandConfigMap(cfg.get(JobConfigProperty.JobCommandConfig.name()));
        b.setJobCommandConfigMap(commandConfigMap);
      }
      if (cfg.containsKey(JobConfigProperty.Timeout.name())) {
        b.setTimeout(Long.parseLong(cfg.get(JobConfigProperty.Timeout.name())));
      }
      if (cfg.containsKey(JobConfigProperty.TimeoutPerPartition.name())) {
        b.setTimeoutPerTask(Long.parseLong(cfg.get(JobConfigProperty.TimeoutPerPartition.name())));
      }
      if (cfg.containsKey(JobConfigProperty.ConcurrentTasksPerInstance.name())) {
        b.setNumConcurrentTasksPerInstance(
            Integer.parseInt(cfg.get(JobConfigProperty.ConcurrentTasksPerInstance.name())));
      }
      if (cfg.containsKey(JobConfigProperty.MaxAttemptsPerTask.name())) {
        b.setMaxAttemptsPerTask(
            Integer.parseInt(cfg.get(JobConfigProperty.MaxAttemptsPerTask.name())));
      }
      if (cfg.containsKey(JobConfigProperty.FailureThreshold.name())) {
        b.setFailureThreshold(Integer.parseInt(cfg.get(JobConfigProperty.FailureThreshold.name())));
      }
      if (cfg.containsKey(JobConfigProperty.TaskRetryDelay.name())) {
        b.setTaskRetryDelay(Long.parseLong(cfg.get(JobConfigProperty.TaskRetryDelay.name())));
      }
      if (cfg.containsKey(JobConfigProperty.DelayTime.name())) {
        b.setExecutionDelay(Long.parseLong(cfg.get(JobConfigProperty.DelayTime.name())));
      }
      if (cfg.containsKey(JobConfigProperty.StartTime.name())) {
        b.setExecutionStart(Long.parseLong(cfg.get(JobConfigProperty.StartTime.name())));
      }
      if (cfg.containsKey(JobConfigProperty.DisableExternalView.name())) {
        b.setDisableExternalView(
            Boolean.valueOf(cfg.get(JobConfigProperty.DisableExternalView.name())));
      }
      if (cfg.containsKey(JobConfigProperty.IgnoreDependentJobFailure.name())) {
        b.setIgnoreDependentJobFailure(
            Boolean.valueOf(cfg.get(JobConfigProperty.IgnoreDependentJobFailure.name())));
      }
      if (cfg.containsKey(JobConfigProperty.JobType.name())) {
        b.setJobType(cfg.get(JobConfigProperty.JobType.name()));
      }
      if (cfg.containsKey(JobConfigProperty.InstanceGroupTag.name())) {
        b.setInstanceGroupTag(cfg.get(JobConfigProperty.InstanceGroupTag.name()));
      }
      if (cfg.containsKey(JobConfigProperty.Expiry.name())) {
        b.setExpiry(Long.valueOf(cfg.get(JobConfigProperty.Expiry.name())));
      }
      if (cfg.containsKey(JobConfigProperty.RebalanceRunningTask.name())) {
        b.setRebalanceRunningTask(
            Boolean.valueOf(cfg.get(JobConfigProperty.RebalanceRunningTask.name())));
      }
      return b;
    }

    public Builder setWorkflow(String v) {
      _workflow = v;
      return this;
    }

    public Builder setJobId(String v) {
      _jobId = v;
      return this;
    }

    public Builder setTargetResource(String v) {
      _targetResource = v;
      return this;
    }

    public Builder setTargetPartitions(List<String> v) {
      _targetPartitions = ImmutableList.copyOf(v);
      return this;
    }

    public Builder setTargetPartitionStates(Set<String> v) {
      _targetPartitionStates = ImmutableSet.copyOf(v);
      return this;
    }

    public Builder setCommand(String v) {
      _command = v;
      return this;
    }

    public Builder setNumberOfTasks(int v) {
      _numberOfTasks = v;
      return this;
    }

    public Builder setJobCommandConfigMap(Map<String, String> v) {
      _commandConfig = v;
      return this;
    }

    public Builder setTimeout(long v) {
      _timeout = v;
      return this;
    }

    public Builder setTimeoutPerTask(long v) {
      _timeoutPerTask = v;
      return this;
    }

    public Builder setNumConcurrentTasksPerInstance(int v) {
      _numConcurrentTasksPerInstance = v;
      return this;
    }

    public Builder setMaxAttemptsPerTask(int v) {
      _maxAttemptsPerTask = v;
      return this;
    }

    // This field will be ignored by Helix
    @Deprecated
    public Builder setMaxForcedReassignmentsPerTask(int v) {
      _maxForcedReassignmentsPerTask = v;
      return this;
    }

    public Builder setFailureThreshold(int v) {
      _failureThreshold = v;
      return this;
    }

    public Builder setTaskRetryDelay(long v) {
      _retryDelay = v;
      return this;
    }

    public Builder setExecutionDelay(long v) {
      _executionDelay = v;
      return this;
    }

    public Builder setExecutionStart(long v) {
      _executionStart = v;
      return this;
    }

    public Builder setDisableExternalView(boolean disableExternalView) {
      _disableExternalView = disableExternalView;
      return this;
    }

    public Builder setIgnoreDependentJobFailure(boolean ignoreDependentJobFailure) {
      _ignoreDependentJobFailure = ignoreDependentJobFailure;
      return this;
    }

    public Builder addTaskConfigs(List<TaskConfig> taskConfigs) {
      if (taskConfigs != null) {
        for (TaskConfig taskConfig : taskConfigs) {
          _taskConfigMap.put(taskConfig.getId(), taskConfig);
        }
      }
      return this;
    }

    public Builder addTaskConfigMap(Map<String, TaskConfig> taskConfigMap) {
      _taskConfigMap.putAll(taskConfigMap);
      return this;
    }

    public Builder setJobType(String jobType) {
      _jobType = jobType;
      return this;
    }

    public Builder setInstanceGroupTag(String instanceGroupTag) {
      _instanceGroupTag = instanceGroupTag;
      return this;
    }

    public Builder setExpiry(Long expiry) {
      _expiry = expiry;
      return this;
    }

    public Builder setRebalanceRunningTask(boolean enabled) {
      _rebalanceRunningTask = enabled;
      return this;
    }

    private void validate() {
      if (_taskConfigMap.isEmpty() && _targetResource == null) {
        throw new IllegalArgumentException(
            String.format("%s cannot be null", JobConfigProperty.TargetResource));
      }
      if (_taskConfigMap.isEmpty() && _targetPartitionStates != null
          && _targetPartitionStates.isEmpty()) {
        throw new IllegalArgumentException(
            String.format("%s cannot be an empty set", JobConfigProperty.TargetPartitionStates));
      }
      if (_taskConfigMap.isEmpty()) {
        // Check Job command is not null when none taskconfig specified
        if (_command == null) {
          throw new IllegalArgumentException(
              String.format("%s cannot be null", JobConfigProperty.Command));
        }
        // Check number of task is set when Job command is not null and none taskconfig specified
        if (_targetResource == null && _numberOfTasks == 0) {
          throw new IllegalArgumentException("Either targetResource or numberOfTask should be set");
        }
      }
      // Check each either Job command is not null or none of task command is not null
      if (_command == null) {
        for (TaskConfig taskConfig : _taskConfigMap.values()) {
          if (taskConfig.getCommand() == null) {
            throw new IllegalArgumentException(
                String.format("Task %s command cannot be null", taskConfig.getId()));
          }
        }
      }
      if (_timeout < TaskConstants.DEFAULT_NEVER_TIMEOUT) {
        throw new IllegalArgumentException(
            String.format("%s has invalid value %s", JobConfigProperty.Timeout, _timeout));
      }
      if (_timeoutPerTask < 0) {
        throw new IllegalArgumentException(String.format("%s has invalid value %s",
            JobConfigProperty.TimeoutPerPartition, _timeoutPerTask));
      }
      if (_numConcurrentTasksPerInstance < 1) {
        throw new IllegalArgumentException(String.format("%s has invalid value %s",
            JobConfigProperty.ConcurrentTasksPerInstance, _numConcurrentTasksPerInstance));
      }
      if (_maxAttemptsPerTask < 1) {
        throw new IllegalArgumentException(String.format("%s has invalid value %s",
            JobConfigProperty.MaxAttemptsPerTask, _maxAttemptsPerTask));
      }
      if (_maxForcedReassignmentsPerTask < 0) {
        throw new IllegalArgumentException(String.format("%s has invalid value %s",
            JobConfigProperty.MaxForcedReassignmentsPerTask, _maxForcedReassignmentsPerTask));
      }
      if (_failureThreshold < 0) {
        throw new IllegalArgumentException(String.format("%s has invalid value %s",
            JobConfigProperty.FailureThreshold, _failureThreshold));
      }
      if (_workflow == null) {
        throw new IllegalArgumentException(
            String.format("%s cannot be null", JobConfigProperty.WorkflowID));
      }
    }

    public static Builder from(JobBean jobBean) {
      Builder b = new Builder();

      b.setMaxAttemptsPerTask(jobBean.maxAttemptsPerTask)
          .setNumConcurrentTasksPerInstance(jobBean.numConcurrentTasksPerInstance)
          .setTimeout(jobBean.timeout).setTimeoutPerTask(jobBean.timeoutPerPartition)
          .setFailureThreshold(jobBean.failureThreshold).setTaskRetryDelay(jobBean.taskRetryDelay)
          .setDisableExternalView(jobBean.disableExternalView)
          .setIgnoreDependentJobFailure(jobBean.ignoreDependentJobFailure)
          .setNumberOfTasks(jobBean.numberOfTasks).setExecutionDelay(jobBean.executionDelay)
          .setExecutionStart(jobBean.executionStart)
          .setRebalanceRunningTask(jobBean.rebalanceRunningTask);

      if (jobBean.jobCommandConfigMap != null) {
        b.setJobCommandConfigMap(jobBean.jobCommandConfigMap);
      }
      if (jobBean.command != null) {
        b.setCommand(jobBean.command);
      }
      if (jobBean.targetResource != null) {
        b.setTargetResource(jobBean.targetResource);
      }
      if (jobBean.targetPartitionStates != null) {
        b.setTargetPartitionStates(new HashSet<>(jobBean.targetPartitionStates));
      }
      if (jobBean.targetPartitions != null) {
        b.setTargetPartitions(jobBean.targetPartitions);
      }
      if (jobBean.tasks != null) {
        List<TaskConfig> taskConfigs = Lists.newArrayList();
        for (TaskBean task : jobBean.tasks) {
          taskConfigs.add(TaskConfig.Builder.from(task));
        }
        b.addTaskConfigs(taskConfigs);
      }
      if (jobBean.jobType != null) {
        b.setJobType(jobBean.jobType);
      }
      if (jobBean.instanceGroupTag != null) {
        b.setInstanceGroupTag(jobBean.instanceGroupTag);
      }
      return b;
    }

    private static List<String> csvToStringList(String csv) {
      String[] vals = csv.split(",");
      return Arrays.asList(vals);
    }
  }
}