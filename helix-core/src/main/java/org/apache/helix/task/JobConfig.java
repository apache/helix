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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.helix.task.beans.JobBean;
import org.apache.helix.task.beans.TaskBean;
import org.apache.helix.HelixProperty;

/**
 * Provides a typed interface to job configurations.
 */
// TODO: extends JobConfig from ResourceConfig
public class JobConfig {

  /**
   * Do not use this value directly, always use the get/set methods in JobConfig and JobConfig.Builder.
   */
  protected enum JobConfigProperty {
    /**
     * The name of the workflow to which the job belongs.
     */
    WorkflowID,
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
     * The set of the target partition ids. The value must be a comma-separated list of partition ids.
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
     * The timeout for a task.
     */
    TimeoutPerPartition,
    /**
     * The maximum number of times the task rebalancer may attempt to execute a task.
     */
    MaxAttemptsPerTask,
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
    JobType
  }

  //Default property values
  public static final long DEFAULT_TIMEOUT_PER_TASK = 60 * 60 * 1000; // 1 hr.
  public static final long DEFAULT_TASK_RETRY_DELAY = -1; // no delay
  public static final int DEFAULT_MAX_ATTEMPTS_PER_TASK = 10;
  public static final int DEFAULT_NUM_CONCURRENT_TASKS_PER_INSTANCE = 1;
  public static final int DEFAULT_FAILURE_THRESHOLD = 0;
  public static final int DEFAULT_MAX_FORCED_REASSIGNMENTS_PER_TASK = 0;
  public static final boolean DEFAULT_DISABLE_EXTERNALVIEW = false;
  public static final boolean DEFAULT_IGNORE_DEPENDENT_JOB_FAILURE = false;

  private final String _workflow;
  private final String _targetResource;
  private final String _jobType;
  private final List<String> _targetPartitions;
  private final Set<String> _targetPartitionStates;
  private final String _command;
  private final Map<String, String> _jobCommandConfigMap;
  private final long _timeoutPerTask;
  private final int _numConcurrentTasksPerInstance;
  private final int _maxAttemptsPerTask;
  private final int _maxForcedReassignmentsPerTask;
  private final int _failureThreshold;
  private final long _retryDelay;
  private final boolean _disableExternalView;
  private final boolean _ignoreDependentJobFailure;
  private final Map<String, TaskConfig> _taskConfigMap;

  private JobConfig(String workflow, String targetResource, List<String> targetPartitions,
      Set<String> targetPartitionStates, String command, Map<String, String> jobCommandConfigMap,
      long timeoutPerTask, int numConcurrentTasksPerInstance, int maxAttemptsPerTask,
      int maxForcedReassignmentsPerTask, int failureThreshold, long retryDelay,
      boolean disableExternalView, boolean ignoreDependentJobFailure,
      Map<String, TaskConfig> taskConfigMap, String jobType) {
    _workflow = workflow;
    _targetResource = targetResource;
    _targetPartitions = targetPartitions;
    _targetPartitionStates = targetPartitionStates;
    _command = command;
    _jobCommandConfigMap = jobCommandConfigMap;
    _timeoutPerTask = timeoutPerTask;
    _numConcurrentTasksPerInstance = numConcurrentTasksPerInstance;
    _maxAttemptsPerTask = maxAttemptsPerTask;
    _maxForcedReassignmentsPerTask = maxForcedReassignmentsPerTask;
    _failureThreshold = failureThreshold;
    _retryDelay = retryDelay;
    _disableExternalView = disableExternalView;
    _ignoreDependentJobFailure = ignoreDependentJobFailure;
    if (taskConfigMap != null) {
      _taskConfigMap = taskConfigMap;
    } else {
      _taskConfigMap = Collections.emptyMap();
    }
    _jobType = jobType;
  }

  public String getWorkflow() {
    return _workflow == null ? Workflow.UNSPECIFIED : _workflow;
  }

  public String getTargetResource() {
    return _targetResource;
  }

  public List<String> getTargetPartitions() {
    return _targetPartitions;
  }

  public Set<String> getTargetPartitionStates() {
    return _targetPartitionStates;
  }

  public String getCommand() {
    return _command;
  }

  public Map<String, String> getJobCommandConfigMap() {
    return _jobCommandConfigMap;
  }

  public long getTimeoutPerTask() {
    return _timeoutPerTask;
  }

  public int getNumConcurrentTasksPerInstance() {
    return _numConcurrentTasksPerInstance;
  }

  public int getMaxAttemptsPerTask() {
    return _maxAttemptsPerTask;
  }

  public int getMaxForcedReassignmentsPerTask() {
    return _maxForcedReassignmentsPerTask;
  }

  public int getFailureThreshold() {
    return _failureThreshold;
  }

  public long getTaskRetryDelay() {
    return _retryDelay;
  }

  public boolean isDisableExternalView() {
    return _disableExternalView;
  }

  public boolean isIgnoreDependentJobFailure() { return _ignoreDependentJobFailure; }

  public Map<String, TaskConfig> getTaskConfigMap() {
    return _taskConfigMap;
  }

  public TaskConfig getTaskConfig(String id) {
    return _taskConfigMap.get(id);
  }

  public Map<String, String> getResourceConfigMap() {
    Map<String, String> cfgMap = new HashMap<String, String>();
    cfgMap.put(JobConfigProperty.WorkflowID.name(), _workflow);
    if (_command != null) {
      cfgMap.put(JobConfigProperty.Command.name(), _command);
    }
    if (_jobCommandConfigMap != null) {
      String serializedConfig = TaskUtil.serializeJobCommandConfigMap(_jobCommandConfigMap);
      if (serializedConfig != null) {
        cfgMap.put(JobConfigProperty.JobCommandConfig.name(), serializedConfig);
      }
    }
    if (_targetResource != null) {
      cfgMap.put(JobConfigProperty.TargetResource.name(), _targetResource);
    }
    if (_targetPartitionStates != null) {
      cfgMap.put(JobConfigProperty.TargetPartitionStates.name(),
          Joiner.on(",").join(_targetPartitionStates));
    }
    if (_targetPartitions != null) {
      cfgMap
          .put(JobConfigProperty.TargetPartitions.name(), Joiner.on(",").join(_targetPartitions));
    }
    if (_retryDelay > 0) {
      cfgMap.put(JobConfigProperty.TaskRetryDelay.name(), "" + _retryDelay);
    }
    cfgMap.put(JobConfigProperty.TimeoutPerPartition.name(), "" + _timeoutPerTask);
    cfgMap.put(JobConfigProperty.MaxAttemptsPerTask.name(), "" + _maxAttemptsPerTask);
    cfgMap.put(JobConfigProperty.MaxForcedReassignmentsPerTask.name(),
        "" + _maxForcedReassignmentsPerTask);
    cfgMap.put(JobConfigProperty.FailureThreshold.name(), "" + _failureThreshold);
    cfgMap.put(JobConfigProperty.DisableExternalView.name(),
        Boolean.toString(_disableExternalView));
    cfgMap.put(JobConfigProperty.ConcurrentTasksPerInstance.name(),
        "" + _numConcurrentTasksPerInstance);
    cfgMap.put(JobConfigProperty.IgnoreDependentJobFailure.name(),
        Boolean.toString(_ignoreDependentJobFailure));
   if (_jobType != null) {
     cfgMap.put(JobConfigProperty.JobType.name(), _jobType);
   }
    return cfgMap;
  }

  public String getJobType() {
    return _jobType;
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
    private String _targetResource;
    private String _jobType;
    private List<String> _targetPartitions;
    private Set<String> _targetPartitionStates;
    private String _command;
    private Map<String, String> _commandConfig;
    private Map<String, TaskConfig> _taskConfigMap = Maps.newHashMap();
    private long _timeoutPerTask = DEFAULT_TIMEOUT_PER_TASK;
    private int _numConcurrentTasksPerInstance = DEFAULT_NUM_CONCURRENT_TASKS_PER_INSTANCE;
    private int _maxAttemptsPerTask = DEFAULT_MAX_ATTEMPTS_PER_TASK;
    private int _maxForcedReassignmentsPerTask = DEFAULT_MAX_FORCED_REASSIGNMENTS_PER_TASK;
    private int _failureThreshold = DEFAULT_FAILURE_THRESHOLD;
    private long _retryDelay = DEFAULT_TASK_RETRY_DELAY;
    private boolean _disableExternalView = DEFAULT_DISABLE_EXTERNALVIEW;
    private boolean _ignoreDependentJobFailure = DEFAULT_IGNORE_DEPENDENT_JOB_FAILURE;

    public JobConfig build() {
      validate();

      return new JobConfig(_workflow, _targetResource, _targetPartitions, _targetPartitionStates,
          _command, _commandConfig, _timeoutPerTask, _numConcurrentTasksPerInstance,
          _maxAttemptsPerTask, _maxForcedReassignmentsPerTask, _failureThreshold, _retryDelay,
          _disableExternalView, _ignoreDependentJobFailure, _taskConfigMap, _jobType);
    }

    /**
     * Convenience method to build a {@link JobConfig} from a {@code Map&lt;String, String&gt;}.
     *
     * @param cfg A map of property names to their string representations.
     * @return A {@link Builder}.
     */
    public static Builder fromMap(Map<String, String> cfg) {
      Builder b = new Builder();
      if (cfg.containsKey(JobConfigProperty.WorkflowID.name())) {
        b.setWorkflow(cfg.get(JobConfigProperty.WorkflowID.name()));
      }
      if (cfg.containsKey(JobConfigProperty.TargetResource.name())) {
        b.setTargetResource(cfg.get(JobConfigProperty.TargetResource.name()));
      }
      if (cfg.containsKey(JobConfigProperty.TargetPartitions.name())) {
        b.setTargetPartitions(csvToStringList(cfg.get(JobConfigProperty.TargetPartitions.name())));
      }
      if (cfg.containsKey(JobConfigProperty.TargetPartitionStates.name())) {
        b.setTargetPartitionStates(new HashSet<String>(
            Arrays.asList(cfg.get(JobConfigProperty.TargetPartitionStates.name()).split(","))));
      }
      if (cfg.containsKey(JobConfigProperty.Command.name())) {
        b.setCommand(cfg.get(JobConfigProperty.Command.name()));
      }
      if (cfg.containsKey(JobConfigProperty.JobCommandConfig.name())) {
        Map<String, String> commandConfigMap = TaskUtil.deserializeJobCommandConfigMap(
            cfg.get(JobConfigProperty.JobCommandConfig.name()));
        b.setJobCommandConfigMap(commandConfigMap);
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
      if (cfg.containsKey(JobConfigProperty.MaxForcedReassignmentsPerTask.name())) {
        b.setMaxForcedReassignmentsPerTask(
            Integer.parseInt(cfg.get(JobConfigProperty.MaxForcedReassignmentsPerTask.name())));
      }
      if (cfg.containsKey(JobConfigProperty.FailureThreshold.name())) {
        b.setFailureThreshold(
            Integer.parseInt(cfg.get(JobConfigProperty.FailureThreshold.name())));
      }
      if (cfg.containsKey(JobConfigProperty.TaskRetryDelay.name())) {
        b.setTaskRetryDelay(Long.parseLong(cfg.get(JobConfigProperty.TaskRetryDelay.name())));
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
      return b;
    }

    public Builder setWorkflow(String v) {
      _workflow = v;
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

    public Builder setJobCommandConfigMap(Map<String, String> v) {
      _commandConfig = v;
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

    private void validate() {
      if (_taskConfigMap.isEmpty() && _targetResource == null) {
        throw new IllegalArgumentException(
            String.format("%s cannot be null", JobConfigProperty.TargetResource));
      }
      if (_taskConfigMap.isEmpty() && _targetPartitionStates != null && _targetPartitionStates
          .isEmpty()) {
        throw new IllegalArgumentException(
            String.format("%s cannot be an empty set", JobConfigProperty.TargetPartitionStates));
      }
      if (_taskConfigMap.isEmpty() && _command == null) {
        throw new IllegalArgumentException(
            String.format("%s cannot be null", JobConfigProperty.Command));
      }
      if (_timeoutPerTask < 0) {
        throw new IllegalArgumentException(String
            .format("%s has invalid value %s", JobConfigProperty.TimeoutPerPartition,
                _timeoutPerTask));
      }
      if (_numConcurrentTasksPerInstance < 1) {
        throw new IllegalArgumentException(String
            .format("%s has invalid value %s", JobConfigProperty.ConcurrentTasksPerInstance,
                _numConcurrentTasksPerInstance));
      }
      if (_maxAttemptsPerTask < 1) {
        throw new IllegalArgumentException(String
            .format("%s has invalid value %s", JobConfigProperty.MaxAttemptsPerTask,
                _maxAttemptsPerTask));
      }
      if (_maxForcedReassignmentsPerTask < 0) {
        throw new IllegalArgumentException(String
            .format("%s has invalid value %s", JobConfigProperty.MaxForcedReassignmentsPerTask,
                _maxForcedReassignmentsPerTask));
      }
      if (_failureThreshold < 0) {
        throw new IllegalArgumentException(String
            .format("%s has invalid value %s", JobConfigProperty.FailureThreshold,
                _failureThreshold));
      }
      if (_workflow == null) {
        throw new IllegalArgumentException(
            String.format("%s cannot be null", JobConfigProperty.WorkflowID));
      }
    }

    public static Builder from(JobBean jobBean) {
      Builder b = new Builder();

      b.setMaxAttemptsPerTask(jobBean.maxAttemptsPerTask)
          .setMaxForcedReassignmentsPerTask(jobBean.maxForcedReassignmentsPerTask)
          .setNumConcurrentTasksPerInstance(jobBean.numConcurrentTasksPerInstance)
          .setTimeoutPerTask(jobBean.timeoutPerPartition)
          .setFailureThreshold(jobBean.failureThreshold).setTaskRetryDelay(jobBean.taskRetryDelay)
          .setDisableExternalView(jobBean.disableExternalView)
          .setIgnoreDependentJobFailure(jobBean.ignoreDependentJobFailure);

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
        b.setTargetPartitionStates(new HashSet<String>(jobBean.targetPartitionStates));
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
      return b;
    }

    private static List<String> csvToStringList(String csv) {
      String[] vals = csv.split(",");
      return Arrays.asList(vals);
    }
  }
}
