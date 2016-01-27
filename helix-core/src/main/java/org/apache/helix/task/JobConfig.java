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
import com.google.common.collect.Maps;

/**
 * Provides a typed interface to job configurations.
 */
public class JobConfig {

  /**
   * Do not use this value directly, always use the get/set methods in JobConfig and JobConfig.Builder.
   */
  protected enum JobConfigProperty {
    /**
     * The name of the workflow to which the job belongs.
     */
    WORKFLOW_ID("WorkflowID"),
    /**
     * The assignment strategy of this job
     */
    ASSIGNMENT_STRATEGY("AssignmentStrategy"),
    /**
     * The name of the target resource.
     */
    TARGET_RESOURCE("TargetResource"),
    /**
     * The set of the target partition states. The value must be a comma-separated list of partition
     * states.
     */
    TARGET_PARTITION_STATES("TargetPartitionStates"),
    /**
     * The set of the target partition ids. The value must be a comma-separated list of partition ids.
     */
    TARGET_PARTITIONS("TargetPartitions"),
    /**
     * The command that is to be run by participants in the case of identical tasks.
     */
    COMMAND("Command"),
    /**
     * The command configuration to be used by the tasks.
     */
    JOB_COMMAND_CONFIG_MAP("JobCommandConfig"),
    /**
     * The timeout for a task.
     */
    TIMEOUT_PER_TASK("TimeoutPerPartition"),
    /**
     * The maximum number of times the task rebalancer may attempt to execute a task.
     */
    MAX_ATTEMPTS_PER_TASK("MaxAttemptsPerTask"),
    /**
     * The maximum number of times Helix will intentionally move a failing task
     */
    MAX_FORCED_REASSIGNMENTS_PER_TASK("MaxForcedReassignmentsPerTask"),
    /**
     * The number of concurrent tasks that are allowed to run on an instance.
     */
    NUM_CONCURRENT_TASKS_PER_INSTANCE("ConcurrentTasksPerInstance"),
    /**
     * The number of tasks within the job that are allowed to fail.
     */
    FAILURE_THRESHOLD("FailureThreshold"),
    /**
     * The amount of time in ms to wait before retrying a task
     */
    TASK_RETRY_DELAY("TaskRetryDelay"),

    /**
     * Whether failure of directly dependent jobs should fail this job.
     */
    IGNORE_DEPENDENT_JOB_FAILURE("IgnoreDependentJobFailure"),

    /**
     * The individual task configurations, if any *
     */
    TASK_CONFIGS("TaskConfigs"),

    /**
     * Disable external view (not showing) for this job resource
     */
    DISABLE_EXTERNALVIEW("DisableExternalView");

    private final String _value;

    private JobConfigProperty(String val) {
      _value = val;
    }

    public String value() {
      return _value;
    }
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
      boolean disableExternalView, boolean ignoreDependentJobFailure, Map<String, TaskConfig> taskConfigMap) {
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
    cfgMap.put(JobConfigProperty.WORKFLOW_ID.value(), _workflow);
    if (_command != null) {
      cfgMap.put(JobConfigProperty.COMMAND.value(), _command);
    }
    if (_jobCommandConfigMap != null) {
      String serializedConfig = TaskUtil.serializeJobCommandConfigMap(_jobCommandConfigMap);
      if (serializedConfig != null) {
        cfgMap.put(JobConfigProperty.JOB_COMMAND_CONFIG_MAP.value(), serializedConfig);
      }
    }
    if (_targetResource != null) {
      cfgMap.put(JobConfigProperty.TARGET_RESOURCE.value(), _targetResource);
    }
    if (_targetPartitionStates != null) {
      cfgMap.put(JobConfigProperty.TARGET_PARTITION_STATES.value(),
          Joiner.on(",").join(_targetPartitionStates));
    }
    if (_targetPartitions != null) {
      cfgMap
          .put(JobConfigProperty.TARGET_PARTITIONS.value(), Joiner.on(",").join(_targetPartitions));
    }
    if (_retryDelay > 0) {
      cfgMap.put(JobConfigProperty.TASK_RETRY_DELAY.value(), "" + _retryDelay);
    }
    cfgMap.put(JobConfigProperty.TIMEOUT_PER_TASK.value(), "" + _timeoutPerTask);
    cfgMap.put(JobConfigProperty.MAX_ATTEMPTS_PER_TASK.value(), "" + _maxAttemptsPerTask);
    cfgMap.put(JobConfigProperty.MAX_FORCED_REASSIGNMENTS_PER_TASK.value(),
        "" + _maxForcedReassignmentsPerTask);
    cfgMap.put(JobConfigProperty.FAILURE_THRESHOLD.value(), "" + _failureThreshold);
    cfgMap.put(JobConfigProperty.DISABLE_EXTERNALVIEW.value(),
        Boolean.toString(_disableExternalView));
    cfgMap.put(JobConfigProperty.NUM_CONCURRENT_TASKS_PER_INSTANCE.value(),
        "" + _numConcurrentTasksPerInstance);
    cfgMap.put(JobConfigProperty.IGNORE_DEPENDENT_JOB_FAILURE.value(),
        Boolean.toString(_ignoreDependentJobFailure));
    return cfgMap;
  }

  /**
   * A builder for {@link JobConfig}. Validates the configurations.
   */
  public static class Builder {
    private String _workflow;
    private String _targetResource;
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
          _disableExternalView, _ignoreDependentJobFailure, _taskConfigMap);
    }

    /**
     * Convenience method to build a {@link JobConfig} from a {@code Map&lt;String, String&gt;}.
     *
     * @param cfg A map of property names to their string representations.
     * @return A {@link Builder}.
     */
    public static Builder fromMap(Map<String, String> cfg) {
      Builder b = new Builder();
      if (cfg.containsKey(JobConfigProperty.WORKFLOW_ID.value())) {
        b.setWorkflow(cfg.get(JobConfigProperty.WORKFLOW_ID.value()));
      }
      if (cfg.containsKey(JobConfigProperty.TARGET_RESOURCE.value())) {
        b.setTargetResource(cfg.get(JobConfigProperty.TARGET_RESOURCE.value()));
      }
      if (cfg.containsKey(JobConfigProperty.TARGET_PARTITIONS.value())) {
        b.setTargetPartitions(csvToStringList(cfg.get(JobConfigProperty.TARGET_PARTITIONS.value())));
      }
      if (cfg.containsKey(JobConfigProperty.TARGET_PARTITION_STATES.value())) {
        b.setTargetPartitionStates(new HashSet<String>(
            Arrays.asList(cfg.get(JobConfigProperty.TARGET_PARTITION_STATES.value()).split(","))));
      }
      if (cfg.containsKey(JobConfigProperty.COMMAND.value())) {
        b.setCommand(cfg.get(JobConfigProperty.COMMAND.value()));
      }
      if (cfg.containsKey(JobConfigProperty.JOB_COMMAND_CONFIG_MAP.value())) {
        Map<String, String> commandConfigMap = TaskUtil.deserializeJobCommandConfigMap(
            cfg.get(JobConfigProperty.JOB_COMMAND_CONFIG_MAP.value()));
        b.setJobCommandConfigMap(commandConfigMap);
      }
      if (cfg.containsKey(JobConfigProperty.TIMEOUT_PER_TASK.value())) {
        b.setTimeoutPerTask(Long.parseLong(cfg.get(JobConfigProperty.TIMEOUT_PER_TASK.value())));
      }
      if (cfg.containsKey(JobConfigProperty.NUM_CONCURRENT_TASKS_PER_INSTANCE.value())) {
        b.setNumConcurrentTasksPerInstance(
            Integer.parseInt(cfg.get(JobConfigProperty.NUM_CONCURRENT_TASKS_PER_INSTANCE.value())));
      }
      if (cfg.containsKey(JobConfigProperty.MAX_ATTEMPTS_PER_TASK.value())) {
        b.setMaxAttemptsPerTask(
            Integer.parseInt(cfg.get(JobConfigProperty.MAX_ATTEMPTS_PER_TASK.value())));
      }
      if (cfg.containsKey(JobConfigProperty.MAX_FORCED_REASSIGNMENTS_PER_TASK.value())) {
        b.setMaxForcedReassignmentsPerTask(
            Integer.parseInt(cfg.get(JobConfigProperty.MAX_FORCED_REASSIGNMENTS_PER_TASK.value())));
      }
      if (cfg.containsKey(JobConfigProperty.FAILURE_THRESHOLD.value())) {
        b.setFailureThreshold(
            Integer.parseInt(cfg.get(JobConfigProperty.FAILURE_THRESHOLD.value())));
      }
      if (cfg.containsKey(JobConfigProperty.TASK_RETRY_DELAY.value())) {
        b.setTaskRetryDelay(Long.parseLong(cfg.get(JobConfigProperty.TASK_RETRY_DELAY.value())));
      }
      if (cfg.containsKey(JobConfigProperty.DISABLE_EXTERNALVIEW.value())) {
        b.setDisableExternalView(
            Boolean.valueOf(cfg.get(JobConfigProperty.DISABLE_EXTERNALVIEW.value())));
      }
      if (cfg.containsKey(JobConfigProperty.IGNORE_DEPENDENT_JOB_FAILURE.value())) {
        b.setIgnoreDependentJobFailure(
            Boolean.valueOf(cfg.get(JobConfigProperty.IGNORE_DEPENDENT_JOB_FAILURE.value())));
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

    private void validate() {
      if (_taskConfigMap.isEmpty() && _targetResource == null) {
        throw new IllegalArgumentException(
            String.format("%s cannot be null", JobConfigProperty.TARGET_RESOURCE));
      }
      if (_taskConfigMap.isEmpty() && _targetPartitionStates != null && _targetPartitionStates
          .isEmpty()) {
        throw new IllegalArgumentException(
            String.format("%s cannot be an empty set", JobConfigProperty.TARGET_PARTITION_STATES));
      }
      if (_taskConfigMap.isEmpty() && _command == null) {
        throw new IllegalArgumentException(
            String.format("%s cannot be null", JobConfigProperty.COMMAND));
      }
      if (_timeoutPerTask < 0) {
        throw new IllegalArgumentException(String
            .format("%s has invalid value %s", JobConfigProperty.TIMEOUT_PER_TASK,
                _timeoutPerTask));
      }
      if (_numConcurrentTasksPerInstance < 1) {
        throw new IllegalArgumentException(String
            .format("%s has invalid value %s", JobConfigProperty.NUM_CONCURRENT_TASKS_PER_INSTANCE,
                _numConcurrentTasksPerInstance));
      }
      if (_maxAttemptsPerTask < 1) {
        throw new IllegalArgumentException(String
            .format("%s has invalid value %s", JobConfigProperty.MAX_ATTEMPTS_PER_TASK,
                _maxAttemptsPerTask));
      }
      if (_maxForcedReassignmentsPerTask < 0) {
        throw new IllegalArgumentException(String
            .format("%s has invalid value %s", JobConfigProperty.MAX_FORCED_REASSIGNMENTS_PER_TASK,
                _maxForcedReassignmentsPerTask));
      }
      if (_failureThreshold < 0) {
        throw new IllegalArgumentException(String
            .format("%s has invalid value %s", JobConfigProperty.FAILURE_THRESHOLD,
                _failureThreshold));
      }
      if (_workflow == null) {
        throw new IllegalArgumentException(
            String.format("%s cannot be null", JobConfigProperty.WORKFLOW_ID));
      }
    }

    private static List<String> csvToStringList(String csv) {
      String[] vals = csv.split(",");
      return Arrays.asList(vals);
    }
  }
}
