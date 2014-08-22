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
  // // Property names ////

  /** The name of the workflow to which the job belongs. */
  public static final String WORKFLOW_ID = "WorkflowID";
  /** The assignment strategy of this job */
  public static final String ASSIGNMENT_STRATEGY = "AssignmentStrategy";
  /** The name of the target resource. */
  public static final String TARGET_RESOURCE = "TargetResource";
  /**
   * The set of the target partition states. The value must be a comma-separated list of partition
   * states.
   */
  public static final String TARGET_PARTITION_STATES = "TargetPartitionStates";
  /**
   * The set of the target partition ids. The value must be a comma-separated list of partition ids.
   */
  public static final String TARGET_PARTITIONS = "TargetPartitions";
  /** The command that is to be run by participants in the case of identical tasks. */
  public static final String COMMAND = "Command";
  /** The command configuration to be used by the tasks. */
  public static final String JOB_COMMAND_CONFIG_MAP = "JobCommandConfig";
  /** The timeout for a task. */
  public static final String TIMEOUT_PER_TASK = "TimeoutPerPartition";
  /** The maximum number of times the task rebalancer may attempt to execute a task. */
  public static final String MAX_ATTEMPTS_PER_TASK = "MaxAttemptsPerTask";
  /** The maximum number of times Helix will intentionally move a failing task */
  public static final String MAX_FORCED_REASSIGNMENTS_PER_TASK = "MaxForcedReassignmentsPerTask";
  /** The number of concurrent tasks that are allowed to run on an instance. */
  public static final String NUM_CONCURRENT_TASKS_PER_INSTANCE = "ConcurrentTasksPerInstance";
  /** The number of tasks within the job that are allowed to fail. */
  public static final String FAILURE_THRESHOLD = "FailureThreshold";
  /** The amount of time in ms to wait before retrying a task */
  public static final String TASK_RETRY_DELAY = "TaskRetryDelay";

  /** The individual task configurations, if any **/
  public static final String TASK_CONFIGS = "TaskConfigs";

  // // Default property values ////

  public static final long DEFAULT_TIMEOUT_PER_TASK = 60 * 60 * 1000; // 1 hr.
  public static final long DEFAULT_TASK_RETRY_DELAY = -1; // no delay
  public static final int DEFAULT_MAX_ATTEMPTS_PER_TASK = 10;
  public static final int DEFAULT_NUM_CONCURRENT_TASKS_PER_INSTANCE = 1;
  public static final int DEFAULT_FAILURE_THRESHOLD = 0;
  public static final int DEFAULT_MAX_FORCED_REASSIGNMENTS_PER_TASK = 0;

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
  private final Map<String, TaskConfig> _taskConfigMap;

  private JobConfig(String workflow, String targetResource, List<String> targetPartitions,
      Set<String> targetPartitionStates, String command, Map<String, String> jobCommandConfigMap,
      long timeoutPerTask, int numConcurrentTasksPerInstance, int maxAttemptsPerTask,
      int maxForcedReassignmentsPerTask, int failureThreshold, long retryDelay,
      Map<String, TaskConfig> taskConfigMap) {
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

  public Map<String, TaskConfig> getTaskConfigMap() {
    return _taskConfigMap;
  }

  public TaskConfig getTaskConfig(String id) {
    return _taskConfigMap.get(id);
  }

  public Map<String, String> getResourceConfigMap() {
    Map<String, String> cfgMap = new HashMap<String, String>();
    cfgMap.put(JobConfig.WORKFLOW_ID, _workflow);
    if (_command != null) {
      cfgMap.put(JobConfig.COMMAND, _command);
    }
    if (_jobCommandConfigMap != null) {
      String serializedConfig = TaskUtil.serializeJobCommandConfigMap(_jobCommandConfigMap);
      if (serializedConfig != null) {
        cfgMap.put(JobConfig.JOB_COMMAND_CONFIG_MAP, serializedConfig);
      }
    }
    if (_targetResource != null) {
      cfgMap.put(JobConfig.TARGET_RESOURCE, _targetResource);
    }
    if (_targetPartitionStates != null) {
      cfgMap.put(JobConfig.TARGET_PARTITION_STATES, Joiner.on(",").join(_targetPartitionStates));
    }
    if (_targetPartitions != null) {
      cfgMap.put(JobConfig.TARGET_PARTITIONS, Joiner.on(",").join(_targetPartitions));
    }
    if (_retryDelay > 0) {
      cfgMap.put(JobConfig.TASK_RETRY_DELAY, "" + _retryDelay);
    }
    cfgMap.put(JobConfig.TIMEOUT_PER_TASK, "" + _timeoutPerTask);
    cfgMap.put(JobConfig.MAX_ATTEMPTS_PER_TASK, "" + _maxAttemptsPerTask);
    cfgMap.put(JobConfig.MAX_FORCED_REASSIGNMENTS_PER_TASK, "" + _maxForcedReassignmentsPerTask);
    cfgMap.put(JobConfig.FAILURE_THRESHOLD, "" + _failureThreshold);
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

    public JobConfig build() {
      validate();

      return new JobConfig(_workflow, _targetResource, _targetPartitions, _targetPartitionStates,
          _command, _commandConfig, _timeoutPerTask, _numConcurrentTasksPerInstance,
          _maxAttemptsPerTask, _maxForcedReassignmentsPerTask, _failureThreshold, _retryDelay,
          _taskConfigMap);
    }

    /**
     * Convenience method to build a {@link JobConfig} from a {@code Map&lt;String, String&gt;}.
     * @param cfg A map of property names to their string representations.
     * @return A {@link Builder}.
     */
    public static Builder fromMap(Map<String, String> cfg) {
      Builder b = new Builder();
      if (cfg.containsKey(WORKFLOW_ID)) {
        b.setWorkflow(cfg.get(WORKFLOW_ID));
      }
      if (cfg.containsKey(TARGET_RESOURCE)) {
        b.setTargetResource(cfg.get(TARGET_RESOURCE));
      }
      if (cfg.containsKey(TARGET_PARTITIONS)) {
        b.setTargetPartitions(csvToStringList(cfg.get(TARGET_PARTITIONS)));
      }
      if (cfg.containsKey(TARGET_PARTITION_STATES)) {
        b.setTargetPartitionStates(new HashSet<String>(Arrays.asList(cfg.get(
            TARGET_PARTITION_STATES).split(","))));
      }
      if (cfg.containsKey(COMMAND)) {
        b.setCommand(cfg.get(COMMAND));
      }
      if (cfg.containsKey(JOB_COMMAND_CONFIG_MAP)) {
        Map<String, String> commandConfigMap =
            TaskUtil.deserializeJobCommandConfigMap(cfg.get(JOB_COMMAND_CONFIG_MAP));
        b.setJobCommandConfigMap(commandConfigMap);
      }
      if (cfg.containsKey(TIMEOUT_PER_TASK)) {
        b.setTimeoutPerTask(Long.parseLong(cfg.get(TIMEOUT_PER_TASK)));
      }
      if (cfg.containsKey(NUM_CONCURRENT_TASKS_PER_INSTANCE)) {
        b.setNumConcurrentTasksPerInstance(Integer.parseInt(cfg
            .get(NUM_CONCURRENT_TASKS_PER_INSTANCE)));
      }
      if (cfg.containsKey(MAX_ATTEMPTS_PER_TASK)) {
        b.setMaxAttemptsPerTask(Integer.parseInt(cfg.get(MAX_ATTEMPTS_PER_TASK)));
      }
      if (cfg.containsKey(MAX_FORCED_REASSIGNMENTS_PER_TASK)) {
        b.setMaxForcedReassignmentsPerTask(Integer.parseInt(cfg
            .get(MAX_FORCED_REASSIGNMENTS_PER_TASK)));
      }
      if (cfg.containsKey(FAILURE_THRESHOLD)) {
        b.setFailureThreshold(Integer.parseInt(cfg.get(FAILURE_THRESHOLD)));
      }
      if (cfg.containsKey(TASK_RETRY_DELAY)) {
        b.setTaskRetryDelay(Long.parseLong(cfg.get(TASK_RETRY_DELAY)));
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
        throw new IllegalArgumentException(String.format("%s cannot be null", TARGET_RESOURCE));
      }
      if (_taskConfigMap.isEmpty() && _targetPartitionStates != null
          && _targetPartitionStates.isEmpty()) {
        throw new IllegalArgumentException(String.format("%s cannot be an empty set",
            TARGET_PARTITION_STATES));
      }
      if (_taskConfigMap.isEmpty() && _command == null) {
        throw new IllegalArgumentException(String.format("%s cannot be null", COMMAND));
      }
      if (_timeoutPerTask < 0) {
        throw new IllegalArgumentException(String.format("%s has invalid value %s",
            TIMEOUT_PER_TASK, _timeoutPerTask));
      }
      if (_numConcurrentTasksPerInstance < 1) {
        throw new IllegalArgumentException(String.format("%s has invalid value %s",
            NUM_CONCURRENT_TASKS_PER_INSTANCE, _numConcurrentTasksPerInstance));
      }
      if (_maxAttemptsPerTask < 1) {
        throw new IllegalArgumentException(String.format("%s has invalid value %s",
            MAX_ATTEMPTS_PER_TASK, _maxAttemptsPerTask));
      }
      if (_maxForcedReassignmentsPerTask < 0) {
        throw new IllegalArgumentException(String.format("%s has invalid value %s",
            MAX_FORCED_REASSIGNMENTS_PER_TASK, _maxForcedReassignmentsPerTask));
      }
      if (_failureThreshold < 0) {
        throw new IllegalArgumentException(String.format("%s has invalid value %s",
            FAILURE_THRESHOLD, _failureThreshold));
      }
      if (_workflow == null) {
        throw new IllegalArgumentException(String.format("%s cannot be null", WORKFLOW_ID));
      }
    }

    private static List<String> csvToStringList(String csv) {
      String[] vals = csv.split(",");
      return Arrays.asList(vals);
    }
  }
}
