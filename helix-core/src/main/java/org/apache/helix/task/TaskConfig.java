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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.task.Workflow.WorkflowEnum;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Provides a typed interface to task configurations.
 */
public class TaskConfig {
  // // Property names ////

  /** The name of the workflow to which the task belongs. */
  public static final String WORKFLOW_ID = "WorkflowID";
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
  /** The command that is to be run by participants. */
  public static final String COMMAND = "Command";
  /** The command configuration to be used by the task partitions. */
  public static final String COMMAND_CONFIG = "CommandConfig";
  /** The timeout for a task partition. */
  public static final String TIMEOUT_PER_PARTITION = "TimeoutPerPartition";
  /** The maximum number of times the task rebalancer may attempt to execute a task partitions. */
  public static final String MAX_ATTEMPTS_PER_PARTITION = "MaxAttemptsPerPartition";
  /** The number of concurrent tasks that are allowed to run on an instance. */
  public static final String NUM_CONCURRENT_TASKS_PER_INSTANCE = "ConcurrentTasksPerInstance";

  // // Default property values ////

  public static final long DEFAULT_TIMEOUT_PER_PARTITION = 60 * 60 * 1000; // 1 hr.
  public static final int DEFAULT_MAX_ATTEMPTS_PER_PARTITION = 10;
  public static final int DEFAULT_NUM_CONCURRENT_TASKS_PER_INSTANCE = 1;

  private final String _workflow;
  private final String _targetResource;
  private final List<Integer> _targetPartitions;
  private final Set<String> _targetPartitionStates;
  private final String _command;
  private final String _commandConfig;
  private final long _timeoutPerPartition;
  private final int _numConcurrentTasksPerInstance;
  private final int _maxAttemptsPerPartition;

  private TaskConfig(String workflow, String targetResource, List<Integer> targetPartitions,
      Set<String> targetPartitionStates, String command, String commandConfig,
      long timeoutPerPartition, int numConcurrentTasksPerInstance, int maxAttemptsPerPartition) {
    _workflow = workflow;
    _targetResource = targetResource;
    _targetPartitions = targetPartitions;
    _targetPartitionStates = targetPartitionStates;
    _command = command;
    _commandConfig = commandConfig;
    _timeoutPerPartition = timeoutPerPartition;
    _numConcurrentTasksPerInstance = numConcurrentTasksPerInstance;
    _maxAttemptsPerPartition = maxAttemptsPerPartition;
  }

  public String getWorkflow() {
    return _workflow == null ? WorkflowEnum.UNSPECIFIED.name() : _workflow;
  }

  public String getTargetResource() {
    return _targetResource;
  }

  public List<Integer> getTargetPartitions() {
    return _targetPartitions;
  }

  public Set<String> getTargetPartitionStates() {
    return _targetPartitionStates;
  }

  public String getCommand() {
    return _command;
  }

  public String getCommandConfig() {
    return _commandConfig;
  }

  public long getTimeoutPerPartition() {
    return _timeoutPerPartition;
  }

  public int getNumConcurrentTasksPerInstance() {
    return _numConcurrentTasksPerInstance;
  }

  public int getMaxAttemptsPerPartition() {
    return _maxAttemptsPerPartition;
  }

  public Map<String, String> getResourceConfigMap() {
    Map<String, String> cfgMap = new HashMap<String, String>();
    cfgMap.put(TaskConfig.WORKFLOW_ID, _workflow);
    cfgMap.put(TaskConfig.COMMAND, _command);
    cfgMap.put(TaskConfig.COMMAND_CONFIG, _commandConfig);
    cfgMap.put(TaskConfig.TARGET_RESOURCE, _targetResource);
    if (_targetPartitionStates != null) {
      cfgMap.put(TaskConfig.TARGET_PARTITION_STATES, Joiner.on(",").join(_targetPartitionStates));
    }
    if (_targetPartitions != null) {
      cfgMap.put(TaskConfig.TARGET_PARTITIONS, Joiner.on(",").join(_targetPartitions));
    }
    cfgMap.put(TaskConfig.TIMEOUT_PER_PARTITION, "" + _timeoutPerPartition);
    cfgMap.put(TaskConfig.MAX_ATTEMPTS_PER_PARTITION, "" + _maxAttemptsPerPartition);

    return cfgMap;
  }

  /**
   * A builder for {@link TaskConfig}. Validates the configurations.
   */
  public static class Builder {
    private String _workflow;
    private String _targetResource;
    private List<Integer> _targetPartitions;
    private Set<String> _targetPartitionStates;
    private String _command;
    private String _commandConfig;
    private long _timeoutPerPartition = DEFAULT_TIMEOUT_PER_PARTITION;
    private int _numConcurrentTasksPerInstance = DEFAULT_NUM_CONCURRENT_TASKS_PER_INSTANCE;
    private int _maxAttemptsPerPartition = DEFAULT_MAX_ATTEMPTS_PER_PARTITION;

    public TaskConfig build() {
      validate();

      return new TaskConfig(_workflow, _targetResource, _targetPartitions, _targetPartitionStates,
          _command, _commandConfig, _timeoutPerPartition, _numConcurrentTasksPerInstance,
          _maxAttemptsPerPartition);
    }

    /**
     * Convenience method to build a {@link TaskConfig} from a {@code Map&lt;String, String&gt;}.
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
        b.setTargetPartitions(csvToIntList(cfg.get(TARGET_PARTITIONS)));
      }
      if (cfg.containsKey(TARGET_PARTITION_STATES)) {
        b.setTargetPartitionStates(new HashSet<String>(Arrays.asList(cfg.get(
            TARGET_PARTITION_STATES).split(","))));
      }
      if (cfg.containsKey(COMMAND)) {
        b.setCommand(cfg.get(COMMAND));
      }
      if (cfg.containsKey(COMMAND_CONFIG)) {
        b.setCommandConfig(cfg.get(COMMAND_CONFIG));
      }
      if (cfg.containsKey(TIMEOUT_PER_PARTITION)) {
        b.setTimeoutPerPartition(Long.parseLong(cfg.get(TIMEOUT_PER_PARTITION)));
      }
      if (cfg.containsKey(NUM_CONCURRENT_TASKS_PER_INSTANCE)) {
        b.setNumConcurrentTasksPerInstance(Integer.parseInt(cfg
            .get(NUM_CONCURRENT_TASKS_PER_INSTANCE)));
      }
      if (cfg.containsKey(MAX_ATTEMPTS_PER_PARTITION)) {
        b.setMaxAttemptsPerPartition(Integer.parseInt(cfg.get(MAX_ATTEMPTS_PER_PARTITION)));
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

    public Builder setTargetPartitions(List<Integer> v) {
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

    public Builder setCommandConfig(String v) {
      _commandConfig = v;
      return this;
    }

    public Builder setTimeoutPerPartition(long v) {
      _timeoutPerPartition = v;
      return this;
    }

    public Builder setNumConcurrentTasksPerInstance(int v) {
      _numConcurrentTasksPerInstance = v;
      return this;
    }

    public Builder setMaxAttemptsPerPartition(int v) {
      _maxAttemptsPerPartition = v;
      return this;
    }

    private void validate() {
      if (_targetResource == null && (_targetPartitions == null || _targetPartitions.isEmpty())) {
        throw new IllegalArgumentException(String.format(
            "%s cannot be null without specified partitions", TARGET_RESOURCE));
      }
      if (_targetResource != null && _targetPartitionStates != null
          && _targetPartitionStates.isEmpty()) {
        throw new IllegalArgumentException(String.format("%s cannot be empty",
            TARGET_PARTITION_STATES));
      }
      if (_command == null) {
        throw new IllegalArgumentException(String.format("%s cannot be null", COMMAND));
      }
      if (_timeoutPerPartition < 0) {
        throw new IllegalArgumentException(String.format("%s has invalid value %s",
            TIMEOUT_PER_PARTITION, _timeoutPerPartition));
      }
      if (_numConcurrentTasksPerInstance < 1) {
        throw new IllegalArgumentException(String.format("%s has invalid value %s",
            NUM_CONCURRENT_TASKS_PER_INSTANCE, _numConcurrentTasksPerInstance));
      }
      if (_maxAttemptsPerPartition < 1) {
        throw new IllegalArgumentException(String.format("%s has invalid value %s",
            MAX_ATTEMPTS_PER_PARTITION, _maxAttemptsPerPartition));
      }
      if (_workflow == null) {
        throw new IllegalArgumentException(String.format("%s cannot be null", WORKFLOW_ID));
      }
    }

    private static List<Integer> csvToIntList(String csv) {
      String[] vals = csv.split(",");
      List<Integer> l = new ArrayList<Integer>();
      for (String v : vals) {
        l.add(Integer.parseInt(v));
      }

      return l;
    }
  }
}
