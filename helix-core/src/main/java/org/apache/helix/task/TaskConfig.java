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

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.apache.helix.task.beans.TaskBean;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration for an individual task to be run as part of a job.
 */
public class TaskConfig {
  private static final Logger LOG = LoggerFactory.getLogger(TaskConfig.class);

  private enum TaskConfigProperty {
    TASK_ID,
    TASK_COMMAND,
    @Deprecated
    TASK_SUCCESS_OPTIONAL,
    TASK_TARGET_PARTITION
  }

  private final Map<String, String> _configMap;

  @Deprecated
  public TaskConfig(String command, Map<String, String> configMap, boolean successOptional,
      String id, String target) {
    this(command, configMap, id, target);
  }

  @Deprecated
  public TaskConfig(String command, Map<String, String> configMap, boolean successOptional) {
    this(command, configMap, null, null);
  }

  /**
   * Instantiate the task config.
   * @param command the command to invoke for the task
   * @param configMap configuration to be passed as part of the invocation
   * @param id existing task ID
   * @param target target partition for a task
   */
  public TaskConfig(String command, Map<String, String> configMap, String id, String target) {
    if (configMap == null) {
      configMap = Maps.newHashMap();
    }
    if (id == null) {
      id = UUID.randomUUID().toString();
    }
    if (command != null) {
      configMap.put(TaskConfigProperty.TASK_COMMAND.name(), command);
    }
    configMap.put(TaskConfigProperty.TASK_ID.name(), id);
    if (target != null) {
      configMap.put(TaskConfigProperty.TASK_TARGET_PARTITION.name(), target);
    }
    _configMap = configMap;
  }

  /**
   * Instantiate the task config.
   * @param command the command to invoke for the task
   * @param configMap configuration to be passed as part of the invocation
   */
  public TaskConfig(String command, Map<String, String> configMap) {
    this(command, configMap, null, null);
  }

  /**
   * Unique identifier for this task
   * @return UUID as a string
   */
  public String getId() {
    return _configMap.get(TaskConfigProperty.TASK_ID.name());
  }

  /**
   * Get the command to invoke for this task
   * @return string command, or null if not overridden
   */
  public String getCommand() {
    return _configMap.get(TaskConfigProperty.TASK_COMMAND.name());
  }

  /**
   * Get the target partition of this task, if any
   * @return the target partition, or null
   */
  public String getTargetPartition() {
    return _configMap.get(TaskConfigProperty.TASK_TARGET_PARTITION.name());
  }

  /**
   * Check if this task must succeed for a job to succeed
   * This field has been ignored by Helix
   * @return true if success is optional, false otherwise
   */
  @Deprecated
  public boolean isSuccessOptional() {
    // This option will not be used in rebalancer anymore, deprecate it.
    return true;
  }

  /**
   * Get the configuration map for this task's command
   * @return map of configuration key to value
   */
  public Map<String, String> getConfigMap() {
    return _configMap;
  }

  @Override
  public String toString() {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(this);
    } catch (IOException e) {
      LOG.error("Could not serialize TaskConfig", e);
    }
    return super.toString();
  }

  public static class Builder {
    private String _taskId;
    private String _command;
    private String _targetPartition;
    private boolean _successOptional = false;
    private Map<String, String> _configMap;

    public TaskConfig build() {
      return new TaskConfig(_command, _configMap, _taskId, _targetPartition);
    }

    public String getTaskId() {
      return _taskId;
    }

    public Builder setTaskId(String taskId) {
      _taskId = taskId;
      return this;
    }

    public String getCommand() {
      return _command;
    }

    public Builder setCommand(String command) {
      _command = command;
      return this;
    }

    public String getTargetPartition() {
      return _targetPartition;
    }

    public Builder setTargetPartition(String targetPartition) {
      _targetPartition = targetPartition;
      return this;
    }

    @Deprecated
    public boolean isSuccessOptional() {
      return _successOptional;
    }

    @Deprecated
    public Builder setSuccessOptional(boolean successOptional) {
      _successOptional = successOptional;
      return this;
    }

    public Builder addConfig(String key, String value) {
      if (_configMap == null) {
        _configMap = Maps.newHashMap();
      }
      _configMap.put(key, value);
      return this;
    }

    /**
     * Instantiate a typed configuration from just a target.
     * @param target the target partition
     * @return instantiated TaskConfig
     */
    public static TaskConfig from(String target) {
      return new TaskConfig(null, null, null, target);
    }

    /**
     * Instantiate a typed configuration from a bean.
     * @param bean plain bean describing the task
     * @return instantiated TaskConfig
     */
    public static TaskConfig from(TaskBean bean) {
      return new TaskConfig(bean.command, bean.taskConfigMap);
    }

    /**
     * Instantiate a typed configuration from a raw string map
     * @param rawConfigMap mixed map of configuration and task metadata
     * @return instantiated TaskConfig
     */
    @Deprecated
    public static TaskConfig from(Map<String, String> rawConfigMap) {
      String taskId = rawConfigMap.get(TaskConfigProperty.TASK_ID.name());
      String command = rawConfigMap.get(TaskConfigProperty.TASK_COMMAND.name());
      String targetPartition = rawConfigMap.get(TaskConfigProperty.TASK_TARGET_PARTITION.name());
      return new TaskConfig(command, rawConfigMap, taskId, targetPartition);
    }
  }
}
