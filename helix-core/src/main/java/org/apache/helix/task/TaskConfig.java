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

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.apache.helix.task.beans.TaskBean;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.Maps;

/**
 * Configuration for an individual task to be run as part of a job.
 */
public class TaskConfig {
  private enum TaskConfigFields {
    TASK_ID,
    TASK_COMMAND,
    TASK_SUCCESS_OPTIONAL,
    TASK_TARGET_PARTITION
  }

  private static final Logger LOG = Logger.getLogger(TaskConfig.class);

  private final Map<String, String> _configMap;

  /**
   * Instantiate the task config
   * @param command the command to invoke for the task
   * @param configMap configuration to be passed as part of the invocation
   * @param successOptional true if this task need not pass for the job to succeed, false
   *          otherwise
   * @param id existing task ID
   * @param target target partition for a task
   */
  public TaskConfig(String command, Map<String, String> configMap, boolean successOptional,
      String id, String target) {
    if (configMap == null) {
      configMap = Maps.newHashMap();
    }
    if (id == null) {
      id = UUID.randomUUID().toString();
    }
    if (command != null) {
      configMap.put(TaskConfigFields.TASK_COMMAND.toString(), command);
    }
    configMap.put(TaskConfigFields.TASK_SUCCESS_OPTIONAL.toString(),
        Boolean.toString(successOptional));
    configMap.put(TaskConfigFields.TASK_ID.toString(), id);
    if (target != null) {
      configMap.put(TaskConfigFields.TASK_TARGET_PARTITION.toString(), target);
    }
    _configMap = configMap;
  }

  /**
   * Instantiate the task config
   * @param command the command to invoke for the task
   * @param configMap configuration to be passed as part of the invocation
   * @param successOptional true if this task need not pass for the job to succeed, false
   *          otherwise
   */
  public TaskConfig(String command, Map<String, String> configMap, boolean successOptional) {
    this(command, configMap, successOptional, null, null);
  }

  /**
   * Unique identifier for this task
   * @return UUID as a string
   */
  public String getId() {
    return _configMap.get(TaskConfigFields.TASK_ID.toString());
  }

  /**
   * Get the command to invoke for this task
   * @return string command, or null if not overridden
   */
  public String getCommand() {
    return _configMap.get(TaskConfigFields.TASK_COMMAND.toString());
  }

  /**
   * Get the target partition of this task, if any
   * @return the target partition, or null
   */
  public String getTargetPartition() {
    return _configMap.get(TaskConfigFields.TASK_TARGET_PARTITION.toString());
  }

  /**
   * Check if this task must succeed for a job to succeed
   * @return true if success is optional, false otherwise
   */
  public boolean isSuccessOptional() {
    String successOptionalStr = _configMap.get(TaskConfigFields.TASK_SUCCESS_OPTIONAL.toString());
    if (successOptionalStr == null) {
      return false;
    } else {
      return Boolean.parseBoolean(successOptionalStr);
    }
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

  /**
   * Instantiate a typed configuration from just a target
   * @param target the target partition
   * @return instantiated TaskConfig
   */
  public static TaskConfig from(String target) {
    return new TaskConfig(null, null, false, null, target);
  }

  /**
   * Instantiate a typed configuration from a bean
   * @param bean plain bean describing the task
   * @return instantiated TaskConfig
   */
  public static TaskConfig from(TaskBean bean) {
    return new TaskConfig(bean.command, bean.taskConfigMap, bean.successOptional);
  }

  /**
   * Instantiate a typed configuration from a raw string map
   * @param rawConfigMap mixed map of configuration and task metadata
   * @return instantiated TaskConfig
   */
  public static TaskConfig from(Map<String, String> rawConfigMap) {
    String taskId = rawConfigMap.get(TaskConfigFields.TASK_ID.toString());
    String command = rawConfigMap.get(TaskConfigFields.TASK_COMMAND.toString());
    String targetPartition = rawConfigMap.get(TaskConfigFields.TASK_TARGET_PARTITION.toString());
    String successOptionalStr = rawConfigMap.get(TaskConfigFields.TASK_SUCCESS_OPTIONAL.toString());
    boolean successOptional =
        (successOptionalStr != null) ? Boolean.valueOf(successOptionalStr) : null;
    return new TaskConfig(command, rawConfigMap, successOptional, taskId, targetPartition);
  }
}
