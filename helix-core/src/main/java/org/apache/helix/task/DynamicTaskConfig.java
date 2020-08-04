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

import java.util.List;

import org.apache.helix.zookeeper.datamodel.ZNRecord;

/**
 * A wrapper class for ZNRecord, used to store configs for tasks that are to be dynamically loaded
 */
public class DynamicTaskConfig {
  private final ZNRecord _taskConfig;

  /**
   * Initialize task config with an existing ZNRecord
   * @param taskConfig
   */
  public DynamicTaskConfig(ZNRecord taskConfig) {
    _taskConfig = taskConfig;
  }

  /**
   * Initialize task config with parameters
   * @param id
   * @param jarFilePath path of the JAR file containing the task
   * @param taskVersion task version
   * @param taskClassesFqns list of the {@link Task} classes fully qualified names
   * @param taskFactoryFqn {@link TaskFactory} class fully qualified name
   */
  public DynamicTaskConfig(String id, String jarFilePath, String taskVersion, List<String> taskClassesFqns,
      String taskFactoryFqn) {
    _taskConfig = new ZNRecord(id);
    _taskConfig.setSimpleField(TaskConstants.TASK_JAR_FILE_KEY, jarFilePath);
    _taskConfig.setSimpleField(TaskConstants.TASK_VERSION_KEY, taskVersion);
    _taskConfig.setListField(TaskConstants.TASK_CLASSES_KEY, taskClassesFqns);
    _taskConfig.setSimpleField(TaskConstants.TASK_FACTORY_KEY, taskFactoryFqn);
  }

  /**
   * Get the task config ZNRecord
   * @return
   */
  public ZNRecord getTaskConfigZNRecord() {
    return _taskConfig;
  }

  /**
   * Get the address of the JAR file containing the task
   * @return
   */
  public String getJarFilePath() {
    return _taskConfig.getSimpleField(TaskConstants.TASK_JAR_FILE_KEY);
  }

  /**
   * Get the task version
   * @return
   */
  public String getTaskVersion() {
    return _taskConfig.getSimpleField(TaskConstants.TASK_VERSION_KEY);
  }

  /**
   * Get the list of the {@link Task} classes fully qualified names
   * @return
   */
  public List<String> getTaskClassesFqns() {
    return _taskConfig.getListField(TaskConstants.TASK_CLASSES_KEY);
  }

  /**
   * Get the {@link TaskFactory} class fully qualified name
   * @return
   */
  public String getTaskFactoryFqn() {
    return _taskConfig.getSimpleField(TaskConstants.TASK_FACTORY_KEY);
  }

  @Override
  public String toString() {
    return "TaskConfig=" + _taskConfig.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj instanceof DynamicTaskConfig) {
      DynamicTaskConfig that = (DynamicTaskConfig) obj;
      if (that._taskConfig != null) {
        return that._taskConfig.equals(this._taskConfig);
      }
    }
    return false;
  }
}
