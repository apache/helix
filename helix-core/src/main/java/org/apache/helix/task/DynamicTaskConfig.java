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
  private ZNRecord _taskConfig;

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
   * @param jarFile address of the JAR file containing the task
   * @param taskVersion task version
   * @param taskClasses list of the {@link Task} classes names
   * @param taskFactory {@link TaskFactory} class name
   */
  public DynamicTaskConfig(String id, String jarFile, String taskVersion, List<String> taskClasses,
      String taskFactory) {
    _taskConfig = new ZNRecord(id);
    _taskConfig.setSimpleField(TaskConstants.TASK_JAR_FILE_KEY, jarFile);
    _taskConfig.setSimpleField(TaskConstants.TASK_VERSION_KEY, taskVersion);
    _taskConfig.setListField(TaskConstants.TASK_CLASSES_KEY, taskClasses);
    _taskConfig.setSimpleField(TaskConstants.TASK_FACTORY_KEY, taskFactory);
  }

  /**
   * Get the task config ZNRecord
   * @return
   */
  public ZNRecord getTaskConfig() {
    return _taskConfig;
  }

  /**
   * Set the task config ZNRecord
   * @param taskConfig
   */
  public void setTaskConfig(ZNRecord taskConfig) {
    _taskConfig = taskConfig;
  }

  /**
   * Get the address of the JAR file containing the task
   * @return
   */
  public String getJarFile() {
    return _taskConfig.getSimpleField(TaskConstants.TASK_JAR_FILE_KEY);
  }

  /**
   * Set the address of the JAR file containing the task
   * @param jarFile
   */
  public void setJarFile(String jarFile) {
    _taskConfig.setSimpleField(TaskConstants.TASK_JAR_FILE_KEY, jarFile);
  }

  /**
   * Get the task version
   * @return
   */
  public String getTaskVersion() {
    return _taskConfig.getSimpleField(TaskConstants.TASK_VERSION_KEY);
  }

  /**
   * Set the task version
   * @param taskVersion
   */
  public void seTaskVersion(String taskVersion) {
    _taskConfig.setSimpleField(TaskConstants.TASK_VERSION_KEY, taskVersion);
  }

  /**
   * Get the list of the {@link Task} classes names
   * @return
   */
  public List<String> getTaskClasses() {
    return _taskConfig.getListField(TaskConstants.TASK_CLASSES_KEY);
  }

  /**
   * Set the list of the {@link Task} classe names
   * @param taskClasses
   */
  public void setTaskClasses(List<String> taskClasses) {
    _taskConfig.setListField(TaskConstants.TASK_CLASSES_KEY, taskClasses);
  }

  /**
   * Get the {@link TaskFactory} class name
   * @return
   */
  public String getTaskFactory() {
    return _taskConfig.getSimpleField(TaskConstants.TASK_FACTORY_KEY);
  }

  /**
   * Set the {@link TaskFactory} class name
   * @param taskFactory
   */
  public void setTaskFactory(String taskFactory) {
    _taskConfig.setSimpleField(TaskConstants.TASK_FACTORY_KEY, taskFactory);
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
      if (that.getTaskConfig() != null) {
        return that.getTaskConfig().equals(this.getTaskConfig());
      }
    }
    return false;
  }
}
