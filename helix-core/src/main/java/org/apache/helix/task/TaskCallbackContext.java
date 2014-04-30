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

import org.apache.helix.HelixManager;

/**
 * A wrapper for all information about a task and the job of which it is a part.
 */
public class TaskCallbackContext {
  private HelixManager _manager;
  private TaskConfig _taskConfig;
  private JobConfig _jobConfig;

  void setManager(HelixManager manager) {
    _manager = manager;
  }

  void setTaskConfig(TaskConfig taskConfig) {
    _taskConfig = taskConfig;
  }

  void setJobConfig(JobConfig jobConfig) {
    _jobConfig = jobConfig;
  }

  /**
   * Get an active Helix connection
   * @return HelixManager instance
   */
  public HelixManager getManager() {
    return _manager;
  }

  /**
   * Get task-specific configuration properties
   * @return TaskConfig instance
   */
  public TaskConfig getTaskConfig() {
    return _taskConfig;
  }

  /**
   * Get job-specific configuration properties
   * @return JobConfig instance
   */
  public JobConfig getJobConfig() {
    return _jobConfig;
  }
}
