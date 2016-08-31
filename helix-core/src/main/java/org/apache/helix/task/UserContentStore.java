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

import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;

/**
 * UserContentStore provides default implementation of user defined key-value pair store per task,
 * job and workflow level.
 *
 * TODO: This class should be merged to Task interface when Helix bump up to Java 8
 */
public abstract class UserContentStore {

  protected enum Scope {
    /**
     * Define the content store in workflow level
     */
    WORKFLOW,

    /**
     * Define the content store in job level
     */
    JOB,

    /**
     * Define the content store in task level
     */
    TASK
  }

  private HelixManager _manager;
  private String _workflowName;
  private String _jobName;
  private String _taskName;

  /**
   * Default initialization of user content store
   * @param manager The Helix manager
   * @param workflowName The name of workflow that the task belongs to
   * @param jobName The name of job that the task belongs to
   * @param taskName The name of current task
   */
  public void init(HelixManager manager, String workflowName, String jobName, String taskName) {
    _manager = manager;
    _workflowName = workflowName;
    _jobName = jobName;
    _taskName = taskName;
  }

  /**
   * Default implementation for user defined put key-value pair
   * @param key The key of key-value pair
   * @param value The value of key-value pair
   * @param scope The scope defines which layer to store
   */
  public void putUserContent(String key, String value, Scope scope) {
    switch (scope) {
    case WORKFLOW:
      TaskUtil.addWorkflowJobUserContent(_manager, _workflowName, key, value);
      break;
    case JOB:
      TaskUtil.addWorkflowJobUserContent(_manager, _jobName, key, value);
      break;
    case TASK:
      TaskUtil.addTaskUserContent(_manager, _jobName, _taskName, key, value);
      break;
    default:
      throw new HelixException("Invalid scope : " + scope.name());
    }
  }

  /**
   * Default implementation for user defined get key-value pair
   * @param key The key of key-value pair
   * @param scope The scope defines which layer that key-value pair stored
   * @return Null if key-value pair not found or this content store does not exists. Otherwise,
   *         return a String
   */
  public String getUserContent(String key, Scope scope) {
    switch (scope) {
    case WORKFLOW:
      return TaskUtil.getWorkflowJobUserContent(_manager, _workflowName, key);
    case JOB:
      return TaskUtil.getWorkflowJobUserContent(_manager, _jobName, key);
    case TASK:
      return TaskUtil.getTaskUserContent(_manager, _jobName, _taskName, key);
    default:
      throw new HelixException("Invalid scope : " + scope.name());
    }
  }
}
