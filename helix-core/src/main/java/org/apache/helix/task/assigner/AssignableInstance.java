package org.apache.helix.task.assigner;

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

import java.util.Map;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.task.TaskConfig;

/**
 * AssignableInstance contains instance capacity profile and methods that control capacity and help
 * with task assignment.
 */
public class AssignableInstance {

  /**
   * Caches tasks currently assigned to this instance.
   * Every pipeline iteration will compare Task states in this map to Task states in TaskDataCache.
   * Tasks in a terminal state (finished or failed) will be removed as soon as they reach the state.
   */
  private Map<String, TaskAssignResult> _currentAssignments;
  private ClusterConfig _clusterConfig;
  private InstanceConfig _instanceConfig;
  private LiveInstance _liveInstance;

  public AssignableInstance(ClusterConfig clusterConfig, InstanceConfig instanceConfig,
      LiveInstance liveInstance) {
    _clusterConfig = clusterConfig;
    _instanceConfig = instanceConfig;
    _liveInstance = liveInstance;
  }

  /**
   * Tries to assign the given task on this instance and returns TaskAssignResult. Instance capacity
   * profile is NOT modified by tryAssign.
   * @param task
   * @return
   */
  public TaskAssignResult tryAssign(TaskConfig task) {
    // TODO: implement
    return null;
  }

  /**
   * Performs the following to accept a task:
   * 1. Deduct the amount of resource required by this task
   * 2. Add this TaskAssignResult to _currentAssignments
   * @param result
   * @throws IllegalStateException if TaskAssignResult is not successful
   */
  public void assign(TaskAssignResult result) throws IllegalStateException {
    // TODO: implement
    return;
  }

  /**
   * Performs the following to release resource for a task:
   * 1. Release the resource by adding back what the task required.
   * 2. Remove the TaskAssignResult from _currentAssignments
   * @param taskID
   * @throws IllegalArgumentException if task is not found
   */
  public void release(String taskID) throws IllegalArgumentException {
    // TODO: implement
    return;
  }

  /**
   * Returns taskID -> TaskAssignResult mappings.
   */
  public Map<String, TaskAssignResult> getCurrentAssignments() {
    return _currentAssignments;
  }

  /**
   * Returns the name of this instance.
   */
  public String getInstanceName() {
    return _instanceConfig.getInstanceName();
  }
}
