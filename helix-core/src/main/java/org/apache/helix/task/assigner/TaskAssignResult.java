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

import org.apache.helix.task.TaskConfig;

/**
 * TaskAssignResult represents assignment metadata for a task and is created by TaskAssigner.
 */
public class TaskAssignResult implements Comparable<TaskAssignResult> {

  public enum FailureReason {
    // Instance does not have sufficient resource quota
    INSUFFICIENT_QUOTA,

    // Instance does not have the required resource type
    NO_SUCH_RESOURCE_TYPE,

    // Required quota type is not configured
    NO_SUCH_QUOTA_TYPE,

    // Task cannot be assigned twice on a node without releasing it first
    TASK_ALREADY_ASSIGNED
  }

  private final boolean _isAssignmentSuccessful;
  private final int _fitnessScore;
  private final FailureReason _reason;
  private final AssignableInstance _node;
  private final TaskConfig _taskConfig;
  private final String _description;
  private final String _quotaType;

  public TaskAssignResult(TaskConfig taskConfig, String quotaType, AssignableInstance node,
      boolean isSuccessful, int fitness, FailureReason reason, String description) {
    _isAssignmentSuccessful = isSuccessful;
    _fitnessScore = fitness;
    _reason = reason;
    _taskConfig = taskConfig;
    _node = node;
    _description = description;
    _quotaType = quotaType;
  }

  /**
   * Returns if the task is successfully assigned or not.
   * @return true if assignment was successful. False otherwise
   */
  public boolean isSuccessful() {
    return _isAssignmentSuccessful;
  }

  /**
   * Returns TaskConfig of this TaskAssignResult.
   */
  public TaskConfig getTaskConfig() {
    return _taskConfig;
  }

  /**
   * Returns the quota type of the underlying task being assigned. This will be used at release time
   * so that the right quota type will see resources being released.
   * @return quota type of the task
   */
  public String getQuotaType() {
    return _quotaType;
  }

  /**
   * Returns the name of the instance this task was assigned to.
   * @return instance name. Null if assignment was not successful
   */
  public String getInstanceName() {
    return _node == null ? null : _node.getInstanceName();
  }

  /**
   * Return the reference of the AssignableInstance of this assignment.
   * @return AssignableInstance object, null if assignment was not successful
   */
  public AssignableInstance getAssignableInstance() {
    return _node;
  }

  /**
   * Returns the reason for assignment failure.
   * @return a FailureReason instance. Null if assignment was successful
   */
  public FailureReason getFailureReason() {
    return _reason;
  }

  /**
   * Returns a one sentence description that carries detail information about
   * assignment failure for debug purpose.
   * @return description
   */
  public String getFailureDescription() {
    return _description;
  }

  /**
   * Returns the fitness score of this assignment. isSuccessful() must return true for
   * this return value to be meaningful.
   * @return an integer describing fitness score
   */
  public int getFitnessScore() {
    return _fitnessScore;
  }

  @Override
  public int compareTo(TaskAssignResult other) {
    // Sorted by descending order
    return other.getFitnessScore() - _fitnessScore;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("TaskAssignResult{");
    sb.append(String.format("_taskConfig=%s, ", _taskConfig));
    sb.append(String.format("_isAssignmentSuccessful=%s, ", _isAssignmentSuccessful));
    sb.append(String.format("_fitnessScore=%s, ", _fitnessScore));
    sb.append(String.format("_reason='%s', ", _reason == null ? "null" : _reason.name()));
    sb.append(String.format("_node='%s', ", _node == null ? "null" : _node.getInstanceName()));
    sb.append(String.format("_description='%s'", _description));
    sb.append("}");
    return sb.toString();
  }
}