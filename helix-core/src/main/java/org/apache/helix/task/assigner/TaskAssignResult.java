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
public class TaskAssignResult {

  public enum FailureReason {
    // Instance does not have sufficient resource quota
    INSUFFICIENT_QUOTA
  }

  private boolean isAssignmentSuccessful;

  /**
   * Returns if the task is successfully assigned or not.
   * @return true if assignment was successful. False otherwise
   */
  public boolean isSuccessful() {
    return isAssignmentSuccessful;
  }

  /**
   * Returns TaskConfig of this TaskAssignResult.
   */
  public TaskConfig getTaskConfig() {
    // TODO: implement
    return new TaskConfig(null, null);
  }

  /**
   * Returns the name of the instance this task was assigned to.
   * @return instance name. Null if assignment was not successful
   */
  public String getInstanceName() {
    // TODO: implement
    return null;
  }

  /**
   * Returns the reason for assignment failure.
   * @return a FailureReason instance. Null if assignment was successful
   */
  public FailureReason getFailureReason() {
    // TODO: implement
    return null;
  }
}
