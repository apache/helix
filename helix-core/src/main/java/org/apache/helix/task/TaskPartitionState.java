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

/**
 * Enumeration of the states in the "Task" state model.
 */
public enum TaskPartitionState {
  /** The initial state of the state model. */
  INIT,
  /** Indicates that the task is currently running. */
  RUNNING,
  /** Indicates that the task was stopped by the controller. */
  STOPPED,
  /** Indicates that the task completed normally. */
  COMPLETED,
  /** Indicates that the task timed out. */
  TIMED_OUT,
  /** Indicates an error occurred during task execution. */
  TASK_ERROR,
  /** Helix's own internal error state. */
  ERROR,
  /** A Helix internal state. */
  DROPPED
}
