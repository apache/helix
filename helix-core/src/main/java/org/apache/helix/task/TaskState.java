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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Enumeration of current task states. This value is stored in the rebalancer context.
 */
public enum TaskState {
  /**
   * The task has yet to start
   */
  NOT_STARTED,
  /**
   * The task is in progress.
   */
  IN_PROGRESS,
  /**
   * The task has been stopped. It may be resumed later.
   */
  STOPPED,
  /**
   * The task is in stopping process. Will complete if subtasks are stopped or completed
   */
  STOPPING,
  /**
   * The task has failed. It cannot be resumed.
   */
  FAILED,
  /**
   * All the task partitions have completed normally.
   */
  COMPLETED,
  /**
   * The task are aborted due to workflow fail
   */
  ABORTED,
  /**
   * The allowed execution time for the job.
   * TODO: also use this for the task
   */
  TIMED_OUT,
  /**
   * The job is in the process to be TIMED_OUT.
   * Usually this means some tasks are still being aborted.
   */
  TIMING_OUT,
  /**
   * The job is in the process to be FAILED.
   * Unfinished tasks are being aborted.
   */
  FAILING
}
