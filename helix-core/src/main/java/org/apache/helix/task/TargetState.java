package org.apache.helix.task;

/**
 * Enumeration of target states for a task.
 */
public enum TargetState {
  /**
   * Indicates that the rebalancer must start/resume the task.
   */
  START,
  /**
   * Indicates that the rebalancer should stop any running task partitions and cease doing any
   * further task
   * assignments.
   */
  STOP,
  /**
   * Indicates that the rebalancer must delete this task.
   */
  DELETE
}
