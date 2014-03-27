/*
 * $Id$
 */
package org.apache.helix.task;

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
