package org.apache.helix.task;

/**
 * The interface that is to be implemented by a specific task implementation.
 */
public interface Task {
  /**
   * Execute the task.
   * @return A {@link TaskResult} object indicating the status of the task and any additional context
   *         information that
   *         can be interpreted by the specific {@link Task} implementation.
   */
  TaskResult run();

  /**
   * Signals the task to stop execution. The task implementation should carry out any clean up
   * actions that may be
   * required and return from the {@link #run()} method.
   */
  void cancel();
}
