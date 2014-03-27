/*
 * $Id$
 */
package org.apache.helix.task;

/**
 * The result of a task execution.
 */
public class TaskResult {
  /**
   * An enumeration of status codes.
   */
  public enum Status {
    /** The task completed normally. */
    COMPLETED,
    /**
     * The task was cancelled externally, i.e. {@link org.apache.helix.task.Task#cancel()} was
     * called.
     */
    CANCELED,
    /** The task encountered an error from which it could not recover. */
    ERROR
  }

  private final Status _status;
  private final String _info;

  /**
   * Constructs a new {@link TaskResult}.
   * @param status The status code.
   * @param info Information that can be interpreted by the {@link Task} implementation that
   *          constructed this object.
   *          May encode progress or check point information that can be used by the task to resume
   *          from where it
   *          left off in a previous execution.
   */
  public TaskResult(Status status, String info) {
    _status = status;
    _info = info;
  }

  public Status getStatus() {
    return _status;
  }

  public String getInfo() {
    return _info;
  }

  @Override
  public String toString() {
    return "TaskResult{" + "_status=" + _status + ", _info='" + _info + '\'' + '}';
  }
}
