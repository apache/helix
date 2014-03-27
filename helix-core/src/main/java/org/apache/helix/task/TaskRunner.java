/*
 * $Id$
 */
package org.apache.helix.task;

import org.apache.helix.HelixManager;
import org.apache.log4j.Logger;

/**
 * A wrapping {@link Runnable} used to manage the life-cycle of a user-defined {@link Task}
 * implementation.
 */
public class TaskRunner implements Runnable {
  private static final Logger LOG = Logger.getLogger(TaskRunner.class);
  private final HelixManager _manager;
  private final String _taskName;
  private final String _taskPartition;
  private final String _sessionId;
  private final String _instance;
  // Synchronization object used to signal that the task has been scheduled on a thread.
  private final Object _startedSync = new Object();
  // Synchronization object used to signal that the task has finished.
  private final Object _doneSync = new Object();
  private final Task _task;
  // Stores the result of the task once it has finished.
  private volatile TaskResult _result = null;
  // If true, indicates that the task has started.
  private volatile boolean _started = false;
  // If true, indicates that the task was canceled due to a task timeout.
  private volatile boolean _timeout = false;
  // If true, indicates that the task has finished.
  private volatile boolean _done = false;

  public TaskRunner(Task task, String taskName, String taskPartition, String instance,
      HelixManager manager, String sessionId) {
    _task = task;
    _taskName = taskName;
    _taskPartition = taskPartition;
    _instance = instance;
    _manager = manager;
    _sessionId = sessionId;
  }

  @Override
  public void run() {
    try {
      signalStarted();
      _result = _task.run();

      switch (_result.getStatus()) {
      case COMPLETED:
        requestStateTransition(TaskPartitionState.COMPLETED);
        break;
      case CANCELED:
        if (_timeout) {
          requestStateTransition(TaskPartitionState.TIMED_OUT);
        }
        // Else the state transition to CANCELED was initiated by the controller.
        break;
      case ERROR:
        requestStateTransition(TaskPartitionState.TASK_ERROR);
        break;
      default:
        throw new AssertionError("Unknown result type.");
      }
    } catch (Exception e) {
      requestStateTransition(TaskPartitionState.TASK_ERROR);
    } finally {
      synchronized (_doneSync) {
        _done = true;
        _doneSync.notifyAll();
      }
    }
  }

  /**
   * Signals the task to cancel itself.
   */
  public void timeout() {
    _timeout = true;
    cancel();
  }

  /**
   * Signals the task to cancel itself.
   */
  public void cancel() {
    _task.cancel();
  }

  /**
   * Waits uninterruptibly until the task has started.
   */
  public void waitTillStarted() {
    synchronized (_startedSync) {
      while (!_started) {
        try {
          _startedSync.wait();
        } catch (InterruptedException e) {
          LOG.warn(
              String.format("Interrupted while waiting for task %s to start.", _taskPartition), e);
        }
      }
    }
  }

  /**
   * Waits uninterruptibly until the task has finished, either normally or due to an
   * error/cancellation..
   */
  public TaskResult waitTillDone() {
    synchronized (_doneSync) {
      while (!_done) {
        try {
          _doneSync.wait();
        } catch (InterruptedException e) {
          LOG.warn(
              String.format("Interrupted while waiting for task %s to complete.", _taskPartition),
              e);
        }
      }
    }
    return _result;
  }

  /**
   * Signals any threads waiting for this task to start.
   */
  private void signalStarted() {
    synchronized (_startedSync) {
      _started = true;
      _startedSync.notifyAll();
    }
  }

  /**
   * Requests the controller for a state transition.
   * @param state The state transition that is being requested.
   */
  private void requestStateTransition(TaskPartitionState state) {
    boolean success =
        TaskUtil.setRequestedState(_manager.getHelixDataAccessor(), _instance, _sessionId,
            _taskName, _taskPartition, state);
    if (!success) {
      LOG.error(String
          .format(
              "Failed to set the requested state to %s for instance %s, session id %s, task partition %s.",
              state, _instance, _sessionId, _taskPartition));
    }
  }
}
