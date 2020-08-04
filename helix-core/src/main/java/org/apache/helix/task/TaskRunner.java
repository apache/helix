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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;
import org.apache.helix.task.TaskResult.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapping {@link Runnable} used to manage the life-cycle of a user-defined {@link Task}
 * implementation.
 */
public class TaskRunner implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(TaskRunner.class);
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
      try {
        _result = _task.run();
      } catch (ThreadDeath death) {
        throw death;
      } catch (Throwable t) {
        LOG.error("Problem running the task, report task as FAILED.", t);
        _result = new TaskResult(Status.FAILED, "Exception happened in running task: " + t.getMessage());
      }

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
      case FAILED:
        requestStateTransition(TaskPartitionState.TASK_ERROR);
        break;
      case FATAL_FAILED:
        requestStateTransition(TaskPartitionState.TASK_ABORTED);
        break;
      default:
        throw new AssertionError("Unknown task result type: " + _result.getStatus().name());
      }
    } catch (Exception e) {
      LOG.error("Problem running the task, report task as FAILED.", e);
      _result =
          new TaskResult(Status.FAILED, "Exception happened in running task: " + e.getMessage());
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
    if (!_done) {
      _timeout = true;
      cancel();
    }
  }

  /**
   * Signals the task to cancel itself.
   */
  public void cancel() {
    if (!_done) {
      _task.cancel();
    }
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
        setRequestedState(_manager.getHelixDataAccessor(), _instance, _sessionId, _taskName,
            _taskPartition, state);
    if (!success) {
      LOG.error(String
          .format(
              "Failed to set the requested state to %s for instance %s, session id %s, task partition %s.",
              state, _instance, _sessionId, _taskPartition));
    }
  }

  /**
   * Request a state change for a specific task.
   *
   * @param accessor  connected Helix data accessor
   * @param instance  the instance serving the task
   * @param sessionId the current session of the instance
   * @param resource  the job name
   * @param partition the task partition name
   * @param state     the requested state
   * @return true if the request was persisted, false otherwise
   */
  private static boolean setRequestedState(HelixDataAccessor accessor, String instance,
      String sessionId, String resource, String partition, TaskPartitionState state) {
    LOG.debug(
        String.format("Requesting a state transition to %s for partition %s.", state, partition));
    try {
      PropertyKey.Builder keyBuilder = accessor.keyBuilder();
      PropertyKey key = keyBuilder.currentState(instance, sessionId, resource);
      CurrentState currStateDelta = new CurrentState(resource);
      currStateDelta.setRequestedState(partition, state.name());

      return accessor.updateProperty(key, currStateDelta);
    } catch (Exception e) {
      LOG.error(String
          .format("Error when requesting a state transition to %s for partition %s.", state,
              partition), e);
      return false;
    }
  }
}
