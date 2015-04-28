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

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;

@StateModelInfo(states = "{'NOT USED BY HELIX'}", initialState = "INIT")
public class TaskStateModel extends TransitionHandler {
  private static final Logger LOG = Logger.getLogger(TaskStateModel.class);
  private final HelixManager _manager;
  private final ExecutorService _taskExecutor;
  private final Map<String, TaskFactory> _taskFactoryRegistry;
  private final Timer _timer = new Timer("TaskStateModel time out daemon", true);
  private TaskRunner _taskRunner;

  public TaskStateModel(HelixManager manager, Map<String, TaskFactory> taskFactoryRegistry) {
    _manager = manager;
    _taskFactoryRegistry = taskFactoryRegistry;
    _taskExecutor = Executors.newFixedThreadPool(40, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "TaskStateModel-thread-pool");
      }
    });
  }

  public boolean isShutdown() {
    return _taskExecutor.isShutdown();
  }

  public boolean isTerminated() {
    return _taskExecutor.isTerminated();
  }

  public void shutdown() {
    reset();
    _taskExecutor.shutdown();
    _timer.cancel();
  }

  public boolean awaitTermination(long timeout, TimeUnit unit)
      throws InterruptedException {
    return _taskExecutor.awaitTermination(timeout, unit);
  }

  @Transition(to = "RUNNING", from = "INIT")
  public void onBecomeRunningFromInit(Message msg, NotificationContext context) {
    startTask(msg, msg.getPartitionName());
  }

  @Transition(to = "STOPPED", from = "RUNNING")
  public String onBecomeStoppedFromRunning(Message msg, NotificationContext context) {
    String taskPartition = msg.getPartitionName();
    if (_taskRunner == null) {
      throw new IllegalStateException(String.format(
          "Invalid state transition. There is no running task for partition %s.", taskPartition));
    }

    _taskRunner.cancel();
    TaskResult r = _taskRunner.waitTillDone();
    LOG.info(String.format("Task %s completed with result %s.", msg.getPartitionName(), r));

    return r.getInfo();
  }

  @Transition(to = "COMPLETED", from = "RUNNING")
  public void onBecomeCompletedFromRunning(Message msg, NotificationContext context) {
    String taskPartition = msg.getPartitionName();
    if (_taskRunner == null) {
      throw new IllegalStateException(String.format(
          "Invalid state transition. There is no running task for partition %s.", taskPartition));
    }

    TaskResult r = _taskRunner.waitTillDone();
    if (r.getStatus() != TaskResult.Status.COMPLETED) {
      throw new IllegalStateException(String.format(
          "Partition %s received a state transition to %s but the result status code is %s.",
          msg.getPartitionName(), msg.getToState(), r.getStatus()));
    }
  }

  @Transition(to = "TIMED_OUT", from = "RUNNING")
  public String onBecomeTimedOutFromRunning(Message msg, NotificationContext context) {
    String taskPartition = msg.getPartitionName();
    if (_taskRunner == null) {
      throw new IllegalStateException(String.format(
          "Invalid state transition. There is no running task for partition %s.", taskPartition));
    }

    TaskResult r = _taskRunner.waitTillDone();
    if (r.getStatus() != TaskResult.Status.CANCELED) {
      throw new IllegalStateException(String.format(
          "Partition %s received a state transition to %s but the result status code is %s.",
          msg.getPartitionName(), msg.getToState(), r.getStatus()));
    }

    return r.getInfo();
  }

  @Transition(to = "TASK_ERROR", from = "RUNNING")
  public String onBecomeTaskErrorFromRunning(Message msg, NotificationContext context) {
    String taskPartition = msg.getPartitionName();
    if (_taskRunner == null) {
      throw new IllegalStateException(String.format(
          "Invalid state transition. There is no running task for partition %s.", taskPartition));
    }

    TaskResult r = _taskRunner.waitTillDone();
    if (r.getStatus() != TaskResult.Status.ERROR) {
      throw new IllegalStateException(String.format(
          "Partition %s received a state transition to %s but the result status code is %s.",
          msg.getPartitionName(), msg.getToState(), r.getStatus()));
    }

    return r.getInfo();
  }

  @Transition(to = "RUNNING", from = "STOPPED")
  public void onBecomeRunningFromStopped(Message msg, NotificationContext context) {
    startTask(msg, msg.getPartitionName());
  }

  @Transition(to = "DROPPED", from = "INIT")
  public void onBecomeDroppedFromInit(Message msg, NotificationContext context) {
    _taskRunner = null;
  }

  @Transition(to = "DROPPED", from = "RUNNING")
  public void onBecomeDroppedFromRunning(Message msg, NotificationContext context) {
    String taskPartition = msg.getPartitionName();
    if (_taskRunner == null) {
      throw new IllegalStateException(String.format(
          "Invalid state transition. There is no running task for partition %s.", taskPartition));
    }

    _taskRunner.cancel();
    TaskResult r = _taskRunner.waitTillDone();
    LOG.info(String.format("Task partition %s returned result %s.", msg.getPartitionName(), r));
    _taskRunner = null;
  }

  @Transition(to = "DROPPED", from = "COMPLETED")
  public void onBecomeDroppedFromCompleted(Message msg, NotificationContext context) {
    _taskRunner = null;
  }

  @Transition(to = "DROPPED", from = "STOPPED")
  public void onBecomeDroppedFromStopped(Message msg, NotificationContext context) {
    _taskRunner = null;
  }

  @Transition(to = "DROPPED", from = "TIMED_OUT")
  public void onBecomeDroppedFromTimedOut(Message msg, NotificationContext context) {
    _taskRunner = null;
  }

  @Transition(to = "DROPPED", from = "TASK_ERROR")
  public void onBecomeDroppedFromTaskError(Message msg, NotificationContext context) {
    _taskRunner = null;
  }

  @Transition(to = "INIT", from = "RUNNING")
  public void onBecomeInitFromRunning(Message msg, NotificationContext context) {
    String taskPartition = msg.getPartitionName();
    if (_taskRunner == null) {
      throw new IllegalStateException(String.format(
          "Invalid state transition. There is no running task for partition %s.", taskPartition));
    }

    _taskRunner.cancel();
    TaskResult r = _taskRunner.waitTillDone();
    LOG.info(String.format("Task partition %s returned result %s.", msg.getPartitionName(), r));
    _taskRunner = null;
  }

  @Transition(to = "INIT", from = "COMPLETED")
  public void onBecomeInitFromCompleted(Message msg, NotificationContext context) {
    _taskRunner = null;
  }

  @Transition(to = "INIT", from = "STOPPED")
  public void onBecomeInitFromStopped(Message msg, NotificationContext context) {
    _taskRunner = null;
  }

  @Transition(to = "INIT", from = "TIMED_OUT")
  public void onBecomeInitFromTimedOut(Message msg, NotificationContext context) {
    _taskRunner = null;
  }

  @Transition(to = "INIT", from = "TASK_ERROR")
  public void onBecomeInitFromTaskError(Message msg, NotificationContext context) {
    _taskRunner = null;
  }

  @Override
  public void reset() {
    if (_taskRunner != null) {
      _taskRunner.cancel();
    }
  }

  private void startTask(Message msg, String taskPartition) {
    JobConfig cfg = TaskUtil.getJobCfg(_manager, msg.getResourceName());
    TaskConfig taskConfig = null;
    String command = cfg.getCommand();

    // Get a task-specific command if specified
    JobContext ctx = TaskUtil.getJobContext(_manager, msg.getResourceName());
    int pId = Integer.parseInt(taskPartition.substring(taskPartition.lastIndexOf('_') + 1));
    if (ctx.getTaskIdForPartition(pId) != null) {
      taskConfig = cfg.getTaskConfig(ctx.getTaskIdForPartition(pId));
      if (taskConfig != null) {
        if (taskConfig.getCommand() != null) {
          command = taskConfig.getCommand();
        }
      }
    }

    // Report a target if that was used to assign the partition
    String target = ctx.getTargetForPartition(pId);
    if (taskConfig == null && target != null) {
      taskConfig = TaskConfig.from(target);
    }

    // Populate a task callback context
    TaskCallbackContext callbackContext = new TaskCallbackContext();
    callbackContext.setManager(_manager);
    callbackContext.setJobConfig(cfg);
    callbackContext.setTaskConfig(taskConfig);

    // Create a task instance with this command
    if (command == null || _taskFactoryRegistry == null
        || !_taskFactoryRegistry.containsKey(command)) {
      throw new IllegalStateException("No callback implemented for task " + command);
    }
    TaskFactory taskFactory = _taskFactoryRegistry.get(command);
    Task task = taskFactory.createNewTask(callbackContext);

    // Submit the task for execution
    _taskRunner =
        new TaskRunner(this, task, msg.getResourceName(), taskPartition, msg.getTgtName(),
            _manager, msg.getTgtSessionId());
    _taskExecutor.submit(_taskRunner);
    _taskRunner.waitTillStarted();

    // Set up a timer to cancel the task when its time out expires.
    _timer.schedule(new TimerTask() {
      @Override
      public void run() {
        if (_taskRunner != null) {
          _taskRunner.timeout();
        }
      }
    }, cfg.getTimeoutPerTask());
  }
}
