/*
 * $Id$
 */
package org.apache.helix.task;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;

@StateModelInfo(states = "{'NOT USED BY HELIX'}", initialState = "INIT")
public class TaskStateModel extends StateModel {
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
    TaskConfig cfg = TaskUtil.getTaskCfg(_manager, msg.getResourceName());
    TaskFactory taskFactory = _taskFactoryRegistry.get(cfg.getCommand());
    Task task = taskFactory.createNewTask(cfg.getCommandConfig());

    _taskRunner =
        new TaskRunner(task, msg.getResourceName(), taskPartition, msg.getTgtName(), _manager,
            msg.getTgtSessionId());
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
    }, cfg.getTimeoutPerPartition());
  }
}
