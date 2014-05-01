package org.apache.helix.provisioning.yarn.example;

import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskResult;
import org.apache.log4j.Logger;

/**
 * Callbacks for task execution - THIS INTERFACE IS SUBJECT TO CHANGE
 */
public class MyTask implements Task {
  private static final Logger LOG = Logger.getLogger(MyTask.class);
  private static final long DEFAULT_DELAY = 60000L;
  private final long _delay;
  private volatile boolean _canceled;

  public MyTask(TaskCallbackContext context) {
    LOG.info("Job config" + context.getJobConfig().getJobConfigMap());
    if (context.getTaskConfig() != null) {
      LOG.info("Task config: " + context.getTaskConfig().getConfigMap());
    }
    _delay = DEFAULT_DELAY;
  }

  @Override
  public TaskResult run() {
    long expiry = System.currentTimeMillis() + _delay;
    long timeLeft;
    while (System.currentTimeMillis() < expiry) {
      if (_canceled) {
        timeLeft = expiry - System.currentTimeMillis();
        return new TaskResult(TaskResult.Status.CANCELED, String.valueOf(timeLeft < 0 ? 0
            : timeLeft));
      }
      sleep(50);
    }
    timeLeft = expiry - System.currentTimeMillis();
    return new TaskResult(TaskResult.Status.COMPLETED, String.valueOf(timeLeft < 0 ? 0 : timeLeft));
  }

  @Override
  public void cancel() {
    _canceled = true;
  }

  private static void sleep(long d) {
    try {
      Thread.sleep(d);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
