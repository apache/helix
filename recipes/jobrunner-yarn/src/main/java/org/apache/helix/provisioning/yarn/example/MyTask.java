package org.apache.helix.provisioning.yarn.example;

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

import com.google.common.util.concurrent.AtomicDouble;
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
  private AtomicDouble _progress = new AtomicDouble(0.0);

  public MyTask(TaskCallbackContext context) {
    LOG.info("Job config" + context.getJobConfig().getJobCommandConfigMap());
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
      long currentTime = System.currentTimeMillis();
      updateProgress(currentTime, expiry);
      if (_canceled) {
        timeLeft = expiry - currentTime;
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

  @Override
  public double getProgress() {
    return _progress.get();
  }

  private void updateProgress(long currentTime, long expiry) {
    double progress = 1.0 - (double)(expiry - currentTime) / _delay;
    if (progress < 0.0) {
      progress = 0.0;
    }
    if (progress > 1.0) {
      progress = 1.0;
    }
    _progress.set(progress);
  }

  private static void sleep(long d) {
    try {
      Thread.sleep(d);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
