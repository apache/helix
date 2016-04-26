package org.apache.helix.integration.task;

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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskResult;

public class MockTask implements Task {
  public static final String TASK_COMMAND = "Reindex";
  public static final String TIMEOUT_CONFIG = "Timeout";
  public static final String TASK_RESULT_STATUS = "TaskResultStatus";
  public static final String THROW_EXCEPTION = "ThrowException";
  private final long _delay;
  private volatile boolean _canceled;
  private TaskResult.Status _taskResultStatus;
  private boolean _throwException;

  public MockTask(TaskCallbackContext context) {
    Map<String, String> cfg = context.getJobConfig().getJobCommandConfigMap();
    if (cfg == null) {
      cfg = new HashMap<String, String>();
    }

    TaskConfig taskConfig = context.getTaskConfig();
    Map<String, String> taskCfg = taskConfig.getConfigMap();
    if (taskCfg != null) {
      cfg.putAll(taskCfg);
    }

    _delay = cfg.containsKey(TIMEOUT_CONFIG) ? Long.parseLong(cfg.get(TIMEOUT_CONFIG)) : 100L;
    _taskResultStatus = cfg.containsKey(TASK_RESULT_STATUS) ?
        TaskResult.Status.valueOf(cfg.get(TASK_RESULT_STATUS)) :
        TaskResult.Status.COMPLETED;
    _throwException = cfg.containsKey(THROW_EXCEPTION) ?
        Boolean.valueOf(cfg.containsKey(THROW_EXCEPTION)) :
        false;
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

    if (_throwException) {
      throw new RuntimeException("Test failed");
    }

    return new TaskResult(_taskResultStatus, String.valueOf(timeLeft < 0 ? 0 : timeLeft));
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
