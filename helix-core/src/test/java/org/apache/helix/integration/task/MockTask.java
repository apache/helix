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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixException;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.UserContentStore;
import org.codehaus.jackson.map.ObjectMapper;


public class MockTask extends UserContentStore implements Task {
  public static final String TASK_COMMAND = "Reindex";
  public static final String JOB_DELAY = "Delay";
  public static final String TASK_RESULT_STATUS = "TaskResultStatus";
  public static final String THROW_EXCEPTION = "ThrowException";
  public static final String ERROR_MESSAGE = "ErrorMessage";
  public static final String FAILURE_COUNT_BEFORE_SUCCESS = "FailureCountBeforeSuccess";
  public static final String SUCCESS_COUNT_BEFORE_FAIL = "SuccessCountBeforeFail";
  public static final String NOT_ALLOW_TO_CANCEL = "NotAllowToCancel";
  public static final String TARGET_PARTITION_CONFIG = "TargetPartitionConfig";
  private long _delay;
  private volatile boolean _notAllowToCancel;
  private volatile boolean _canceled;
  private TaskResult.Status _taskResultStatus;
  private boolean _throwException;
  private int _numOfFailBeforeSuccess;
  private int _numOfSuccessBeforeFail;
  private String _errorMsg;

  public static boolean _signalFail;

  public MockTask(TaskCallbackContext context) {
    Map<String, String> cfg = context.getJobConfig().getJobCommandConfigMap();
    if (cfg == null) {
      cfg = new HashMap<>();
    }

    TaskConfig taskConfig = context.getTaskConfig();
    Map<String, String> taskConfigMap = taskConfig.getConfigMap();
    if (taskConfigMap != null) {
      cfg.putAll(taskConfigMap);
    }

    _delay = cfg.containsKey(JOB_DELAY) ? Long.parseLong(cfg.get(JOB_DELAY)) : 100L;
    _notAllowToCancel = cfg.containsKey(NOT_ALLOW_TO_CANCEL)
        ? Boolean.parseBoolean(cfg.get(NOT_ALLOW_TO_CANCEL))
        : false;
    _taskResultStatus = cfg.containsKey(TASK_RESULT_STATUS) ?
        TaskResult.Status.valueOf(cfg.get(TASK_RESULT_STATUS)) :
        TaskResult.Status.COMPLETED;
    _throwException = cfg.containsKey(THROW_EXCEPTION) ?
        Boolean.valueOf(cfg.containsKey(THROW_EXCEPTION)) :
        false;
    _numOfFailBeforeSuccess =
        cfg.containsKey(FAILURE_COUNT_BEFORE_SUCCESS) ? Integer.parseInt(
            cfg.get(FAILURE_COUNT_BEFORE_SUCCESS)) : 0;
    _numOfSuccessBeforeFail = cfg.containsKey(SUCCESS_COUNT_BEFORE_FAIL) ? Integer
        .parseInt(cfg.get(SUCCESS_COUNT_BEFORE_FAIL)) : Integer.MAX_VALUE;

    _errorMsg = cfg.containsKey(ERROR_MESSAGE) ? cfg.get(ERROR_MESSAGE) : null;

    setTargetPartitionsConfigs(cfg, taskConfig.getTargetPartition());
  }

  // Override configs if there's config for specific target partitions
  private void setTargetPartitionsConfigs(Map<String, String> cfg, String targetPartition) {
    if (cfg.containsKey(TARGET_PARTITION_CONFIG)) {
      Map<String, Map<String, String>> targetPartitionConfigs =
          deserializeTargetPartitionConfig(cfg.get(TARGET_PARTITION_CONFIG));
      if (targetPartitionConfigs.containsKey(targetPartition)) {
        Map<String, String> targetPartitionConfig = targetPartitionConfigs.get(targetPartition);
        if (targetPartitionConfig.containsKey(JOB_DELAY)) {
          _delay = Long.parseLong(targetPartitionConfig.get(JOB_DELAY));
        }
        if (targetPartitionConfig.containsKey(TASK_RESULT_STATUS)) {
          _taskResultStatus = TaskResult.Status.valueOf(targetPartitionConfig.get(TASK_RESULT_STATUS));
        }
      }
    }
  }

  public static String serializeTargetPartitionConfig(Map<String, Map<String, String>> config) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(config);
    } catch (IOException e) {
      throw new HelixException(e);
    }
  }

  private static Map<String, Map<String, String>> deserializeTargetPartitionConfig(String configString) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(configString, Map.class);
    } catch (IOException e) {
      throw new HelixException(e);
    }
  }

  @Override
  public TaskResult run() {
    long expiry = System.currentTimeMillis() + _delay;
    long timeLeft;
    while (System.currentTimeMillis() < expiry) {
      if (_canceled && !_notAllowToCancel) {
        timeLeft = expiry - System.currentTimeMillis();
        return new TaskResult(TaskResult.Status.CANCELED, String.valueOf(timeLeft < 0 ? 0
            : timeLeft));
      }
      if (_signalFail) {
        return new TaskResult(TaskResult.Status.FAILED, "Signaled to fail.");
      }
      sleep(10);
    }
    timeLeft = expiry - System.currentTimeMillis();

    if (_throwException) {
      _numOfFailBeforeSuccess--;
      if (_errorMsg == null) {
        _errorMsg = "Test failed";
      }
      throw new RuntimeException(_errorMsg != null ? _errorMsg : "Test failed");
    }

    if (getUserContent(SUCCESS_COUNT_BEFORE_FAIL, Scope.WORKFLOW) != null) {
      _numOfSuccessBeforeFail =
          Integer.parseInt(getUserContent(SUCCESS_COUNT_BEFORE_FAIL, Scope.WORKFLOW));
    }
    putUserContent(SUCCESS_COUNT_BEFORE_FAIL, "" + --_numOfSuccessBeforeFail, Scope.WORKFLOW);

    if (_numOfFailBeforeSuccess > 0 || _numOfSuccessBeforeFail < 0){
      _numOfFailBeforeSuccess--;
      throw new RuntimeException(_errorMsg != null ? _errorMsg : "Test failed");
    }

    return new TaskResult(_taskResultStatus,
        _errorMsg != null ? _errorMsg : String.valueOf(timeLeft < 0 ? 0 : timeLeft));
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
