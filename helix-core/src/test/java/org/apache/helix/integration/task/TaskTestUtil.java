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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.helix.TestHelper;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.ScheduleConfig;
import org.apache.helix.task.TargetState;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.testng.Assert;

/**
 * Static test utility methods.
 */
public class TaskTestUtil {
  private final static int _default_timeout = 2 * 60 * 1000; /* 2 mins */

  public static void pollForEmptyJobState(final TaskDriver driver, final String workflowName,
      final String jobName) throws Exception {
    final String namespacedJobName = String.format("%s_%s", workflowName, jobName);
    boolean succeed = TestHelper.verify(new TestHelper.Verifier() {

      @Override public boolean verify() throws Exception {
        WorkflowContext ctx = driver.getWorkflowContext(workflowName);
        return ctx == null || ctx.getJobState(namespacedJobName) == null;
      }
    }, _default_timeout);
    Assert.assertTrue(succeed);
  }

  public static WorkflowContext pollForWorkflowContext(TaskDriver driver, String workflowResource)
      throws InterruptedException {
    // Wait for completion.
    long st = System.currentTimeMillis();
    WorkflowContext ctx;
    do {
      ctx = driver.getWorkflowContext(workflowResource);
      Thread.sleep(100);
    } while (ctx == null && System.currentTimeMillis() < st + _default_timeout);
    Assert.assertNotNull(ctx);
    return ctx;
  }

  // 1. Different jobs in a same work flow is in RUNNING at the same time
  // 2. No two jobs in the same work flow is in RUNNING at the same instance
  public static boolean pollForWorkflowParallelState(TaskDriver driver, String workflowName)
      throws InterruptedException {

    WorkflowConfig workflowConfig = driver.getWorkflowConfig(workflowName);
    Assert.assertNotNull(workflowConfig);

    WorkflowContext workflowContext = null;
    while (workflowContext == null) {
      workflowContext = driver.getWorkflowContext(workflowName);
      Thread.sleep(100);
    }

    int maxRunningCount = 0;
    boolean finished = false;

    while (!finished) {
      finished = true;
      int runningCount = 0;

      workflowContext = driver.getWorkflowContext(workflowName);
      for (String jobName : workflowConfig.getJobDag().getAllNodes()) {
        TaskState jobState = workflowContext.getJobState(jobName);
        if (jobState == TaskState.IN_PROGRESS) {
          ++runningCount;
          finished = false;
        }
      }

      if (runningCount > maxRunningCount ) {
        maxRunningCount = runningCount;
      }

      List<JobContext> jobContextList = new ArrayList<JobContext>();
      for (String jobName : workflowConfig.getJobDag().getAllNodes()) {
        JobContext jobContext = driver.getJobContext(jobName);
        if (jobContext != null) {
          jobContextList.add(driver.getJobContext(jobName));
        }
      }

      Set<String> instances = new HashSet<String>();
      for (JobContext jobContext : jobContextList) {
        for (int partition : jobContext.getPartitionSet()) {
          String instance = jobContext.getAssignedParticipant(partition);
          TaskPartitionState taskPartitionState = jobContext.getPartitionState(partition);

          if (instance == null) {
            continue;
          }
          if (taskPartitionState != TaskPartitionState.INIT &&
              taskPartitionState != TaskPartitionState.RUNNING) {
            continue;
          }
          if (instances.contains(instance)) {
            return false;
          }

          TaskPartitionState state = jobContext.getPartitionState(partition);
          if (state != TaskPartitionState.COMPLETED) {
            instances.add(instance);
          }
        }
      }

      Thread.sleep(100);
    }

    return maxRunningCount > 1 && maxRunningCount <= workflowConfig.getParallelJobs();
  }

  public static Date getDateFromStartTime(String startTime)
  {
    int splitIndex = startTime.indexOf(':');
    int hourOfDay = 0, minutes = 0;
    try
    {
      hourOfDay = Integer.parseInt(startTime.substring(0, splitIndex));
      minutes = Integer.parseInt(startTime.substring(splitIndex + 1));
    }
    catch (NumberFormatException e)
    {

    }
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.HOUR_OF_DAY, hourOfDay);
    cal.set(Calendar.MINUTE, minutes);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTime();
  }

  public static JobQueue.Builder buildRecurrentJobQueue(String jobQueueName, int delayStart) {
    return buildRecurrentJobQueue(jobQueueName, delayStart, 60);
  }

  public static JobQueue.Builder buildRecurrentJobQueue(String jobQueueName, int delayStart,
      int recurrenInSeconds) {
    return buildRecurrentJobQueue(jobQueueName, delayStart, recurrenInSeconds, null);
  }

  public static JobQueue.Builder buildRecurrentJobQueue(String jobQueueName, int delayStart,
      int recurrenInSeconds, TargetState targetState) {
    WorkflowConfig.Builder workflowCfgBuilder = new WorkflowConfig.Builder();
    workflowCfgBuilder.setExpiry(120000);
    if (targetState != null) {
      workflowCfgBuilder.setTargetState(TargetState.STOP);
    }

    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.MINUTE, cal.get(Calendar.MINUTE) + delayStart / 60);
    cal.set(Calendar.SECOND, cal.get(Calendar.SECOND) + delayStart % 60);
    cal.set(Calendar.MILLISECOND, 0);
    ScheduleConfig scheduleConfig =
        ScheduleConfig.recurringFromDate(cal.getTime(), TimeUnit.SECONDS, recurrenInSeconds);
    workflowCfgBuilder.setScheduleConfig(scheduleConfig);
    return new JobQueue.Builder(jobQueueName).setWorkflowConfig(workflowCfgBuilder.build());
  }

  public static JobQueue.Builder buildRecurrentJobQueue(String jobQueueName) {
    return buildRecurrentJobQueue(jobQueueName, 0);
  }

  public static JobQueue.Builder buildJobQueue(String jobQueueName, int delayStart,
      int failureThreshold) {
    WorkflowConfig.Builder workflowCfgBuilder = new WorkflowConfig.Builder();
    workflowCfgBuilder.setExpiry(120000);

    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.MINUTE, cal.get(Calendar.MINUTE) + delayStart / 60);
    cal.set(Calendar.SECOND, cal.get(Calendar.SECOND) + delayStart % 60);
    cal.set(Calendar.MILLISECOND, 0);
    workflowCfgBuilder.setScheduleConfig(ScheduleConfig.oneTimeDelayedStart(cal.getTime()));

    if (failureThreshold > 0) {
      workflowCfgBuilder.setFailureThreshold(failureThreshold);
    }
    return new JobQueue.Builder(jobQueueName).setWorkflowConfig(workflowCfgBuilder.build());
  }

  public static JobQueue.Builder buildJobQueue(String jobQueueName) {
    return buildJobQueue(jobQueueName, 0, 0);
  }
}
