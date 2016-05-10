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

import java.util.Calendar;
import java.util.concurrent.TimeUnit;

import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.ScheduleConfig;
import org.apache.helix.task.TargetState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

public class TestUpdateWorkflow extends TaskTestBase {
  private static final Logger LOG = Logger.getLogger(TestUpdateWorkflow.class);

  @Test
  public void testUpdateRunningQueue() throws InterruptedException {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue queue = createDefaultRecurrentJobQueue(queueName, 2);
    _driver.start(queue);

    WorkflowContext wCtx = TaskTestUtil.pollForWorkflowContext(_driver, queueName);

    WorkflowConfig workflowConfig = _driver.getWorkflowConfig(queueName);
    WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(workflowConfig);

    Calendar startTime = Calendar.getInstance();
    startTime.set(Calendar.SECOND, startTime.get(Calendar.SECOND) + 1);

    ScheduleConfig scheduleConfig =
        ScheduleConfig.recurringFromDate(startTime.getTime(), TimeUnit.MINUTES, 2);

    configBuilder.setScheduleConfig(scheduleConfig);

    // ensure current schedule is started
    String scheduledQueue = wCtx.getLastScheduledSingleWorkflow();
    _driver.pollForWorkflowState(scheduledQueue, TaskState.IN_PROGRESS);

    _driver.updateWorkflow(queueName, configBuilder.build());

    // ensure current schedule is completed
    _driver.pollForWorkflowState(scheduledQueue, TaskState.COMPLETED);

    Thread.sleep(1000);

    wCtx = TaskTestUtil.pollForWorkflowContext(_driver, queueName);
    scheduledQueue = wCtx.getLastScheduledSingleWorkflow();
    WorkflowConfig wCfg = _driver.getWorkflowConfig(scheduledQueue);

    Calendar configStartTime = Calendar.getInstance();
    configStartTime.setTime(wCfg.getStartTime());

    Assert.assertTrue(
        (startTime.get(Calendar.HOUR_OF_DAY) == configStartTime.get(Calendar.HOUR_OF_DAY) &&
            startTime.get(Calendar.MINUTE) == configStartTime.get(Calendar.MINUTE) &&
            startTime.get(Calendar.SECOND) == configStartTime.get(Calendar.SECOND)));
  }

  @Test
  public void testUpdateStoppedQueue() throws InterruptedException {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue queue = createDefaultRecurrentJobQueue(queueName, 2);
    _driver.start(queue);

    WorkflowContext wCtx = TaskTestUtil.pollForWorkflowContext(_driver, queueName);

    // ensure current schedule is started
    String scheduledQueue = wCtx.getLastScheduledSingleWorkflow();
    _driver.pollForWorkflowState(scheduledQueue, TaskState.IN_PROGRESS);

    _driver.stop(queueName);

    WorkflowConfig workflowConfig = _driver.getWorkflowConfig(queueName);
    Assert.assertEquals(workflowConfig.getTargetState(), TargetState.STOP);

    WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(workflowConfig);
    Calendar startTime = Calendar.getInstance();
    startTime.set(Calendar.SECOND, startTime.get(Calendar.SECOND) + 1);

    ScheduleConfig scheduleConfig =
        ScheduleConfig.recurringFromDate(startTime.getTime(), TimeUnit.MINUTES, 2);

    configBuilder.setScheduleConfig(scheduleConfig);

    _driver.updateWorkflow(queueName, configBuilder.build());

    workflowConfig = _driver.getWorkflowConfig(queueName);
    Assert.assertEquals(workflowConfig.getTargetState(), TargetState.STOP);

    _driver.resume(queueName);

    // ensure current schedule is completed
    _driver.pollForWorkflowState(scheduledQueue, TaskState.COMPLETED);

    Thread.sleep(1000);

    wCtx = TaskTestUtil.pollForWorkflowContext(_driver, queueName);
    scheduledQueue = wCtx.getLastScheduledSingleWorkflow();
    WorkflowConfig wCfg = _driver.getWorkflowConfig(scheduledQueue);

    Calendar configStartTime = Calendar.getInstance();
    configStartTime.setTime(wCfg.getStartTime());

    Assert.assertTrue(
        (startTime.get(Calendar.HOUR_OF_DAY) == configStartTime.get(Calendar.HOUR_OF_DAY) &&
            startTime.get(Calendar.MINUTE) == configStartTime.get(Calendar.MINUTE) &&
            startTime.get(Calendar.SECOND) == configStartTime.get(Calendar.SECOND)));
  }

  private JobQueue createDefaultRecurrentJobQueue(String queueName, int numJobs) {
    JobQueue.Builder queueBuild = TaskTestUtil.buildRecurrentJobQueue(queueName, 0, 600000);
    for (int i = 0; i <= numJobs; i++) {
      String targetPartition = (i == 0) ? "MASTER" : "SLAVE";

      JobConfig.Builder jobConfig =
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
              .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
              .setTargetPartitionStates(Sets.newHashSet(targetPartition));
      String jobName = targetPartition.toLowerCase() + "Job" + i;
      queueBuild.enqueueJob(jobName, jobConfig);
    }

    return queueBuild.build();
  }
}

