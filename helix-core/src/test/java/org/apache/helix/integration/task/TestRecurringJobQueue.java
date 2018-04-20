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
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TargetState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class TestRecurringJobQueue extends TaskTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestRecurringJobQueue.class);

  @Test
  public void deleteRecreateRecurrentQueue() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuild = TaskTestUtil.buildRecurrentJobQueue(queueName);
    List<String> currentJobNames = createAndEnqueueJob(queueBuild, 2);

    _driver.start(queueBuild.build());

    WorkflowContext wCtx = TaskTestUtil.pollForWorkflowContext(_driver, queueName);

    // ensure job 1 is started before stop it
    String scheduledQueue = wCtx.getLastScheduledSingleWorkflow();
    String namedSpaceJob1 = String.format("%s_%s", scheduledQueue, currentJobNames.get(0));
    _driver.pollForJobState(scheduledQueue, namedSpaceJob1, TaskState.IN_PROGRESS);

    _driver.stop(queueName);
    _driver.delete(queueName);
    Thread.sleep(500);

    JobQueue.Builder queueBuilder = TaskTestUtil.buildRecurrentJobQueue(queueName, 5);
    currentJobNames.clear();
    currentJobNames = createAndEnqueueJob(queueBuilder, 2);

    _driver.createQueue(queueBuilder.build());


    wCtx = TaskTestUtil.pollForWorkflowContext(_driver, queueName);

    // ensure jobs are started and completed
    scheduledQueue = wCtx.getLastScheduledSingleWorkflow();
    namedSpaceJob1 = String.format("%s_%s", scheduledQueue, currentJobNames.get(0));
    _driver.pollForJobState(scheduledQueue, namedSpaceJob1, TaskState.COMPLETED);

    scheduledQueue = wCtx.getLastScheduledSingleWorkflow();
    String namedSpaceJob2 = String.format("%s_%s", scheduledQueue, currentJobNames.get(1));
    _driver.pollForJobState(scheduledQueue, namedSpaceJob2, TaskState.COMPLETED);
  }

  @Test
  public void stopDeleteJobAndResumeRecurrentQueue() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuilder = TaskTestUtil.buildRecurrentJobQueue(queueName, 5);

    // Create and Enqueue jobs
    Map<String, String> commandConfig = ImmutableMap.of(MockTask.JOB_DELAY, String.valueOf(500));
    Thread.sleep(100);
    List<String> currentJobNames = createAndEnqueueJob(queueBuilder, 5);
    _driver.createQueue(queueBuilder.build());

    WorkflowContext wCtx = TaskTestUtil.pollForWorkflowContext(_driver, queueName);
    String scheduledQueue = wCtx.getLastScheduledSingleWorkflow();

    // ensure job 1 is started before deleting it
    String deletedJob1 = currentJobNames.get(0);
    String namedSpaceDeletedJob1 = String.format("%s_%s", scheduledQueue, deletedJob1);
    _driver.pollForJobState(scheduledQueue, namedSpaceDeletedJob1, TaskState.IN_PROGRESS,
        TaskState.COMPLETED);

    // stop the queue
    LOG.info("Pausing job-queue: " + scheduledQueue);
    _driver.stop(queueName);
    _driver.pollForJobState(scheduledQueue, namedSpaceDeletedJob1, TaskState.STOPPED);
    _driver.pollForWorkflowState(scheduledQueue, TaskState.STOPPED);

    // delete the in-progress job (job 1) and verify it being deleted
    _driver.deleteJob(queueName, deletedJob1);
    verifyJobDeleted(queueName, namedSpaceDeletedJob1);
    verifyJobDeleted(scheduledQueue, namedSpaceDeletedJob1);

    LOG.info("Resuming job-queue: " + queueName);
    _driver.resume(queueName);

    // ensure job 2 is started
    _driver.pollForJobState(scheduledQueue,
        String.format("%s_%s", scheduledQueue, currentJobNames.get(1)), TaskState.IN_PROGRESS,
        TaskState.COMPLETED);

    // stop the queue
    LOG.info("Pausing job-queue: " + queueName);
    _driver.stop(queueName);
    _driver.pollForJobState(scheduledQueue,
        String.format("%s_%s", scheduledQueue, currentJobNames.get(1)), TaskState.STOPPED);
    _driver.pollForWorkflowState(scheduledQueue, TaskState.STOPPED);

    // Ensure job 3 is not started before deleting it
    String deletedJob2 = currentJobNames.get(2);
    String namedSpaceDeletedJob2 = String.format("%s_%s", scheduledQueue, deletedJob2);
    TaskTestUtil.pollForEmptyJobState(_driver, scheduledQueue, namedSpaceDeletedJob2);

    // delete not-started job (job 3) and verify it being deleted
    _driver.deleteJob(queueName, deletedJob2);
    verifyJobDeleted(queueName, namedSpaceDeletedJob2);
    verifyJobDeleted(scheduledQueue, namedSpaceDeletedJob2);

    LOG.info("Resuming job-queue: " + queueName);
    _driver.resume(queueName);

    // Ensure the jobs left are successful completed in the correct order
    currentJobNames.remove(deletedJob1);
    currentJobNames.remove(deletedJob2);
    long preJobFinish = 0;
    for (int i = 0; i < currentJobNames.size(); i++) {
      String namedSpaceJobName = String.format("%s_%s", scheduledQueue, currentJobNames.get(i));
      _driver.pollForJobState(scheduledQueue, namedSpaceJobName, TaskState.COMPLETED);

      JobContext jobContext = _driver.getJobContext(namedSpaceJobName);
      long jobStart = jobContext.getStartTime();
      Assert.assertTrue(jobStart >= preJobFinish);
      preJobFinish = jobContext.getFinishTime();
    }
    // verify the job is not there for the next recurrence of queue schedule
  }

  @Test
  public void deleteJobFromRecurrentQueueNotStarted() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuilder = TaskTestUtil.buildRecurrentJobQueue(queueName);

    // create jobs
    List<JobConfig.Builder> jobs = new ArrayList<JobConfig.Builder>();
    List<String> jobNames = new ArrayList<String>();
    Map<String, String> commandConfig = ImmutableMap.of(MockTask.JOB_DELAY, String.valueOf(500));

    final int JOB_COUNTS = 3;
    for (int i = 0; i < JOB_COUNTS; i++) {
      String targetPartition = (i == 0) ? "MASTER" : "SLAVE";

      JobConfig.Builder job = new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
          .setJobCommandConfigMap(commandConfig).setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
          .setTargetPartitionStates(Sets.newHashSet(targetPartition));
      jobs.add(job);
      jobNames.add(targetPartition.toLowerCase() + "Job" + i);
    }

    // enqueue all jobs except last one
    for (int i = 0; i < JOB_COUNTS - 1; ++i) {
      LOG.info("Enqueuing job: " + jobNames.get(i));
      queueBuilder.enqueueJob(jobNames.get(i), jobs.get(i));
    }

    _driver.createQueue(queueBuilder.build());

    String currentLastJob = jobNames.get(JOB_COUNTS - 2);

    WorkflowContext wCtx = TaskTestUtil.pollForWorkflowContext(_driver, queueName);
    String scheduledQueue = wCtx.getLastScheduledSingleWorkflow();

    // ensure all jobs are finished
    String namedSpaceJob = String.format("%s_%s", scheduledQueue, currentLastJob);
    _driver.pollForJobState(scheduledQueue, namedSpaceJob, TaskState.COMPLETED);

    // enqueue the last job
    LOG.info("Enqueuing job: " + jobNames.get(JOB_COUNTS - 1));
    _driver.enqueueJob(queueName, jobNames.get(JOB_COUNTS - 1), jobs.get(JOB_COUNTS - 1));
    _driver.stop(queueName);

    // remove the last job
    _driver.deleteJob(queueName, jobNames.get(JOB_COUNTS - 1));

    // verify
    verifyJobDeleted(queueName,
        String.format("%s_%s", scheduledQueue, jobNames.get(JOB_COUNTS - 1)));
  }

  @Test
  public void testCreateStoppedQueue() throws InterruptedException {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuild = TaskTestUtil.buildRecurrentJobQueue(queueName, 0, 600000,
        TargetState.STOP);
    createAndEnqueueJob(queueBuild, 2);

    _driver.createQueue(queueBuild.build());
    WorkflowConfig workflowConfig = _driver.getWorkflowConfig(queueName);
    Assert.assertEquals(workflowConfig.getTargetState(), TargetState.STOP);

    _driver.resume(queueName);

    //TaskTestUtil.pollForWorkflowState(_driver, queueName, );
    WorkflowContext wCtx = TaskTestUtil.pollForWorkflowContext(_driver, queueName);

    // ensure current schedule is started
    String scheduledQueue = wCtx.getLastScheduledSingleWorkflow();
    _driver.pollForWorkflowState(scheduledQueue, TaskState.COMPLETED);
  }

  @Test
  public void testDeletingRecurrentQueueWithHistory() throws Exception {
    final String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuild = TaskTestUtil.buildRecurrentJobQueue(queueName, 0, 60,
        TargetState.STOP);
    createAndEnqueueJob(queueBuild, 2);

    _driver.createQueue(queueBuild.build());
    WorkflowConfig workflowConfig = _driver.getWorkflowConfig(queueName);
    Assert.assertEquals(workflowConfig.getTargetState(), TargetState.STOP);

    _driver.resume(queueName);

    WorkflowContext wCtx;
    // wait until at least 2 workflows are scheduled based on template queue
    do {
      Thread.sleep(60000);
      wCtx = TaskTestUtil.pollForWorkflowContext(_driver, queueName);
    } while (wCtx.getScheduledWorkflows().size() < 2);

    // Stop recurring workflow
    _driver.stop(queueName);
    _driver.pollForWorkflowState(queueName, TaskState.STOPPED);

    // Record all scheduled workflows
    wCtx = TaskTestUtil.pollForWorkflowContext(_driver, queueName);
    List<String> scheduledWorkflows = new ArrayList<String>(wCtx.getScheduledWorkflows());
    final String lastScheduledWorkflow = wCtx.getLastScheduledSingleWorkflow();

    // Delete recurrent workflow
    _driver.delete(queueName);

    // Wait until recurrent workflow and the last scheduled workflow are cleaned up
    boolean result = TestHelper.verify(new TestHelper.Verifier() {
      @Override public boolean verify() throws Exception {
        WorkflowContext wCtx = _driver.getWorkflowContext(queueName);
        WorkflowContext lastWfCtx = _driver.getWorkflowContext(lastScheduledWorkflow);
        return (wCtx == null && lastWfCtx == null);
      }
    }, 5 * 1000);
    Assert.assertTrue(result);

    for (String scheduledWorkflow : scheduledWorkflows) {
      WorkflowContext scheduledWorkflowCtx = _driver.getWorkflowContext(scheduledWorkflow);
      WorkflowConfig scheduledWorkflowCfg = _driver.getWorkflowConfig(scheduledWorkflow);
      Assert.assertNull(scheduledWorkflowCtx);
      Assert.assertNull(scheduledWorkflowCfg);
    }
  }

  @Test
  public void testGetNoExistWorkflowConfig() {
    String randomName = "randomJob";
    WorkflowConfig workflowConfig = _driver.getWorkflowConfig(randomName);
    Assert.assertNull(workflowConfig);
    JobConfig jobConfig = _driver.getJobConfig(randomName);
    Assert.assertNull(jobConfig);
    WorkflowContext workflowContext = _driver.getWorkflowContext(randomName);
    Assert.assertNull(workflowContext);
    JobContext jobContext = _driver.getJobContext(randomName);
    Assert.assertNull(jobContext);
  }

  private void verifyJobDeleted(String queueName, String jobName) throws Exception {
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    Assert.assertNull(accessor.getProperty(keyBuilder.idealStates(jobName)));
    Assert.assertNull(accessor.getProperty(keyBuilder.resourceConfig(jobName)));
    TaskTestUtil.pollForEmptyJobState(_driver, queueName, jobName);
  }

  private List<String> createAndEnqueueJob(JobQueue.Builder queueBuild, int jobCount) {
    List<String> currentJobNames = new ArrayList<String>();
    for (int i = 0; i < jobCount; i++) {
      String targetPartition = (i == 0) ? "MASTER" : "SLAVE";

      JobConfig.Builder jobConfig =
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
              .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
              .setTargetPartitionStates(Sets.newHashSet(targetPartition));
      String jobName = targetPartition.toLowerCase() + "Job" + i;
      queueBuild.enqueueJob(jobName, jobConfig);
      currentJobNames.add(jobName);
    }
    Assert.assertEquals(currentJobNames.size(), jobCount);
    return currentJobNames;
  }
}

