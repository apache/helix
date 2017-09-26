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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobDag;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.ScheduleConfig;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TestTaskRebalancerStopResume extends TaskTestBase {
  private static final Logger LOG = Logger.getLogger(TestTaskRebalancerStopResume.class);
  private static final String TIMEOUT_CONFIG = "Timeout";
  private static final String JOB_RESOURCE = "SomeJob";

  @Test public void stopAndResume() throws Exception {
    Map<String, String> commandConfig = ImmutableMap.of(TIMEOUT_CONFIG, String.valueOf(100));

    JobConfig.Builder jobBuilder =
        JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG);
    jobBuilder.setJobCommandConfigMap(commandConfig);
    Workflow flow =
        WorkflowGenerator.generateSingleJobWorkflowBuilder(JOB_RESOURCE, jobBuilder).build();

    LOG.info("Starting flow " + flow.getName());
    _driver.start(flow);
    _driver.pollForWorkflowState(JOB_RESOURCE, TaskState.IN_PROGRESS);

    LOG.info("Pausing job");
    _driver.stop(JOB_RESOURCE);
    _driver.pollForWorkflowState(JOB_RESOURCE, TaskState.STOPPED);

    LOG.info("Resuming job");
    _driver.resume(JOB_RESOURCE);
    _driver.pollForWorkflowState(JOB_RESOURCE, TaskState.COMPLETED);
  }

  @Test
  public void stopAndResumeWorkflow() throws Exception {
    String workflow = "SomeWorkflow";
    Workflow flow = WorkflowGenerator.generateDefaultRepeatedJobWorkflowBuilder(workflow).build();

    LOG.info("Starting flow " + workflow);
    _driver.start(flow);
    _driver.pollForWorkflowState(workflow, TaskState.IN_PROGRESS);

    LOG.info("Pausing workflow");
    _driver.stop(workflow);
    _driver.pollForWorkflowState(workflow, TaskState.STOPPED);

    LOG.info("Resuming workflow");
    _driver.resume(workflow);
    _driver.pollForWorkflowState(workflow, TaskState.COMPLETED);
  }

  @Test
  public void stopAndResumeNamedQueue() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue queue = new JobQueue.Builder(queueName).build();
    _driver.createQueue(queue);

    // Enqueue jobs
    Set<String> master = Sets.newHashSet("MASTER");
    JobConfig.Builder job1 =
        new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
            .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB).setTargetPartitionStates(master);
    String job1Name = "masterJob";
    LOG.info("Enqueuing job: " + job1Name);
    _driver.enqueueJob(queueName, job1Name, job1);

    Set<String> slave = Sets.newHashSet("SLAVE");
    JobConfig.Builder job2 =
        new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
            .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB).setTargetPartitionStates(slave);
    String job2Name = "slaveJob";
    LOG.info("Enqueuing job: " + job2Name);
    _driver.enqueueJob(queueName, job2Name, job2);

    String namespacedJob1 = String.format("%s_%s", queueName,  job1Name);
    _driver.pollForJobState(queueName, namespacedJob1, TaskState.IN_PROGRESS);

    // stop job1
    LOG.info("Pausing job-queue: " + queueName);
    _driver.stop(queueName);
    _driver.pollForJobState(queueName, namespacedJob1, TaskState.STOPPED);
    _driver.pollForWorkflowState(queueName, TaskState.STOPPED);

    // Ensure job2 is not started
    TimeUnit.MILLISECONDS.sleep(200);
    String namespacedJob2 = String.format("%s_%s", queueName, job2Name);
    TaskTestUtil.pollForEmptyJobState(_driver, queueName, job2Name);

    LOG.info("Resuming job-queue: " + queueName);
    _driver.resume(queueName);

    // Ensure successful completion
    _driver.pollForJobState(queueName, namespacedJob1, TaskState.COMPLETED);
    _driver.pollForJobState(queueName, namespacedJob2, TaskState.COMPLETED);
    JobContext masterJobContext = _driver.getJobContext(namespacedJob1);
    JobContext slaveJobContext = _driver.getJobContext(namespacedJob2);

    // Ensure correct ordering
    long job1Finish = masterJobContext.getFinishTime();
    long job2Start = slaveJobContext.getStartTime();
    Assert.assertTrue(job2Start >= job1Finish);

    // Flush queue and check cleanup
    LOG.info("Flusing job-queue: " + queueName);
    _driver.flushQueue(queueName);

    verifyJobDeleted(queueName, namespacedJob1);
    verifyJobDeleted(queueName, namespacedJob2);
    verifyJobNotInQueue(queueName, namespacedJob1);
    verifyJobNotInQueue(queueName, namespacedJob2);
  }


  @Test
  public void stopDeleteJobAndResumeNamedQueue() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuilder = TaskTestUtil.buildJobQueue(queueName);

    // Create and Enqueue jobs
    List<String> currentJobNames = new ArrayList<String>();
    for (int i = 0; i <= 4; i++) {
      String targetPartition = (i == 0) ? "MASTER" : "SLAVE";

      JobConfig.Builder jobBuilder =
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
              .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
              .setTargetPartitionStates(Sets.newHashSet(targetPartition));
      String jobName = targetPartition.toLowerCase() + "Job" + i;
      LOG.info("Enqueuing job: " + jobName);
      queueBuilder.enqueueJob(jobName, jobBuilder);
      currentJobNames.add(i, jobName);
    }

    _driver.createQueue(queueBuilder.build());

    // ensure job 1 is started before deleting it
    String deletedJob1 = currentJobNames.get(0);
    String namedSpaceDeletedJob1 = String.format("%s_%s", queueName, deletedJob1);
    _driver.pollForJobState(queueName, namedSpaceDeletedJob1, TaskState.IN_PROGRESS);

    // stop the queue
    LOG.info("Pausing job-queue: " + queueName);
    _driver.stop(queueName);
    _driver.pollForJobState(queueName, namedSpaceDeletedJob1, TaskState.STOPPED);
    _driver.pollForWorkflowState(queueName, TaskState.STOPPED);

    // delete the in-progress job (job 1) and verify it being deleted
    _driver.deleteJob(queueName, deletedJob1);
    verifyJobDeleted(queueName, namedSpaceDeletedJob1);

    LOG.info("Resuming job-queue: " + queueName);
    _driver.resume(queueName);

    // ensure job 2 is started
    _driver.pollForJobState(queueName, String.format("%s_%s", queueName, currentJobNames.get(1)),
        TaskState.IN_PROGRESS);

    // stop the queue
    LOG.info("Pausing job-queue: " + queueName);
    _driver.stop(queueName);
    _driver.pollForJobState(queueName, String.format("%s_%s", queueName, currentJobNames.get(1)),
        TaskState.STOPPED);
    _driver.pollForWorkflowState(queueName, TaskState.STOPPED);

    // Ensure job 3 is not started before deleting it
    String deletedJob2 = currentJobNames.get(2);
    String namedSpaceDeletedJob2 = String.format("%s_%s", queueName, deletedJob2);
    TaskTestUtil.pollForEmptyJobState(_driver, queueName, namedSpaceDeletedJob2);

    // delete not-started job (job 3) and verify it being deleted
    _driver.deleteJob(queueName, deletedJob2);
    verifyJobDeleted(queueName, namedSpaceDeletedJob2);

    LOG.info("Resuming job-queue: " + queueName);
    _driver.resume(queueName);

    currentJobNames.remove(deletedJob1);
    currentJobNames.remove(deletedJob2);

    // add job 3 back
    JobConfig.Builder job =
        new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
            .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB).setTargetPartitionStates(Sets.newHashSet("SLAVE"));
    LOG.info("Enqueuing job: " + deletedJob2);
    _driver.enqueueJob(queueName, deletedJob2, job);
    currentJobNames.add(deletedJob2);

    // Ensure the jobs left are successful completed in the correct order
    long preJobFinish = 0;
    for (int i = 0; i < currentJobNames.size(); i++) {
      String namedSpaceJobName = String.format("%s_%s", queueName, currentJobNames.get(i));
      _driver.pollForJobState(queueName, namedSpaceJobName, TaskState.COMPLETED);

      JobContext jobContext = _driver.getJobContext(namedSpaceJobName);
      long jobStart = jobContext.getStartTime();
      Assert.assertTrue(jobStart >= preJobFinish);
      preJobFinish = jobContext.getFinishTime();
    }

    // Flush queue
    LOG.info("Flusing job-queue: " + queueName);
    _driver.flushQueue(queueName);

    // TODO: Use TestHelper.verify() instead of waiting here.
    TimeUnit.MILLISECONDS.sleep(5000);

    // verify the cleanup
    for (int i = 0; i < currentJobNames.size(); i++) {
      String namedSpaceJobName = String.format("%s_%s", queueName, currentJobNames.get(i));
      verifyJobDeleted(queueName, namedSpaceJobName);
      verifyJobNotInQueue(queueName, namedSpaceJobName);
    }
  }

  @Test
  public void stopDeleteJobAndResumeRecurrentQueue() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuilder = TaskTestUtil.buildRecurrentJobQueue(queueName);

    // Create and Enqueue jobs
    List<String> currentJobNames = new ArrayList<String>();
    Map<String, String> commandConfig = ImmutableMap.of(TIMEOUT_CONFIG, String.valueOf(500));
    for (int i = 0; i <= 4; i++) {
      String targetPartition = (i == 0) ? "MASTER" : "SLAVE";

      JobConfig.Builder job =
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
              .setJobCommandConfigMap(commandConfig)
              .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
              .setTargetPartitionStates(Sets.newHashSet(targetPartition));
      String jobName = targetPartition.toLowerCase() + "Job" + i;
      LOG.info("Enqueuing job: " + jobName);
      queueBuilder.enqueueJob(jobName, job);
      currentJobNames.add(i, jobName);
    }

    _driver.createQueue(queueBuilder.build());

    WorkflowContext wCtx = TaskTestUtil.pollForWorkflowContext(_driver, queueName);
    String scheduledQueue = wCtx.getLastScheduledSingleWorkflow();

    // ensure job 1 is started before deleting it
    String deletedJob1 = currentJobNames.get(0);
    String namedSpaceDeletedJob1 = String.format("%s_%s", scheduledQueue, deletedJob1);
    _driver.pollForJobState(scheduledQueue, namedSpaceDeletedJob1, TaskState.IN_PROGRESS);

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
        String.format("%s_%s", scheduledQueue, currentJobNames.get(1)), TaskState.IN_PROGRESS);

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

    List<JobConfig.Builder> jobs = new ArrayList<JobConfig.Builder>();
    List<String> jobNames = new ArrayList<String>();
    Map<String, String> commandConfig = ImmutableMap.of(TIMEOUT_CONFIG, String.valueOf(500));


    int JOB_COUNTS = 3;
    for (int i = 0; i < JOB_COUNTS; i++) {
      String targetPartition = (i == 0) ? "MASTER" : "SLAVE";
      String jobName = targetPartition.toLowerCase() + "Job" + i;

      JobConfig.Builder job = new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
          .setJobCommandConfigMap(commandConfig).setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
          .setTargetPartitionStates(Sets.newHashSet(targetPartition));
      jobs.add(job);
      jobNames.add(jobName);
    }

    for (int i = 0; i < JOB_COUNTS -1; i++) {
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

    // remove the last job
    _driver.deleteJob(queueName, jobNames.get(JOB_COUNTS - 1));

    // verify
    verifyJobDeleted(queueName, String.format("%s_%s", scheduledQueue, jobNames.get(JOB_COUNTS - 1)));
  }

  @Test
  public void stopAndDeleteQueue() throws Exception {
    final String queueName = TestHelper.getTestMethodName();

    // Create a queue
    System.out.println("START " + queueName + " at " + new Date(System.currentTimeMillis()));
    WorkflowConfig wfCfg = new WorkflowConfig.Builder(queueName).setExpiry(2, TimeUnit.MINUTES).build();
    JobQueue qCfg = new JobQueue.Builder(queueName).fromMap(wfCfg.getResourceConfigMap()).build();
    _driver.createQueue(qCfg);

    // Enqueue 2 jobs
    Set<String> master = Sets.newHashSet("MASTER");
    JobConfig.Builder job1 =
        new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
            .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB).setTargetPartitionStates(master);
    String job1Name = "masterJob";
    LOG.info("Enqueuing job1: " + job1Name);
    _driver.enqueueJob(queueName, job1Name, job1);

    Set<String> slave = Sets.newHashSet("SLAVE");
    JobConfig.Builder job2 =
        new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
            .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB).setTargetPartitionStates(slave);
    String job2Name = "slaveJob";
    LOG.info("Enqueuing job2: " + job2Name);
    _driver.enqueueJob(queueName, job2Name, job2);

    String namespacedJob1 = String.format("%s_%s", queueName,  job1Name);
    _driver.pollForJobState(queueName, namespacedJob1, TaskState.COMPLETED);

    String namespacedJob2 = String.format("%s_%s", queueName,  job2Name);
    _driver.pollForJobState(queueName, namespacedJob2, TaskState.COMPLETED);

    // Stop queue
    _driver.stop(queueName);

    boolean result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    // Delete queue
    _driver.delete(queueName);

    // Wait until all status are cleaned up
    result = TestHelper.verify(new TestHelper.Verifier() {
      @Override public boolean verify() throws Exception {
        HelixDataAccessor accessor = _manager.getHelixDataAccessor();
        PropertyKey.Builder keyBuilder = accessor.keyBuilder();

        // check paths for resource-config, ideal-state, external-view, property-store
        List<String> paths
            = Lists.newArrayList(keyBuilder.resourceConfigs().getPath(),
            keyBuilder.idealStates().getPath(),
            keyBuilder.externalViews().getPath(),
            PropertyPathBuilder.propertyStore(CLUSTER_NAME) + TaskConstants.REBALANCER_CONTEXT_ROOT);

        for (String path : paths) {
          List<String> childNames = accessor.getBaseDataAccessor().getChildNames(path, 0);
          for (String childName : childNames) {
            if (childName.startsWith(queueName)) {
              return false;
            }
          }
        }

        return true;
      }
    }, 30 * 1000);
    Assert.assertTrue(result);

    System.out.println("END " + queueName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testStopWorkflowInStoppingState() throws InterruptedException {
    final String workflowName = TestHelper.getTestMethodName();

    // Create a workflow
    Workflow.Builder builder = new Workflow.Builder(workflowName);

    // Add 2 jobs
    Map<String, String> jobCommandConfigMap = new HashMap<String, String>();
    jobCommandConfigMap.put(MockTask.TIMEOUT_CONFIG, "1000000");
    jobCommandConfigMap.put(MockTask.NOT_ALLOW_TO_CANCEL, String.valueOf(true));
    List<TaskConfig> taskConfigs = ImmutableList
        .of(new TaskConfig.Builder().setCommand(MockTask.TASK_COMMAND).setTaskId("testTask")
            .build());
    JobConfig.Builder job1 = new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
   .addTaskConfigs(taskConfigs)
        .setJobCommandConfigMap(jobCommandConfigMap);
    String job1Name = "Job1";

    JobConfig.Builder job2 =
        new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND).addTaskConfigs(taskConfigs);
    String job2Name = "Job2";

    builder.addJob(job1Name, job1);
    builder.addJob(job2Name, job2);

    _driver.start(builder.build());
    Thread.sleep(2000);
    _driver.stop(workflowName);
    _driver.pollForWorkflowState(workflowName, TaskState.STOPPING);

    // Expect job and workflow stuck in STOPPING state.
    WorkflowContext workflowContext = _driver.getWorkflowContext(workflowName);
    Assert.assertEquals(
        workflowContext.getJobState(TaskUtil.getNamespacedJobName(workflowName, job1Name)),
        TaskState.STOPPING);
  }

  private void verifyJobDeleted(String queueName, String jobName) throws Exception {
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    Assert.assertNull(accessor.getProperty(keyBuilder.idealStates(jobName)), jobName + "'s idealstate has not been deleted!");
    Assert.assertNull(accessor.getProperty(keyBuilder.resourceConfig(jobName)), jobName + "'s resourceConfig has not been deleted!");
    TaskTestUtil.pollForEmptyJobState(_driver, queueName, jobName);
  }

  private void verifyJobNotInQueue(String queueName, String namedSpacedJobName) {
    WorkflowConfig workflowCfg = _driver.getWorkflowConfig(queueName);
    JobDag dag = workflowCfg.getJobDag();
    Assert.assertFalse(dag.getAllNodes().contains(namedSpacedJobName));
    Assert.assertFalse(dag.getChildrenToParents().containsKey(namedSpacedJobName));
    Assert.assertFalse(dag.getParentsToChildren().containsKey(namedSpacedJobName));
  }
}
