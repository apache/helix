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

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.helix.TestHelper;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * This test contains cases when a workflow finish
 */
public class TestWorkflowTermination extends TaskTestBase {
  private final static String JOB_NAME = "TestJob";
  private final static String WORKFLOW_TYPE = "DEFAULT";
  private static final MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();

  @BeforeClass
  public void beforeClass() throws Exception {
    _numDbs = 1;
    _numNodes = 3;
    _numPartitions = 5;
    _numReplicas = 3;
    super.beforeClass();
  }

  private JobConfig.Builder createJobConfigBuilder(String workflow, boolean shouldJobFail,
      long timeoutMs) {
    String taskState = shouldJobFail ? TaskState.FAILED.name() : TaskState.COMPLETED.name();
    return new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
        .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
        .setWorkflow(workflow).setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, Long.toString(timeoutMs),
            MockTask.TASK_RESULT_STATUS, taskState));
  }

  @Test
  public void testWorkflowSucceed() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    long workflowExpiry = 2000;
    long timeout = 2000;
    JobConfig.Builder jobBuilder = createJobConfigBuilder(workflowName, false, 50);
    jobBuilder.setWorkflow(workflowName);
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName)
        .setWorkflowConfig(new WorkflowConfig.Builder(workflowName).setTimeout(timeout)
            .setWorkFlowType(WORKFLOW_TYPE).build())
        .addJob(JOB_NAME, jobBuilder).setExpiry(workflowExpiry);
    _driver.start(workflowBuilder.build());

    // Timeout is longer than job finish so workflow status should be COMPLETED
    _driver.pollForWorkflowState(workflowName, 5000L, TaskState.COMPLETED);
    WorkflowContext context = _driver.getWorkflowContext(workflowName);
    Assert.assertTrue(context.getFinishTime() - context.getStartTime() < timeout);

    // Workflow should be cleaned up after expiry
    Thread.sleep(workflowExpiry + 200);
    verifyWorkflowCleanup(workflowName, getJobNameToPoll(workflowName, JOB_NAME));

    ObjectName objectName = getWorkflowMBeanObjectName(workflowName);
    Assert.assertEquals((long) beanServer.getAttribute(objectName, "SuccessfulWorkflowCount"), 1);
    Assert
        .assertTrue((long) beanServer.getAttribute(objectName, "MaximumWorkflowLatencyGauge") > 0);
    Assert.assertTrue((long) beanServer.getAttribute(objectName, "WorkflowLatencyCount") > 0);

  }

  @Test
  public void testWorkflowRunningTimeout() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    String notStartedJobName = JOB_NAME + "-NotStarted";
    long workflowExpiry = 2000; // 2sec expiry time
    long timeout = 50;
    JobConfig.Builder jobBuilder = createJobConfigBuilder(workflowName, false, 5000);
    jobBuilder.setWorkflow(workflowName);

    // Create a workflow where job2 depends on job1. Workflow would timeout before job1 finishes
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName)
        .setWorkflowConfig(new WorkflowConfig.Builder(workflowName).setTimeout(timeout)
            .setWorkFlowType(WORKFLOW_TYPE).build())
        .addJob(JOB_NAME, jobBuilder).addJob(notStartedJobName, jobBuilder)
        .addParentChildDependency(JOB_NAME, notStartedJobName).setExpiry(workflowExpiry);

    _driver.start(workflowBuilder.build());

    _driver.pollForWorkflowState(workflowName, 10000L, TaskState.TIMED_OUT);

    // Running job should be marked as timeout
    // and job not started should not appear in workflow context
    _driver.pollForJobState(workflowName, getJobNameToPoll(workflowName, JOB_NAME), 10000L,
        TaskState.TIMED_OUT);

    WorkflowContext context = _driver.getWorkflowContext(workflowName);
    Assert.assertNull(context.getJobState(notStartedJobName));
    Assert.assertTrue(context.getFinishTime() - context.getStartTime() >= timeout);

    // Make sure config and context don't get deleted. workflowIsCleanedUp should be false
    boolean workflowIsCleanedUp = TestHelper.verify(() -> {
      WorkflowContext wCtx = _driver.getWorkflowContext(workflowName);
      WorkflowConfig wCfg = _driver.getWorkflowConfig(workflowName);
      return (wCtx == null || wCfg == null);
    }, TestHelper.WAIT_DURATION);
    Assert.assertFalse(workflowIsCleanedUp);
  }

  @Test
  public void testWorkflowPausedTimeout() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    long workflowExpiry = 2000; // 2sec expiry time
    long timeout = 5000;
    String notStartedJobName = JOB_NAME + "-NotStarted";

    JobConfig.Builder jobBuilder = createJobConfigBuilder(workflowName, false, 5000);
    jobBuilder.setWorkflow(workflowName);
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName)
        .setWorkflowConfig(new WorkflowConfig.Builder(workflowName).setTimeout(timeout)
            .setWorkFlowType(WORKFLOW_TYPE).build())
        .addJob(JOB_NAME, jobBuilder).addJob(notStartedJobName, jobBuilder)
        .addParentChildDependency(JOB_NAME, notStartedJobName).setExpiry(workflowExpiry);

    _driver.start(workflowBuilder.build());

    // Wait a bit for the job to get scheduled. Job runs for 100ms so this will very likely
    // to trigger a job stopped
    Thread.sleep(100);

    // Pause the queue
    _driver.waitToStop(workflowName, 10000L);

    _driver.pollForJobState(workflowName, getJobNameToPoll(workflowName, JOB_NAME), 10000L,
        TaskState.STOPPED);
    WorkflowContext context = _driver.getWorkflowContext(workflowName);
    Assert.assertNull(context.getJobState(notStartedJobName));

    _driver.pollForWorkflowState(workflowName, 10000L, TaskState.TIMED_OUT);

    context = _driver.getWorkflowContext(workflowName);
    Assert.assertTrue(context.getFinishTime() - context.getStartTime() >= timeout);

    // Make sure config and context don't get deleted. workflowIsCleanedUp should be false
    boolean workflowIsCleanedUp = TestHelper.verify(() -> {
      WorkflowContext wCtx = _driver.getWorkflowContext(workflowName);
      WorkflowConfig wCfg = _driver.getWorkflowConfig(workflowName);
      return (wCtx == null || wCfg == null);
    }, TestHelper.WAIT_DURATION);
    Assert.assertFalse(workflowIsCleanedUp);

  }

  @Test
  public void testJobQueueNotApplyTimeout() throws InterruptedException {
    String queueName = TestHelper.getTestMethodName();
    long timeout = 1000;
    // Make jobs run success
    JobConfig.Builder jobBuilder = createJobConfigBuilder(queueName, false, 10);
    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(queueName);
    jobQueue
        .setWorkflowConfig(new WorkflowConfig.Builder(queueName).setTimeout(timeout)
            .setWorkFlowType(WORKFLOW_TYPE).build())
        .enqueueJob(JOB_NAME, jobBuilder).enqueueJob(JOB_NAME + 1, jobBuilder);

    _driver.start(jobQueue.build());

    _driver.pollForJobState(queueName, TaskUtil.getNamespacedJobName(queueName, JOB_NAME),
        TaskState.COMPLETED);
    _driver.pollForJobState(queueName, TaskUtil.getNamespacedJobName(queueName, JOB_NAME + 1),
        TaskState.COMPLETED);

    Thread.sleep(timeout);

    // Verify that job queue is still in progress
    _driver.pollForWorkflowState(queueName, 10000L, TaskState.IN_PROGRESS);
  }

  @Test
  public void testWorkflowJobFail() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    String job1 = JOB_NAME + "1";
    String job2 = JOB_NAME + "2";
    String job3 = JOB_NAME + "3";
    String job4 = JOB_NAME + "4";
    long workflowExpiry = 10000;
    long timeout = 10000;

    JobConfig.Builder jobBuilder = createJobConfigBuilder(workflowName, false, 1);
    JobConfig.Builder failedJobBuilder = createJobConfigBuilder(workflowName, true, 1);

    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName)
        .setWorkflowConfig(new WorkflowConfig.Builder(workflowName).setWorkFlowType(WORKFLOW_TYPE)
            .setTimeout(timeout).setParallelJobs(4).setFailureThreshold(1).build())
        .addJob(job1, jobBuilder).addJob(job2, jobBuilder).addJob(job3, failedJobBuilder)
        .addJob(job4, jobBuilder).addParentChildDependency(job1, job2)
        .addParentChildDependency(job1, job3).addParentChildDependency(job2, job4)
        .addParentChildDependency(job3, job4).setExpiry(workflowExpiry);

    _driver.start(workflowBuilder.build());

    _driver.pollForWorkflowState(workflowName, 10000L, TaskState.FAILED);

    // Timeout is longer than fail time, so the failover should occur earlier
    WorkflowContext context = _driver.getWorkflowContext(workflowName);
    Assert.assertTrue(context.getFinishTime() - context.getStartTime() < timeout);

    // job1 will complete
    _driver.pollForJobState(workflowName, getJobNameToPoll(workflowName, job1), 10000L,
        TaskState.COMPLETED);

    // Possible race between 2 and 3 so it's likely for job2 to stay in either COMPLETED or ABORTED
    _driver.pollForJobState(workflowName, getJobNameToPoll(workflowName, job2), 10000L,
        TaskState.COMPLETED, TaskState.ABORTED);

    // job3 meant to fail
    _driver.pollForJobState(workflowName, getJobNameToPoll(workflowName, job3), 10000L,
        TaskState.FAILED);

    // because job4 has dependency over job3, it will fail as well
    _driver.pollForJobState(workflowName, getJobNameToPoll(workflowName, job4), 10000L,
        TaskState.FAILED);

    // Check MBean is updated
    ObjectName objectName = getWorkflowMBeanObjectName(workflowName);
    Assert.assertEquals((long) beanServer.getAttribute(objectName, "FailedWorkflowCount"), 1);

    // Make sure config and context don't get deleted. workflowIsCleanedUp should be false
    boolean workflowIsCleanedUp = TestHelper.verify(() -> {
      WorkflowContext wCtx = _driver.getWorkflowContext(workflowName);
      WorkflowConfig wCfg = _driver.getWorkflowConfig(workflowName);
      return (wCtx == null || wCfg == null);
    }, TestHelper.WAIT_DURATION);
    Assert.assertFalse(workflowIsCleanedUp);
  }

  private void verifyWorkflowCleanup(String workflowName, String... jobNames) {
    Assert.assertNull(_driver.getWorkflowConfig(workflowName));
    Assert.assertNull(_driver.getWorkflowContext(workflowName));
    for (String job : jobNames) {
      Assert.assertNull(_driver.getJobConfig(job));
      Assert.assertNull(_driver.getJobContext(job));
    }
  }

  private static String getJobNameToPoll(String workflowName, String jobName) {
    return String.format("%s_%s", workflowName, jobName);
  }

  private ObjectName getWorkflowMBeanObjectName(String workflowName)
      throws MalformedObjectNameException {
    return new ObjectName(String.format("%s:%s=%s, %s=%s", MonitorDomainNames.ClusterStatus.name(),
        "cluster", CLUSTER_NAME, "workflowType", WORKFLOW_TYPE));
  }
}
