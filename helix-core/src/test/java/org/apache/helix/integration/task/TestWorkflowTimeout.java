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

import java.util.Collections;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.helix.TestHelper;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestWorkflowTimeout extends TaskTestBase {
  private final static String JOB_NAME = "TestJob";
  private JobConfig.Builder _jobBuilder;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numDbs = 1;
    _numNodes = 3;
    _numPartitions = 5;
    _numReplicas = 3;
    super.beforeClass();

    // Create a non-stop job
    _jobBuilder = new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
        .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
        .setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));
  }

  @Test
  public void testWorkflowRunningTime() throws InterruptedException {
    String workflowName = TestHelper.getTestMethodName();
    _jobBuilder.setWorkflow(workflowName);
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName)
        .setWorkflowConfig(new WorkflowConfig.Builder(workflowName).setTimeout(1000).build())
        .addJob(JOB_NAME, _jobBuilder);
    _driver.start(workflowBuilder.build());

    _driver.pollForWorkflowState(workflowName, 10000L, TaskState.TIMED_OUT);
  }

  @Test
  public void testWorkflowPausedTimeout() throws InterruptedException {
    String workflowName = TestHelper.getTestMethodName();
    _jobBuilder.setWorkflow(workflowName);
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName)
        .setWorkflowConfig(new WorkflowConfig.Builder(workflowName).setTimeout(5000).build())
        .addJob(JOB_NAME, _jobBuilder);

    _driver.start(workflowBuilder.build());
    // Pause the queue
    _driver.waitToStop(workflowName, 10000L);

    _driver.pollForWorkflowState(workflowName, 10000L, TaskState.TIMED_OUT);
  }

  @Test
  public void testJobQueueNotApplyTimeout() throws InterruptedException {
    String queueName = TestHelper.getTestMethodName();
    // Make jobs run success
    _jobBuilder.setWorkflow(queueName).setJobCommandConfigMap(Collections.EMPTY_MAP);
    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(queueName);
    jobQueue.setWorkflowConfig(new WorkflowConfig.Builder(queueName).setTimeout(1000).build())
        .enqueueJob(JOB_NAME, _jobBuilder).enqueueJob(JOB_NAME + 1, _jobBuilder);

    _driver.start(jobQueue.build());

    _driver.pollForJobState(queueName, TaskUtil.getNamespacedJobName(queueName, JOB_NAME),
        TaskState.COMPLETED);
    _driver.pollForJobState(queueName, TaskUtil.getNamespacedJobName(queueName, JOB_NAME + 1),
        TaskState.COMPLETED);

    // Add back the config
    _jobBuilder.setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));
  }

  @Test
  public void testWorkflowTimeoutWhenWorkflowCompleted() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    long expiry = 2000L;
    _jobBuilder.setWorkflow(workflowName);
    _jobBuilder.setJobCommandConfigMap(Collections.<String, String> emptyMap());
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName)
        .setWorkflowConfig(new WorkflowConfig.Builder(workflowName).setTimeout(0).build())
        .addJob(JOB_NAME, _jobBuilder).setExpiry(expiry);

    // Since workflow's Timeout is 0, the workflow goes to TIMED_OUT state right away
    long startTime = System.currentTimeMillis();
    _driver.start(workflowBuilder.build());

    Assert.assertTrue(TestHelper.verify(() -> (_driver.getWorkflowConfig(workflowName) == null
        && _driver.getWorkflowContext(workflowName) == null), TestHelper.WAIT_DURATION));

    long cleanUpTime = System.currentTimeMillis();
    Assert.assertTrue(cleanUpTime - startTime >= expiry);
  }
}
