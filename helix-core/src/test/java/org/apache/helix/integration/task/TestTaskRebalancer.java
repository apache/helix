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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobDag;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestTaskRebalancer extends TaskTestBase {
  @Test
  public void basic() throws Exception {
    basic(100);
  }

  @Test
  public void zeroTaskCompletionTime() throws Exception {
    basic(0);
  }

  @Test
  public void testExpiry() throws Exception {
    String jobName = "Expiry";
    long expiry = 1000;
    Map<String, String> commandConfig = ImmutableMap.of(MockTask.JOB_DELAY, String.valueOf(100));
    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG);
    jobBuilder.setJobCommandConfigMap(commandConfig);

    Workflow flow = WorkflowGenerator
            .generateSingleJobWorkflowBuilder(jobName, jobBuilder)
            .setExpiry(expiry).build();

    _driver.start(flow);
    _driver.pollForWorkflowState(jobName, TaskState.IN_PROGRESS);

    // Running workflow should have config and context viewable through accessor
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    PropertyKey workflowCfgKey = accessor.keyBuilder().resourceConfig(jobName);
    String workflowPropStoreKey =
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, jobName);

    // Ensure context and config exist
    Assert.assertTrue(_manager.getHelixPropertyStore().exists(workflowPropStoreKey,
        AccessOption.PERSISTENT));
    Assert.assertNotSame(accessor.getProperty(workflowCfgKey), null);

    // Wait for job to finish and expire
    _driver.pollForWorkflowState(jobName, TaskState.COMPLETED);
    Thread.sleep(expiry + 100);

    // Ensure workflow config and context were cleaned up by now
    Assert.assertFalse(_manager.getHelixPropertyStore().exists(workflowPropStoreKey,
        AccessOption.PERSISTENT));
    Assert.assertEquals(accessor.getProperty(workflowCfgKey), null);
  }

  private void basic(long jobCompletionTime) throws Exception {
    // We use a different resource name in each test method as a work around for a helix participant
    // bug where it does
    // not clear locally cached state when a resource partition is dropped. Once that is fixed we
    // should change these
    // tests to use the same resource name and implement a beforeMethod that deletes the task
    // resource.
    final String jobResource = "basic" + jobCompletionTime;
    Map<String, String> commandConfig =
        ImmutableMap.of(MockTask.JOB_DELAY, String.valueOf(jobCompletionTime));

    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG);
    jobBuilder.setJobCommandConfigMap(commandConfig);

    Workflow flow =
        WorkflowGenerator.generateSingleJobWorkflowBuilder(jobResource, jobBuilder).build();
    _driver.start(flow);

    // Wait for job completion
    _driver.pollForWorkflowState(jobResource, TaskState.COMPLETED);

    // Ensure all partitions are completed individually
    JobContext ctx = _driver.getJobContext(TaskUtil.getNamespacedJobName(jobResource));
    for (int i = 0; i < _numParitions; i++) {
      Assert.assertEquals(ctx.getPartitionState(i), TaskPartitionState.COMPLETED);
      Assert.assertEquals(ctx.getPartitionNumAttempts(i), 1);
    }
  }

  @Test public void partitionSet() throws Exception {
    final String jobResource = "partitionSet";
    ImmutableList<String> targetPartitions =
        ImmutableList.of("TestDB_1", "TestDB_2", "TestDB_3", "TestDB_5", "TestDB_8", "TestDB_13");

    // construct and submit our basic workflow
    Map<String, String> commandConfig = ImmutableMap.of(MockTask.JOB_DELAY, String.valueOf(100));

    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG);
    jobBuilder.setJobCommandConfigMap(commandConfig).setMaxAttemptsPerTask(1)
        .setTargetPartitions(targetPartitions);

    Workflow flow =
        WorkflowGenerator.generateSingleJobWorkflowBuilder(jobResource, jobBuilder).build();
    _driver.start(flow);

    // wait for job completeness/timeout
    _driver.pollForWorkflowState(jobResource, TaskState.COMPLETED);

    // see if resulting context completed successfully for our partition set
    String namespacedName = TaskUtil.getNamespacedJobName(jobResource);

    JobContext ctx = _driver.getJobContext(namespacedName);
    WorkflowContext workflowContext = _driver.getWorkflowContext(jobResource);
    Assert.assertNotNull(ctx);
    Assert.assertNotNull(workflowContext);
    Assert.assertEquals(workflowContext.getJobState(namespacedName), TaskState.COMPLETED);
    for (String pName : targetPartitions) {
      int i = ctx.getPartitionsByTarget().get(pName).get(0);
      Assert.assertEquals(ctx.getPartitionState(i), TaskPartitionState.COMPLETED);
      Assert.assertEquals(ctx.getPartitionNumAttempts(i), 1);
    }
  }

  @Test
  public void testRepeatedWorkflow() throws Exception {
    String workflowName = "SomeWorkflow";
    Workflow flow =
        WorkflowGenerator.generateDefaultRepeatedJobWorkflowBuilder(workflowName).build();
    new TaskDriver(_manager).start(flow);

    // Wait until the workflow completes
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);

    // Assert completion for all tasks within two minutes
    for (String task : flow.getJobConfigs().keySet()) {
      _driver.pollForJobState(workflowName, task, TaskState.COMPLETED);
    }
  }

  @Test public void timeouts() throws Exception {
    final String jobResource = "timeouts";

    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG);
    jobBuilder.setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG)
        .setMaxAttemptsPerTask(2).setTimeoutPerTask(100);

    Workflow flow =
        WorkflowGenerator.generateSingleJobWorkflowBuilder(jobResource, jobBuilder).build();
    _driver.start(flow);

    // Wait until the job reports failure.
    _driver.pollForWorkflowState(jobResource, TaskState.FAILED);

    // Check that all partitions timed out up to maxAttempts
    JobContext ctx = _driver.getJobContext(TaskUtil.getNamespacedJobName(jobResource));
    int maxAttempts = 0;
    boolean sawTimedoutTask = false;
    for (int i = 0; i < _numParitions; i++) {
      TaskPartitionState state = ctx.getPartitionState(i);
      if (state != null) {
        if (state == TaskPartitionState.TIMED_OUT) {
          sawTimedoutTask = true;
        }
        // At least one task timed out, other might be aborted due to job failure.
        Assert.assertTrue(
            state == TaskPartitionState.TIMED_OUT || state == TaskPartitionState.TASK_ABORTED);
        maxAttempts = Math.max(maxAttempts, ctx.getPartitionNumAttempts(i));
      }
    }
    Assert.assertTrue(sawTimedoutTask);
    Assert.assertEquals(maxAttempts, 2);
  }

  @Test
  public void testNamedQueue() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    JobQueue queue = new JobQueue.Builder(queueName).build();
    _driver.createQueue(queue);

    // Enqueue jobs
    Set<String> master = Sets.newHashSet("MASTER");
    Set<String> slave = Sets.newHashSet("SLAVE");
    JobConfig.Builder job1 =
        new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
            .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB).setTargetPartitionStates(master);
    JobConfig.Builder job2 =
        new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND)
            .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB).setTargetPartitionStates(slave);
    _driver.enqueueJob(queueName, "masterJob", job1);
    _driver.enqueueJob(queueName, "slaveJob", job2);

    // Ensure successful completion
    String namespacedJob1 = queueName + "_masterJob";
    String namespacedJob2 = queueName + "_slaveJob";
    _driver.pollForJobState(queueName, namespacedJob1, TaskState.COMPLETED);
    _driver.pollForJobState(queueName, namespacedJob2, TaskState.COMPLETED);
    JobContext masterJobContext = _driver.getJobContext(namespacedJob1);
    JobContext slaveJobContext = _driver.getJobContext(namespacedJob2);

    // Ensure correct ordering
    long job1Finish = masterJobContext.getFinishTime();
    long job2Start = slaveJobContext.getStartTime();
    Assert.assertTrue(job2Start >= job1Finish);

    // Flush queue and check cleanup
    _driver.flushQueue(queueName);
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Assert.assertNull(accessor.getProperty(keyBuilder.idealStates(namespacedJob1)));
    Assert.assertNull(accessor.getProperty(keyBuilder.resourceConfig(namespacedJob1)));
    Assert.assertNull(accessor.getProperty(keyBuilder.idealStates(namespacedJob2)));
    Assert.assertNull(accessor.getProperty(keyBuilder.resourceConfig(namespacedJob2)));
    WorkflowConfig workflowCfg = _driver.getWorkflowConfig(queueName);
    JobDag dag = workflowCfg.getJobDag();
    Assert.assertFalse(dag.getAllNodes().contains(namespacedJob1));
    Assert.assertFalse(dag.getAllNodes().contains(namespacedJob2));
    Assert.assertFalse(dag.getChildrenToParents().containsKey(namespacedJob1));
    Assert.assertFalse(dag.getChildrenToParents().containsKey(namespacedJob2));
    Assert.assertFalse(dag.getParentsToChildren().containsKey(namespacedJob1));
    Assert.assertFalse(dag.getParentsToChildren().containsKey(namespacedJob2));
  }
}
