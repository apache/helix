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

import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixException;
import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class TestAddTask extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
    _numNodes = 3;
    super.beforeClass();
  }

  @AfterClass
  public void afterClass() throws Exception {
    super.afterClass();
  }

  @Test
  public void testAddWorkflowMissing() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    String jobName = "JOB0";
    TaskConfig task = new TaskConfig(null, null, null, null);
    try {
      _driver.addTask(task, workflowName, jobName);
      Assert.fail("Exception is expected because workflow config is missing");
    } catch (HelixException e) {
      // Helix Exception is expected because workflow config is missing
    }
  }

  @Test(dependsOnMethods = "testAddWorkflowMissing")
  public void testAddJobMissing() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    String jobName = "JOB0";

    Workflow.Builder workflowBuilder1 = new Workflow.Builder(workflowName);
    _driver.start(workflowBuilder1.build());

    // Make sure workflow config and context have been created
    Assert.assertTrue(TestHelper.verify(() -> {
      WorkflowConfig config = _driver.getWorkflowConfig(workflowName);
      WorkflowContext context = _driver.getWorkflowContext(workflowName);
      return (config != null && context != null);
    }, TestHelper.WAIT_DURATION));

    TaskConfig task = new TaskConfig(null, null, null, null);
    try {
      _driver.addTask(task, workflowName, jobName);
      Assert.fail("Exception is expected because job config is missing");
    } catch (HelixException e) {
      // Helix Exception is expected because job config is missing
    }
  }

  @Test(dependsOnMethods = "testAddJobMissing")
  public void testAddTaskToTargetedJob() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    String jobName = "JOB0";

    JobConfig.Builder jobBuilder1 = new JobConfig.Builder().setWorkflow(workflowName)
        .setNumberOfTasks(1).setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
        .setTargetPartitionStates(Sets.newHashSet("MASTER")).setNumConcurrentTasksPerInstance(100)
        .setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));

    Workflow.Builder workflowBuilder1 =
        new Workflow.Builder(workflowName).addJob(jobName, jobBuilder1);
    _driver.start(workflowBuilder1.build());

    // Make sure workflow config and context have been created
    Assert.assertTrue(TestHelper.verify(() -> {
      WorkflowConfig config = _driver.getWorkflowConfig(workflowName);
      WorkflowContext context = _driver.getWorkflowContext(workflowName);
      return (config != null && context != null);
    }, TestHelper.WAIT_DURATION));

    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, jobName),
        TaskState.IN_PROGRESS);

    TaskConfig task = new TaskConfig(null, null, null, null);
    try {
      _driver.addTask(task, workflowName, jobName);
      Assert.fail("Exception is expected because job is targeted");
    } catch (HelixException e) {
      // Helix Exception is expected because job is targeted
    }
    _driver.waitToStop(workflowName, TestHelper.WAIT_DURATION);
  }

  @Test(dependsOnMethods = "testAddTaskToTargetedJob")
  public void testAddTaskJobAndTaskCommand() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    String jobName = "JOB0";

    JobConfig.Builder jobBuilder1 = new JobConfig.Builder().setWorkflow(workflowName)
        .setNumberOfTasks(1).setNumConcurrentTasksPerInstance(100).setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));

    Workflow.Builder workflowBuilder1 =
        new Workflow.Builder(workflowName).addJob(jobName, jobBuilder1);
    _driver.start(workflowBuilder1.build());

    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, jobName),
        TaskState.IN_PROGRESS);

    // Make sure workflow config and context have been created
    Assert.assertTrue(TestHelper.verify(() -> {
      WorkflowConfig config = _driver.getWorkflowConfig(workflowName);
      WorkflowContext context = _driver.getWorkflowContext(workflowName);
      return (config != null && context != null);
    }, TestHelper.WAIT_DURATION));

    TaskConfig task = new TaskConfig("dummy", null, null, null);
    try {
      _driver.addTask(task, workflowName, jobName);
      Assert.fail("Exception is expected because job and task both have command field");
    } catch (HelixException e) {
      // Helix Exception is expected job config and new task have command field
    }
    _driver.waitToStop(workflowName, TestHelper.WAIT_DURATION);
  }

  @Test(dependsOnMethods = "testAddTaskJobAndTaskCommand")
  public void testAddTaskJobNotRunning() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    String jobName = "JOB0";

    JobConfig.Builder jobBuilder1 = new JobConfig.Builder().setWorkflow(workflowName)
        .setNumberOfTasks(1).setNumConcurrentTasksPerInstance(100).setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));

    Workflow.Builder workflowBuilder1 =
        new Workflow.Builder(workflowName).addJob(jobName, jobBuilder1);
    _driver.start(workflowBuilder1.build());

    // Make sure workflow config and context have been created
    Assert.assertTrue(TestHelper.verify(() -> {
      WorkflowConfig config = _driver.getWorkflowConfig(workflowName);
      WorkflowContext context = _driver.getWorkflowContext(workflowName);
      return (config != null && context != null);
    }, TestHelper.WAIT_DURATION));

    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, jobName),
        TaskState.COMPLETED);

    TaskConfig task = new TaskConfig(null, null, null, null);
    try {
      _driver.addTask(task, workflowName, jobName);
      Assert.fail("Exception is expected because job is not running");
    } catch (HelixException e) {
      // Helix Exception is expected because job id not running
    }
  }

  @Test(dependsOnMethods = "testAddTaskJobNotRunning")
  public void testAddTaskWithNullConfig() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    String jobName = "JOB0";

    JobConfig.Builder jobBuilder1 = new JobConfig.Builder().setWorkflow(workflowName)
        .setNumberOfTasks(1).setNumConcurrentTasksPerInstance(100).setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));

    Workflow.Builder workflowBuilder1 =
        new Workflow.Builder(workflowName).addJob(jobName, jobBuilder1);
    _driver.start(workflowBuilder1.build());

    // Make sure workflow config and context have been created
    Assert.assertTrue(TestHelper.verify(() -> {
      WorkflowConfig config = _driver.getWorkflowConfig(workflowName);
      WorkflowContext context = _driver.getWorkflowContext(workflowName);
      return (config != null && context != null);
    }, TestHelper.WAIT_DURATION));

    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, jobName),
        TaskState.IN_PROGRESS);

    try {
      _driver.addTask(null, workflowName, jobName);
      Assert.fail("Exception is expected because job is not running");
    } catch (IllegalArgumentException e) {
      // Helix Exception is expected because job id not running
    }
  }

  @Test(dependsOnMethods = "testAddTaskWithNullConfig")
  public void testAddTaskSuccessfully() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    String jobName = "JOB0";

    JobConfig.Builder jobBuilder1 = new JobConfig.Builder().setWorkflow(workflowName)
        .setNumberOfTasks(1).setNumConcurrentTasksPerInstance(100).setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));

    Workflow.Builder workflowBuilder1 =
        new Workflow.Builder(workflowName).addJob(jobName, jobBuilder1);
    _driver.start(workflowBuilder1.build());

    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, jobName),
        TaskState.IN_PROGRESS);

    // Add short running task
    Map<String, String> newTaskConfig =
        new HashMap<String, String>(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));
    TaskConfig task = new TaskConfig(null, newTaskConfig, null, null);
    _driver.addTask(task, workflowName, jobName);

    Assert.assertTrue(TestHelper.verify(() -> {
      JobContext jobContext =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName));
      TaskPartitionState state = jobContext.getPartitionState(1);
      return (jobContext != null && state == TaskPartitionState.COMPLETED);
    }, TestHelper.WAIT_DURATION));

    _driver.waitToStop(workflowName, TestHelper.WAIT_DURATION);
  }

  @Test(dependsOnMethods = "testAddTaskSuccessfully")
  public void testAddTaskTwice() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    String jobName = "JOB0";

    JobConfig.Builder jobBuilder1 = new JobConfig.Builder().setWorkflow(workflowName)
        .setNumberOfTasks(1).setNumConcurrentTasksPerInstance(100).setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));

    Workflow.Builder workflowBuilder1 =
        new Workflow.Builder(workflowName).addJob(jobName, jobBuilder1);
    _driver.start(workflowBuilder1.build());

    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, jobName),
        TaskState.IN_PROGRESS);

    // Add short running task
    Map<String, String> newTaskConfig =
        new HashMap<String, String>(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));
    TaskConfig task = new TaskConfig(null, newTaskConfig, null, null);
    _driver.addTask(task, workflowName, jobName);

    try {
      _driver.addTask(task, workflowName, jobName);
      Assert.fail("Exception is expected because task is being added multiple times");
    } catch (HelixException e) {
      // Helix Exception is expected because task is being added multiple times
    }

    Assert.assertTrue(TestHelper.verify(() -> {
      JobContext jobContext =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName));
      TaskPartitionState state = jobContext.getPartitionState(1);
      return (jobContext != null && state == TaskPartitionState.COMPLETED);
    }, TestHelper.WAIT_DURATION));

    _driver.waitToStop(workflowName, TestHelper.WAIT_DURATION);
  }
}
