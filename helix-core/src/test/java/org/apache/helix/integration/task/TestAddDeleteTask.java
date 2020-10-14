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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixException;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestAddDeleteTask extends TaskTestBase {

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
  public void testAddDeleteTaskWorkflowMissing() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    String jobName = "JOB0";
    TaskConfig task = new TaskConfig(null, null, null, null);
    try {
      _driver.addTask(workflowName, jobName, task);
      Assert.fail("Exception is expected because workflow config is missing");
    } catch (IllegalArgumentException e) {
      // Helix Exception is expected because workflow config is missing
    }

    try {
      _driver.deleteTask(workflowName, jobName, task.getId());
      Assert.fail("Exception is expected because workflow config is missing");
    } catch (IllegalArgumentException e) {
      // Helix Exception is expected because workflow config is missing
    }
  }

  @Test(dependsOnMethods = "testAddDeleteTaskWorkflowMissing")
  public void testAddDeleteTaskJobMissing() throws Exception {
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
      _driver.addTask(workflowName, jobName, task);
      Assert.fail("Exception is expected because job config is missing");
    } catch (IllegalArgumentException e) {
      // Helix Exception is expected because job config is missing
    }

    try {
      _driver.deleteTask(workflowName, jobName, task.getId());
      Assert.fail("Exception is expected because job config is missing");
    } catch (IllegalArgumentException e) {
      // Helix Exception is expected because job config is missing
    }
  }

  @Test(dependsOnMethods = "testAddDeleteTaskJobMissing")
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
      _driver.addTask(workflowName, jobName, task);
      Assert.fail("Exception is expected because job is targeted");
    } catch (HelixException e) {
      // Helix Exception is expected because job is targeted
    }
    _driver.stop(workflowName);
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
      _driver.addTask(workflowName, jobName, task);
      Assert.fail("Exception is expected because job and task both have command field");
    } catch (HelixException e) {
      // Helix Exception is expected job config and new task have command field
    }
    _driver.stop(workflowName);
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
      _driver.addTask(workflowName, jobName, task);
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
      _driver.addTask(workflowName, jobName, null);
      Assert.fail("Exception is expected because job is not running");
    } catch (IllegalArgumentException e) {
      // Helix Exception is expected because job id not running
    }

    _driver.stop(workflowName);
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
    _driver.addTask(workflowName, jobName, task);

    Assert.assertTrue(TestHelper.verify(() -> {
      JobContext jobContext =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName));
      TaskPartitionState state = jobContext.getPartitionState(1);
      return (jobContext != null && state == TaskPartitionState.COMPLETED);
    }, TestHelper.WAIT_DURATION));

    _driver.stop(workflowName);
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
    _driver.addTask(workflowName, jobName, task);

    try {
      _driver.addTask(workflowName, jobName, task);
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

    _driver.stop(workflowName);
  }

  @Test(dependsOnMethods = "testAddTaskTwice")
  public void testAddTaskToJobNotStarted() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    String jobName = "JOB0";

    JobConfig.Builder jobBuilder1 = new JobConfig.Builder().setWorkflow(workflowName)
        .setExecutionDelay(5000L).setNumberOfTasks(1).setNumConcurrentTasksPerInstance(100)
        .setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));

    Workflow.Builder workflowBuilder1 =
        new Workflow.Builder(workflowName).addJob(jobName, jobBuilder1);
    _driver.start(workflowBuilder1.build());

    Assert.assertTrue(TestHelper.verify(() -> {
      WorkflowContext workflowContext = _driver.getWorkflowContext(workflowName);
      JobContext jobContext =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName));
      return (workflowContext != null && jobContext == null);
    }, TestHelper.WAIT_DURATION));

    // Add short running task
    Map<String, String> newTaskConfig =
        new HashMap<String, String>(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));
    TaskConfig task = new TaskConfig(null, newTaskConfig, null, null);
    _driver.addTask(workflowName, jobName, task);

    Assert.assertTrue(TestHelper.verify(() -> {
      JobContext jobContext =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName));
      if (jobContext == null) {
        return false;
      }
      TaskPartitionState state = jobContext.getPartitionState(1);
      if (state == null) {
        return false;
      }
      return (state == TaskPartitionState.COMPLETED);
    }, TestHelper.WAIT_DURATION));

    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);
  }

  @Test(dependsOnMethods = "testAddTaskToJobNotStarted")
  public void testAddTaskWorkflowAndJobNotStarted() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    String jobName = "JOB0";

    JobConfig.Builder jobBuilder1 = new JobConfig.Builder().setWorkflow(workflowName)
        .setNumberOfTasks(1).setNumConcurrentTasksPerInstance(100).setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));

    Workflow.Builder workflowBuilder1 =
        new Workflow.Builder(workflowName).addJob(jobName, jobBuilder1);

    _controller.syncStop();
    _driver.start(workflowBuilder1.build());

    Assert.assertTrue(TestHelper.verify(() -> {
      WorkflowContext workflowContext = _driver.getWorkflowContext(workflowName);
      JobContext jobContext =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName));
      return (workflowContext == null && jobContext == null);
    }, TestHelper.WAIT_DURATION));

    // Add short running task
    Map<String, String> newTaskConfig =
        new HashMap<String, String>(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));
    TaskConfig task = new TaskConfig(null, newTaskConfig, null, null);
    _driver.addTask(workflowName, jobName, task);

    // Start the Controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);
  }

  @Test(dependsOnMethods = "testAddTaskWorkflowAndJobNotStarted")
  public void testDeleteNonExistedTask() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    String jobName = "JOB0";

    JobConfig.Builder jobBuilder1 = new JobConfig.Builder().setWorkflow(workflowName)
        .setNumberOfTasks(1).setNumConcurrentTasksPerInstance(100).setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "9999999"));

    Workflow.Builder workflowBuilder1 =
        new Workflow.Builder(workflowName).addJob(jobName, jobBuilder1);

    _driver.start(workflowBuilder1.build());

    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, jobName),
        TaskState.IN_PROGRESS);
    String dummyID = "1234";
    try {
      _driver.deleteTask(workflowName, jobName, dummyID);
      Assert.fail("Exception is expected because a task with such ID does not exists!");
    } catch (IllegalArgumentException e) {
      // Helix Exception is expected because job id not running
    }
    _driver.waitToStop(workflowName, TestHelper.WAIT_DURATION);
  }

  @Test(dependsOnMethods = "testDeleteNonExistedTask")
  public void testDeleteTaskFromJobNotStarted() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    String jobName = "JOB0";

    JobConfig.Builder jobBuilder1 = new JobConfig.Builder().setWorkflow(workflowName)
        .setExecutionDelay(500000L).setNumberOfTasks(1).setNumConcurrentTasksPerInstance(100)
        .setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));

    Workflow.Builder workflowBuilder1 =
        new Workflow.Builder(workflowName).addJob(jobName, jobBuilder1);
    _driver.start(workflowBuilder1.build());

    Assert.assertTrue(TestHelper.verify(() -> {
      WorkflowContext workflowContext = _driver.getWorkflowContext(workflowName);
      JobContext jobContext =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName));
      return (workflowContext != null && jobContext == null);
    }, TestHelper.WAIT_DURATION));

    // Add short running task
    Map<String, String> newTaskConfig =
        new HashMap<String, String>(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));
    TaskConfig task = new TaskConfig(null, newTaskConfig, null, null);
    _driver.addTask(workflowName, jobName, task);

    JobConfig jobConfig =
        _driver.getJobConfig(TaskUtil.getNamespacedJobName(workflowName, jobName));

    // Make sure task has been added to the job config
    Assert.assertTrue(jobConfig.getMapConfigs().containsKey(task.getId()));

    _driver.deleteTask(workflowName, jobName, task.getId());
    jobConfig = _driver.getJobConfig(TaskUtil.getNamespacedJobName(workflowName, jobName));

    // Make sure task has been removed from job config
    Assert.assertFalse(jobConfig.getMapConfigs().containsKey(task.getId()));

    _driver.deleteAndWaitForCompletion(workflowName, TestHelper.WAIT_DURATION);
  }

  @Test(dependsOnMethods = "testDeleteTaskFromJobNotStarted")
  public void testAddAndDeleteTask() throws Exception {
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

    // Wait until initial task goes to RUNNING state
    Assert.assertTrue(TestHelper.verify(() -> {
      JobContext jobContext =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName));
      if (jobContext == null) {
        return false;
      }
      TaskPartitionState state = jobContext.getPartitionState(0);
      if (state == null) {
        return false;
      }
      return (state == TaskPartitionState.RUNNING);
    }, TestHelper.WAIT_DURATION));

    // Add new task
    Map<String, String> newTaskConfig =
        new HashMap<String, String>(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));
    TaskConfig task = new TaskConfig(null, newTaskConfig, null, null);
    _driver.addTask(workflowName, jobName, task);

    // Wait until new task goes to RUNNING state
    Assert.assertTrue(TestHelper.verify(() -> {
      JobContext jobContext =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName));
      if (jobContext == null) {
        return false;
      }
      TaskPartitionState state = jobContext.getPartitionState(1);
      if (state == null) {
        return false;
      }
      return (state == TaskPartitionState.RUNNING);
    }, TestHelper.WAIT_DURATION));

    _driver.deleteTask(workflowName, jobName, task.getId());
    JobConfig jobConfig =
        _driver.getJobConfig(TaskUtil.getNamespacedJobName(workflowName, jobName));
    // Make sure task has been removed from job config
    Assert.assertFalse(jobConfig.getMapConfigs().containsKey(task.getId()));

    Assert.assertTrue(TestHelper.verify(() -> {
      JobContext jobContext =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName));
      if (jobContext == null) {
        return false;
      }
      return (!jobContext.getPartitionSet().contains(1));
    }, TestHelper.WAIT_DURATION));

    _driver.stop(workflowName);
  }

  @Test(dependsOnMethods = "testAddAndDeleteTask")
  public void testDeleteTaskAndJobCompleted() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    String jobName = "JOB0";

    JobConfig.Builder jobBuilder1 = new JobConfig.Builder().setWorkflow(workflowName)
        .setNumberOfTasks(1).setNumConcurrentTasksPerInstance(100).setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "20000"));

    Workflow.Builder workflowBuilder1 =
        new Workflow.Builder(workflowName).addJob(jobName, jobBuilder1);
    _driver.start(workflowBuilder1.build());

    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, jobName),
        TaskState.IN_PROGRESS);

    // Wait until initial task goes to RUNNING state
    Assert.assertTrue(TestHelper.verify(() -> {
      JobContext jobContext =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName));
      if (jobContext == null) {
        return false;
      }
      TaskPartitionState state = jobContext.getPartitionState(0);
      if (state == null) {
        return false;
      }
      return (state == TaskPartitionState.RUNNING);
    }, TestHelper.WAIT_DURATION));

    // Add new task
    Map<String, String> taskConfig1 =
        new HashMap<String, String>(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));
    Map<String, String> taskConfig2 =
        new HashMap<String, String>(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));
    TaskConfig task1 = new TaskConfig(null, taskConfig1, null, null);
    TaskConfig task2 = new TaskConfig(null, taskConfig2, null, null);

    _driver.addTask(workflowName, jobName, task1);
    _driver.addTask(workflowName, jobName, task2);

    // Wait until new task goes to RUNNING state
    Assert.assertTrue(TestHelper.verify(() -> {
      JobContext jobContext =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName));
      if (jobContext == null) {
        return false;
      }
      TaskPartitionState state1 = jobContext.getPartitionState(1);
      TaskPartitionState state2 = jobContext.getPartitionState(2);
      if (state1 == null && state2 == null) {
        return false;
      }
      return (state1 == TaskPartitionState.RUNNING && state2 == TaskPartitionState.RUNNING);
    }, TestHelper.WAIT_DURATION));

    _driver.deleteTask(workflowName, jobName, task1.getId());
    _driver.deleteTask(workflowName, jobName, task2.getId());

    JobConfig jobConfig =
        _driver.getJobConfig(TaskUtil.getNamespacedJobName(workflowName, jobName));
    // Make sure task has been removed from job config
    Assert.assertFalse(jobConfig.getMapConfigs().containsKey(task1.getId()));
    Assert.assertFalse(jobConfig.getMapConfigs().containsKey(task2.getId()));

    Assert.assertTrue(TestHelper.verify(() -> {
      JobContext jobContext =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName));
      if (jobContext == null) {
        return false;
      }
      return (!jobContext.getPartitionSet().contains(1)
          && !jobContext.getPartitionSet().contains(2));
    }, TestHelper.WAIT_DURATION));

    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, jobName),
        TaskState.COMPLETED);
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);
  }

  @Test(dependsOnMethods = "testDeleteTaskAndJobCompleted")
  public void testAddDeleteTaskOneInstance() throws Exception {
    // Stop all participant other than participant 0
    for (int i = 1; i < _numNodes; i++) {
      super.stopParticipant(i);
      Assert.assertFalse(_participants[i].isConnected());
    }

    String workflowName = TestHelper.getTestMethodName();
    String jobName = "JOB0";

    JobConfig.Builder jobBuilder1 = new JobConfig.Builder().setWorkflow(workflowName)
        .setNumberOfTasks(1).setNumConcurrentTasksPerInstance(1).setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));

    Workflow.Builder workflowBuilder1 =
        new Workflow.Builder(workflowName).addJob(jobName, jobBuilder1);
    _driver.start(workflowBuilder1.build());

    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, jobName),
        TaskState.IN_PROGRESS);

    // Wait until initial task goes to RUNNING state
    Assert.assertTrue(TestHelper.verify(() -> {
      JobContext jobContext =
          _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName));
      if (jobContext == null) {
        return false;
      }
      TaskPartitionState state = jobContext.getPartitionState(0);
      if (state == null) {
        return false;
      }
      return (state == TaskPartitionState.RUNNING);
    }, TestHelper.WAIT_DURATION));

    // Add new task
    Map<String, String> newTaskConfig =
        new HashMap<String, String>(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));
    TaskConfig task = new TaskConfig(null, newTaskConfig, null, null);
    _driver.addTask(workflowName, jobName, task);
    Assert.assertEquals(_driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName))
        .getPartitionSet().size(), 2);
    // Since only one task is allowed per instance, the new task should be scheduled
    Assert.assertNull(_driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName))
        .getPartitionState(1));
    _driver.deleteTask(workflowName, jobName, task.getId());
    Assert.assertEquals(_driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName))
        .getPartitionSet().size(), 1);
    _driver.stop(workflowName);
  }
}
