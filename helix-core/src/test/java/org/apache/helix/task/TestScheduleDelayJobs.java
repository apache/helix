package org.apache.helix.task;

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

import org.apache.helix.TestHelper;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.TaskTestUtil;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestScheduleDelayJobs extends TaskSynchronizedTestBase {
  private TestRebalancer _testRebalancer = new TestRebalancer();
  private ClusterDataCache _cache;

  @BeforeClass
  public void beforeClass() throws Exception {
    setSingleTestEnvironment();
    super.beforeClass();
  }

  @Test
  public void testScheduleDelayTime() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = new Workflow.Builder(workflowName);
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2)
            .setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG);
    builder.addParentChildDependency("JOB0", "JOB1");
    builder.addJob("JOB0", jobBuilder);
    builder.addJob("JOB1", jobBuilder.setExecutionDelay(10000L));
    WorkflowContext workflowContext = TaskTestUtil
        .buildWorkflowContext(workflowName, TaskState.IN_PROGRESS, null, TaskState.COMPLETED,
            TaskState.NOT_STARTED);
    _driver.start(builder.build());
    _cache = TaskTestUtil.buildClusterDataCache(_manager.getHelixDataAccessor(), CLUSTER_NAME);
    long currentTime = System.currentTimeMillis();
    TaskUtil.setWorkflowContext(_manager, workflowName, workflowContext);
    TaskTestUtil.calculateTaskSchedulingStage(_cache, _manager);
    TaskTestUtil.calculateTaskSchedulingStage(_cache, _manager);
    Assert.assertTrue(_testRebalancer.getRebalanceTime(workflowName) - currentTime >= 10000L);
  }

  @Test
  public void testScheduleStartTime() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = new Workflow.Builder(workflowName);
    JobConfig.Builder jobBuilder =
        new JobConfig.Builder().setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setCommand(MockTask.TASK_COMMAND).setMaxAttemptsPerTask(2)
            .setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG);
    long currentTime = System.currentTimeMillis() + 10000L;
    builder.addParentChildDependency("JOB0", "JOB2");
    builder.addParentChildDependency("JOB1", "JOB2");
    builder.addJob("JOB0", jobBuilder);
    builder.addJob("JOB1", jobBuilder);
    builder.addJob("JOB2", jobBuilder.setExecutionStart(currentTime));
    WorkflowContext workflowContext = TaskTestUtil
        .buildWorkflowContext(workflowName, TaskState.IN_PROGRESS, null, TaskState.COMPLETED,
            TaskState.COMPLETED, TaskState.NOT_STARTED);
    _driver.start(builder.build());
    _cache = TaskTestUtil.buildClusterDataCache(_manager.getHelixDataAccessor(), CLUSTER_NAME);
    TaskUtil.setWorkflowContext(_manager, workflowName, workflowContext);
    TaskTestUtil.calculateTaskSchedulingStage(_cache, _manager);
    TaskTestUtil.calculateTaskSchedulingStage(_cache, _manager);
    Assert.assertTrue(_testRebalancer.getRebalanceTime(workflowName) == currentTime);
  }

  private class TestRebalancer extends WorkflowRebalancer {
    public long getRebalanceTime(String workflow) {
      return _rebalanceScheduler.getRebalanceTime(workflow);
    }
  }

}
