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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

/**
 * Test to check workflow context is not created without workflow config.
 * Test workflow context will be deleted if workflow config has been removed.
 */
public class TestWorkflowContextWithoutConfig extends TaskTestBase {

  @Test
  public void testWorkflowContextGarbageCollection() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder1 = createSimpleWorkflowBuilder(workflowName);
    _driver.start(builder1.build());

    // Wait until workflow is created and IN_PROGRESS state.
    _driver.pollForWorkflowState(workflowName, TaskState.IN_PROGRESS);

    // Check that WorkflowConfig and WorkflowContext are indeed created for this workflow
    Assert.assertNotNull(_driver.getWorkflowConfig(workflowName));
    Assert.assertNotNull(_driver.getWorkflowContext(workflowName));

    String workflowContextPath =
        "/" + CLUSTER_NAME + "/PROPERTYSTORE/TaskRebalancer/" + workflowName + "/Context";

    ZNRecord record = _manager.getHelixDataAccessor().getBaseDataAccessor().get(workflowContextPath,
        null, AccessOption.PERSISTENT);
    Assert.assertNotNull(record);

    // Wait until workflow is completed.
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);

    // Verify that WorkflowConfig and WorkflowContext are removed after workflow got expired.
    boolean workflowExpired = TestHelper.verify(() -> {
      WorkflowContext wCtx = _driver.getWorkflowContext(workflowName);
      WorkflowConfig wCfg = _driver.getWorkflowConfig(workflowName);
      return (wCtx == null && wCfg == null);
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(workflowExpired);

    _controller.syncStop();

    // Write workflow context to ZooKeeper
    _manager.getHelixDataAccessor().getBaseDataAccessor().set(workflowContextPath, record,
        AccessOption.PERSISTENT);

    // Verify context is written back to ZK.
    record = _manager.getHelixDataAccessor().getBaseDataAccessor().get(workflowContextPath,
        null, AccessOption.PERSISTENT);
    Assert.assertNotNull(record);

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // Create and start new workflow just to make sure controller is running and new workflow is
    // scheduled successfully.
    String workflowName2 = TestHelper.getTestMethodName() + "_2";
    Workflow.Builder builder2 = createSimpleWorkflowBuilder(workflowName2);
    _driver.start(builder2.build());
    _driver.pollForWorkflowState(workflowName2, TaskState.COMPLETED);

    // Verify that WorkflowContext will be deleted
    boolean contextDeleted = TestHelper.verify(() -> {
      WorkflowContext wCtx = _driver.getWorkflowContext(workflowName);
      return (wCtx == null);
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(contextDeleted);
  }

  @Test
  public void testJobContextGarbageCollection() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder builder = new Workflow.Builder(workflowName).setExpiry(1000000L);
    JobConfig.Builder jobBuilder1 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName).setTimeoutPerTask(Long.MAX_VALUE)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));
    builder.addJob("JOB0", jobBuilder1);

    _driver.start(builder.build());

    // Wait until workflow is COMPLETED
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);

    HelixDataAccessor accessor = _manager.getHelixDataAccessor();

    JobContext jobContext =
        accessor.getProperty(accessor.keyBuilder().jobContextZNode(workflowName, "JOB0"));

    Assert.assertNotNull(jobContext);

    _controller.syncStop();

    // Write workflow context to ZooKeeper
    jobContext.setName(TaskUtil.getNamespacedJobName(workflowName, "JOB1"));
    accessor.setProperty(accessor.keyBuilder().jobContextZNode(workflowName, "JOB1"), jobContext);

    // Verify context is written back to ZK.
    jobContext =
        accessor.getProperty(accessor.keyBuilder().jobContextZNode(workflowName, "JOB1"));
    Assert.assertNotNull(jobContext);

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // Create and start new workflow just to make sure controller is running and new workflow is
    // scheduled successfully.
    String workflowName2 = TestHelper.getTestMethodName() + "_2";
    Workflow.Builder builder2 = createSimpleWorkflowBuilder(workflowName2);
    _driver.start(builder2.build());
    _driver.pollForWorkflowState(workflowName2, TaskState.COMPLETED);

    // Verify that JobContext will be deleted
    boolean contextDeleted = TestHelper.verify(() -> {
      JobContext jobCtx = _driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, "JOB1"));
      return (jobCtx == null);
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(contextDeleted);
  }

  private Workflow.Builder createSimpleWorkflowBuilder(String workflowName) {
    final long expiryTime = 5000L;
    Workflow.Builder builder = new Workflow.Builder(workflowName);

    // Workflow DAG Schematic:
    //          JOB0
    //           /\
    //          /  \
    //         /    \
    //       JOB1   JOB2

    JobConfig.Builder jobBuilder1 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));

    JobConfig.Builder jobBuilder2 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));

    JobConfig.Builder jobBuilder3 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setMaxAttemptsPerTask(1).setWorkflow(workflowName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));

    builder.addParentChildDependency("JOB0", "JOB1");
    builder.addParentChildDependency("JOB0", "JOB2");
    builder.addJob("JOB0", jobBuilder1);
    builder.addJob("JOB1", jobBuilder2);
    builder.addJob("JOB2", jobBuilder3);
    builder.setExpiry(expiryTime);
    return builder;
  }
}
