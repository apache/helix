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

import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.TestHelper;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

/**
 * Test to check workflow context is not created without workflow config.
 * Test workflow context will be deleted if workflow config has been removed.
 */
public class TestWorkflowContextWithoutConfig extends TaskTestBase {
  private HelixAdmin _admin;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    _admin = _gSetupTool.getClusterManagementTool();
  }

  @Test
  public void testWorkflowContextWithoutConfig() throws Exception {
    String workflowName1 = TestHelper.getTestMethodName() + "_1";
    Workflow.Builder builder1 = createSimpleWorkflowBuilder(workflowName1);
    _driver.start(builder1.build());

    // Wait until workflow is created and IN_PROGRESS state.
    _driver.pollForWorkflowState(workflowName1, TaskState.IN_PROGRESS);

    // Check that WorkflowConfig, WorkflowContext, and IdealState are indeed created for this
    // workflow
    Assert.assertNotNull(_driver.getWorkflowConfig(workflowName1));
    Assert.assertNotNull(_driver.getWorkflowContext(workflowName1));
    Assert.assertNotNull(_admin.getResourceIdealState(CLUSTER_NAME, workflowName1));

    String idealStatePath = "/" + CLUSTER_NAME + "/IDEALSTATES/" + workflowName1;
    ZNRecord record = _manager.getHelixDataAccessor().getBaseDataAccessor().get(idealStatePath,
        null, AccessOption.PERSISTENT);
    Assert.assertNotNull(record);

    // Wait until workflow is completed.
    _driver.pollForWorkflowState(workflowName1, TaskState.COMPLETED);

    // Verify that WorkflowConfig, WorkflowContext, and IdealState are removed after workflow got
    // expired.
    boolean workflowExpired = TestHelper.verify(() -> {
      WorkflowContext wCtx = _driver.getWorkflowContext(workflowName1);
      WorkflowConfig wCfg = _driver.getWorkflowConfig(workflowName1);
      IdealState idealState = _admin.getResourceIdealState(CLUSTER_NAME, workflowName1);
      return (wCtx == null && wCfg == null && idealState == null);
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(workflowExpired);

    // Write idealState to ZooKeeper
    _manager.getHelixDataAccessor().getBaseDataAccessor().set(idealStatePath, record,
        AccessOption.PERSISTENT);

    // Create and start a new workflow just to make sure pipeline runs several times and context
    // will not be created for workflow1 again
    String workflowName2 = TestHelper.getTestMethodName() + "_2";
    Workflow.Builder builder2 = createSimpleWorkflowBuilder(workflowName2);
    _driver.start(builder2.build());
    _driver.pollForWorkflowState(workflowName2, TaskState.COMPLETED);

    // Verify that context is not created after IdealState is written back to ZK.
    boolean workflowContextNotCreated = TestHelper.verify(() -> {
      WorkflowContext wCtx = _driver.getWorkflowContext(workflowName1);
      WorkflowConfig wCfg = _driver.getWorkflowConfig(workflowName1);
      IdealState idealState = _admin.getResourceIdealState(CLUSTER_NAME, workflowName1);
      return (wCtx == null && wCfg == null && idealState != null);
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(workflowContextNotCreated);
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
