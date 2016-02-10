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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test task will be retried up to MaxAttemptsPerTask {@see HELIX-562}
 */
public class TestTaskRebalancerRetryLimit extends ZkIntegrationTestBase {
  private final String _clusterName = TestHelper.getTestClassName();
  private static final int _n = 5;
  private static final int _p = 20;
  private static final int _r = 3;
  private final MockParticipantManager[] _participants = new MockParticipantManager[_n];
  private ClusterControllerManager _controller;
  private HelixManager _manager;
  private TaskDriver _driver;

  @BeforeClass
  public void beforeClass() throws Exception {
    ClusterSetup setup = new ClusterSetup(_gZkClient);
    setup.addCluster(_clusterName, true);
    for (int i = 0; i < _n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      setup.addInstanceToCluster(_clusterName, instanceName);
    }

    // Set up target db
    setup.addResourceToCluster(_clusterName, WorkflowGenerator.DEFAULT_TGT_DB, _p, "MasterSlave");
    setup.rebalanceStorageCluster(_clusterName, WorkflowGenerator.DEFAULT_TGT_DB, _r);

    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
    taskFactoryReg.put("ErrorTask", new TaskFactory() {
      @Override
      public Task createNewTask(TaskCallbackContext context) {
        return new ErrorTask();
      }
    });

    // start dummy participants
    for (int i = 0; i < _n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, _clusterName, instanceName);

      // Register a Task state model factory.
      StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
      stateMachine.registerStateModelFactory("Task", new TaskStateModelFactory(_participants[i],
          taskFactoryReg));
      _participants[i].syncStart();
    }

    // start controller
    String controllerName = "controller";
    _controller = new ClusterControllerManager(ZK_ADDR, _clusterName, controllerName);
    _controller.syncStart();

    // create cluster manager
    _manager =
        HelixManagerFactory.getZKHelixManager(_clusterName, "Admin", InstanceType.ADMINISTRATOR,
            ZK_ADDR);
    _manager.connect();
    _driver = new TaskDriver(_manager);

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                _clusterName));
    Assert.assertTrue(result);
  }

  @AfterClass
  public void afterClass() throws Exception {
    _controller.syncStop();
    for (int i = 0; i < _n; i++) {
      if (_participants[i] != null && _participants[i].isConnected()) {
        _participants[i].syncStop();
      }
    }
    _manager.disconnect();
  }

  @Test public void test() throws Exception {
    String jobResource = TestHelper.getTestMethodName();

    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG);
    jobBuilder.setJobCommandConfigMap(WorkflowGenerator.DEFAULT_COMMAND_CONFIG)
        .setMaxAttemptsPerTask(2).setCommand("ErrorTask").setFailureThreshold(Integer.MAX_VALUE);

    Workflow flow =
        WorkflowGenerator.generateSingleJobWorkflowBuilder(jobResource, jobBuilder).build();

    _driver.start(flow);

    // Wait until the job completes.
    TestUtil.pollForWorkflowState(_manager, jobResource, TaskState.COMPLETED);

    JobContext ctx = TaskUtil.getJobContext(_manager, TaskUtil.getNamespacedJobName(jobResource));
    for (int i = 0; i < _p; i++) {
      TaskPartitionState state = ctx.getPartitionState(i);
      if (state != null) {
        Assert.assertEquals(state, TaskPartitionState.TASK_ERROR);
        Assert.assertEquals(ctx.getPartitionNumAttempts(i), 2);
      }
    }
  }

  private static class ErrorTask implements Task {
    public ErrorTask() {
    }

    @Override
    public TaskResult run() {
      throw new RuntimeException("IGNORABLE exception: test throw exception from task");
    }

    @Override
    public void cancel() {
    }
  }
}
