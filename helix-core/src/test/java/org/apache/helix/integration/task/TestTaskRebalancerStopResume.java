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
import org.apache.helix.integration.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.Workflow;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestTaskRebalancerStopResume extends ZkIntegrationTestBase {
  private static final Logger LOG = Logger.getLogger(TestTaskRebalancerStopResume.class);
  private static final int n = 5;
  private static final int START_PORT = 12918;
  private static final String MASTER_SLAVE_STATE_MODEL = "MasterSlave";
  private static final String TGT_DB = "TestDB";
  private static final String TASK_RESOURCE = "SomeTask";
  private static final int NUM_PARTITIONS = 20;
  private static final int NUM_REPLICAS = 3;
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + getShortClassName();
  private final MockParticipantManager[] _participants = new MockParticipantManager[n];
  private ClusterControllerManager _controller;

  private HelixManager _manager;
  private TaskDriver _driver;

  @BeforeClass
  public void beforeClass() throws Exception {
    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }

    ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);
    setupTool.addCluster(CLUSTER_NAME, true);
    for (int i = 0; i < n; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    // Set up target db
    setupTool.addResourceToCluster(CLUSTER_NAME, TGT_DB, NUM_PARTITIONS, MASTER_SLAVE_STATE_MODEL);
    setupTool.rebalanceStorageCluster(CLUSTER_NAME, TGT_DB, NUM_REPLICAS);

    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
    taskFactoryReg.put("Reindex", new TaskFactory() {
      @Override
      public Task createNewTask(String config) {
        return new ReindexTask(config);
      }
    });

    // start dummy participants
    for (int i = 0; i < n; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);

      // Register a Task state model factory.
      StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
      stateMachine.registerStateModelFactory("Task", new TaskStateModelFactory(_participants[i],
          taskFactoryReg));

      _participants[i].syncStart();
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // create cluster manager
    _manager =
        HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin", InstanceType.ADMINISTRATOR,
            ZK_ADDR);
    _manager.connect();

    _driver = new TaskDriver(_manager);

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new ClusterStateVerifier.MasterNbInExtViewVerifier(
            ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                CLUSTER_NAME));
    Assert.assertTrue(result);
  }

  @AfterClass
  public void afterClass() throws Exception {
    _controller.syncStop();
    for (int i = 0; i < n; i++) {
      _participants[i].syncStop();
    }
    _manager.disconnect();
  }

  @Test
  public void stopAndResume() throws Exception {
    Workflow flow =
        WorkflowGenerator.generateDefaultSingleTaskWorkflowBuilderWithExtraConfigs(TASK_RESOURCE,
            TaskConfig.COMMAND_CONFIG, String.valueOf(100)).build();

    LOG.info("Starting flow " + flow.getName());
    _driver.start(flow);
    TestUtil.pollForWorkflowState(_manager, TASK_RESOURCE, TaskState.IN_PROGRESS);

    LOG.info("Pausing task");
    _driver.stop(TASK_RESOURCE);
    TestUtil.pollForWorkflowState(_manager, TASK_RESOURCE, TaskState.STOPPED);

    LOG.info("Resuming task");
    _driver.resume(TASK_RESOURCE);
    TestUtil.pollForWorkflowState(_manager, TASK_RESOURCE, TaskState.COMPLETED);
  }

  @Test
  public void stopAndResumeWorkflow() throws Exception {
    String workflow = "SomeWorkflow";
    Workflow flow = WorkflowGenerator.generateDefaultRepeatedTaskWorkflowBuilder(workflow).build();

    LOG.info("Starting flow " + workflow);
    _driver.start(flow);
    TestUtil.pollForWorkflowState(_manager, workflow, TaskState.IN_PROGRESS);

    LOG.info("Pausing workflow");
    _driver.stop(workflow);
    TestUtil.pollForWorkflowState(_manager, workflow, TaskState.STOPPED);

    LOG.info("Resuming workflow");
    _driver.resume(workflow);
    TestUtil.pollForWorkflowState(_manager, workflow, TaskState.COMPLETED);
  }

  public static class ReindexTask implements Task {
    private final long _delay;
    private volatile boolean _canceled;

    public ReindexTask(String cfg) {
      _delay = Long.parseLong(cfg);
    }

    @Override
    public TaskResult run() {
      long expiry = System.currentTimeMillis() + _delay;
      long timeLeft;
      while (System.currentTimeMillis() < expiry) {
        if (_canceled) {
          timeLeft = expiry - System.currentTimeMillis();
          return new TaskResult(TaskResult.Status.CANCELED, String.valueOf(timeLeft < 0 ? 0
              : timeLeft));
        }
        sleep(50);
      }
      timeLeft = expiry - System.currentTimeMillis();
      return new TaskResult(TaskResult.Status.COMPLETED,
          String.valueOf(timeLeft < 0 ? 0 : timeLeft));
    }

    @Override
    public void cancel() {
      _canceled = true;
    }

    private static void sleep(long d) {
      try {
        Thread.sleep(d);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
