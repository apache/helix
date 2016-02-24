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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestTaskRebalancerParallel extends ZkIntegrationTestBase {
  private static final int n = 5;
  private static final int START_PORT = 12918;
  private static final String MASTER_SLAVE_STATE_MODEL = "MasterSlave";
  private static final int NUM_PARTITIONS = 20;
  private static final int NUM_REPLICAS = 3;
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + getShortClassName();
  private final List<String> testDbNames =
      Arrays.asList("TestDB_1", "TestDB_2", "TestDB_3", "TestDB_4");


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

    for (String testDbName : testDbNames) {
      setupTool.addResourceToCluster(CLUSTER_NAME, testDbName, NUM_PARTITIONS,
          MASTER_SLAVE_STATE_MODEL);
      setupTool.rebalanceStorageCluster(CLUSTER_NAME, testDbName, NUM_REPLICAS);
    }

    // start dummy participants
    for (int i = 0; i < n; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);

      final long delay = (i + 1) * 1000L;
      Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
      taskFactoryReg.put(MockTask.TASK_COMMAND, new TaskFactory() {
        @Override
        public Task createNewTask(TaskCallbackContext context) {
          return new MockTask(context);
        }
      });

      // Register a Task state model factory.
      StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
      stateMachine.registerStateModelFactory("Task",
          new TaskStateModelFactory(_participants[i], taskFactoryReg));
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
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                CLUSTER_NAME));
    Assert.assertTrue(result);
  }

  @AfterClass
  public void afterClass() throws Exception {
    _manager.disconnect();
    _controller.syncStop();
    // _controller = null;
    for (int i = 0; i < n; i++) {
      _participants[i].syncStop();
      // _participants[i] = null;
    }
  }

  @Test public void test() throws Exception {
    final int PARALLEL_COUNT = 2;

    String queueName = TestHelper.getTestMethodName();

    WorkflowConfig.Builder cfgBuilder = new WorkflowConfig.Builder();
    cfgBuilder.setParallelJobs(PARALLEL_COUNT);

    JobQueue.Builder queueBuild =
        new JobQueue.Builder(queueName).setWorkflowConfig(cfgBuilder.build());
    JobQueue queue = queueBuild.build();
    _driver.createQueue(queue);

    List<JobConfig.Builder> jobConfigBuilders = new ArrayList<JobConfig.Builder>();
    for (String testDbName : testDbNames) {
      jobConfigBuilders.add(
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND).setTargetResource(testDbName)
              .setTargetPartitionStates(Collections.singleton("SLAVE")));
    }

    for (int i = 0; i < jobConfigBuilders.size(); ++i) {
      _driver.enqueueJob(queueName, "job_" + (i + 1), jobConfigBuilders.get(i));
    }

    Assert.assertTrue(TaskTestUtil.pollForWorkflowParallelState(_driver, queueName));
  }
}
