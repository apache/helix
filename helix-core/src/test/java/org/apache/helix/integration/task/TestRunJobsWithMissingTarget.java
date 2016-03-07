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
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestRunJobsWithMissingTarget extends ZkIntegrationTestBase {
  private static final Logger LOG = Logger.getLogger(TestRunJobsWithMissingTarget.class);
  private static final int num_nodes = 5;
  private static final int num_dbs = 5;
  private static final int START_PORT = 12918;
  private static final String MASTER_SLAVE_STATE_MODEL = "MasterSlave";
  private static final int NUM_PARTITIONS = 20;
  private static final int NUM_REPLICAS = 3;
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + getShortClassName();
  private final MockParticipantManager[] _participants = new MockParticipantManager[num_nodes];
  private ClusterControllerManager _controller;
  private ClusterSetup _setupTool;

  private List<String> _test_dbs = new ArrayList<String>();

  private HelixManager _manager;
  private TaskDriver _driver;

  @BeforeClass
  public void beforeClass() throws Exception {
    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }

    _setupTool = new ClusterSetup(ZK_ADDR);
    _setupTool.addCluster(CLUSTER_NAME, true);
    for (int i = 0; i < num_nodes; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    // Set up target dbs
    for (int i = 0; i < num_dbs; i++) {
      String db = "TestDB" + i;
      _setupTool
          .addResourceToCluster(CLUSTER_NAME, db, NUM_PARTITIONS + 10 * i, MASTER_SLAVE_STATE_MODEL,
              IdealState.RebalanceMode.FULL_AUTO.toString());
      _setupTool.rebalanceStorageCluster(CLUSTER_NAME, db, NUM_REPLICAS);
      _test_dbs.add(db);
    }

    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
    taskFactoryReg.put(MockTask.TASK_COMMAND, new TaskFactory() {
      @Override public Task createNewTask(TaskCallbackContext context) {
        return new MockTask(context);
      }
    });

    // start dummy participants
    for (int i = 0; i < num_nodes; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);

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
    _manager = HelixManagerFactory
        .getZKHelixManager(CLUSTER_NAME, "Admin", InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();

    _driver = new TaskDriver(_manager);

    boolean result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.MasterNbInExtViewVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);
  }

  @AfterClass
  public void afterClass() throws Exception {
    _manager.disconnect();
    _controller.syncStop();
    for (int i = 0; i < num_nodes; i++) {
      _participants[i].syncStop();
    }
    _setupTool.deleteCluster(CLUSTER_NAME);
  }

  @Test
  public void testJobFailsWithMissingTarget() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuilder = TaskTestUtil.buildJobQueue(queueName);
    // Create and Enqueue jobs
    List<String> currentJobNames = new ArrayList<String>();
    for (int i = 0; i < num_dbs; i++) {
      JobConfig.Builder jobConfig =
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND).setTargetResource(
              _test_dbs.get(i))
              .setTargetPartitionStates(Sets.newHashSet("SLAVE"));
      String jobName = "job" + _test_dbs.get(i);
      queueBuilder.enqueueJob(jobName, jobConfig);
      currentJobNames.add(jobName);
    }

    _setupTool.dropResourceFromCluster(CLUSTER_NAME, _test_dbs.get(1));
    _driver.start(queueBuilder.build());

    String namedSpaceJob = String.format("%s_%s", queueName, currentJobNames.get(1));
    TaskTestUtil.pollForJobState(_driver, queueName, namedSpaceJob, TaskState.FAILED);
    TaskTestUtil.pollForWorkflowState(_driver, queueName, TaskState.FAILED);

    _driver.delete(queueName);
  }

  @Test(dependsOnMethods = "testJobFailsWithMissingTarget")
  public void testJobContinueUponParentJobFailure() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuilder = TaskTestUtil.buildJobQueue(queueName, 0, 3);
    // Create and Enqueue jobs
    List<String> currentJobNames = new ArrayList<String>();
    for (int i = 0; i < num_dbs; i++) {
      JobConfig.Builder jobConfig =
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND).setTargetResource(_test_dbs.get(i))
              .setTargetPartitionStates(Sets.newHashSet("SLAVE")).setIgnoreDependentJobFailure(true);
      String jobName = "job" + _test_dbs.get(i);
      queueBuilder.enqueueJob(jobName, jobConfig);
      currentJobNames.add(jobName);
    }

    _driver.start(queueBuilder.build());

    String namedSpaceJob1 = String.format("%s_%s", queueName, currentJobNames.get(1));
    TaskTestUtil.pollForJobState(_driver, queueName, namedSpaceJob1, TaskState.FAILED);
    String lastJob =
        String.format("%s_%s", queueName, currentJobNames.get(currentJobNames.size() - 1));
    TaskTestUtil.pollForJobState(_driver, queueName, lastJob, TaskState.COMPLETED);

    _driver.delete(queueName);
  }

  @Test(dependsOnMethods = "testJobContinueUponParentJobFailure")
  public void testJobFailsWithMissingTargetInRunning() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuilder = TaskTestUtil.buildJobQueue(queueName);
    // Create and Enqueue jobs
    List<String> currentJobNames = new ArrayList<String>();
    for (int i = 0; i < num_dbs; i++) {
      JobConfig.Builder jobConfig =
          new JobConfig.Builder().setCommand(MockTask.TASK_COMMAND).setTargetResource(_test_dbs.get(i))
              .setTargetPartitionStates(Sets.newHashSet("SLAVE"));
      String jobName = "job" + _test_dbs.get(i);
      queueBuilder.enqueueJob(jobName, jobConfig);
      currentJobNames.add(jobName);
    }

    _driver.start(queueBuilder.build());
    _setupTool.dropResourceFromCluster(CLUSTER_NAME, _test_dbs.get(0));

    String namedSpaceJob1 = String.format("%s_%s", queueName, currentJobNames.get(0));
    TaskTestUtil.pollForJobState(_driver, queueName, namedSpaceJob1, TaskState.FAILED);
    TaskTestUtil.pollForWorkflowState(_driver, queueName, TaskState.FAILED);

    _driver.delete(queueName);
  }
}
