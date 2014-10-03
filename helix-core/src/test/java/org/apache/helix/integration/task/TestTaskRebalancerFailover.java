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
import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobDag;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

public class TestTaskRebalancerFailover extends ZkUnitTestBase {
  private static final Logger LOG = Logger.getLogger(TestTaskRebalancerFailover.class);

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
    taskFactoryReg.put("DummyTask", new TaskFactory() {
      @Override
      public Task createNewTask(TaskCallbackContext context) {
        return new DummyTask(context);
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

  @Test
  public void test() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue queue = new JobQueue.Builder(queueName).build();
    _driver.createQueue(queue);

    // Enqueue jobs
    Set<String> master = Sets.newHashSet("MASTER");
    JobConfig.Builder job =
        new JobConfig.Builder().setCommand("DummyTask")
            .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB).setTargetPartitionStates(master);
    String job1Name = "masterJob";
    LOG.info("Enqueuing job: " + job1Name);
    _driver.enqueueJob(queueName, job1Name, job);

    // check all tasks completed on MASTER
    String namespacedJob1 = String.format("%s_%s", queueName, job1Name);
    TestUtil.pollForJobState(_manager, queueName, namespacedJob1, TaskState.COMPLETED);

    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    ExternalView ev =
        accessor.getProperty(keyBuilder.externalView(WorkflowGenerator.DEFAULT_TGT_DB));
    JobContext ctx = TaskUtil.getJobContext(_manager, namespacedJob1);
    Set<String> failOverPartitions = Sets.newHashSet();
    for (int p = 0; p < _p; p++) {
      String instanceName = ctx.getAssignedParticipant(p);
      Assert.assertNotNull(instanceName);
      String partitionName = ctx.getTargetForPartition(p);
      Assert.assertNotNull(partitionName);
      String state = ev.getStateMap(partitionName).get(instanceName);
      Assert.assertNotNull(state);
      Assert.assertEquals(state, "MASTER");
      if (instanceName.equals("localhost_12918")) {
        failOverPartitions.add(partitionName);
      }
    }

    // enqueue another master job and fail localhost_12918
    String job2Name = "masterJob2";
    String namespacedJob2 = String.format("%s_%s", queueName, job2Name);
    LOG.info("Enqueuing job: " + job2Name);
    _driver.enqueueJob(queueName, job2Name, job);

    TestUtil.pollForJobState(_manager, queueName, namespacedJob2, TaskState.IN_PROGRESS);
    _participants[0].syncStop();
    TestUtil.pollForJobState(_manager, queueName, namespacedJob2, TaskState.COMPLETED);

    // tasks previously assigned to localhost_12918 should be re-scheduled on new master
    ctx = TaskUtil.getJobContext(_manager, namespacedJob2);
    ev = accessor.getProperty(keyBuilder.externalView(WorkflowGenerator.DEFAULT_TGT_DB));
    for (int p = 0; p < _p; p++) {
      String partitionName = ctx.getTargetForPartition(p);
      Assert.assertNotNull(partitionName);
      if (failOverPartitions.contains(partitionName)) {
        String instanceName = ctx.getAssignedParticipant(p);
        Assert.assertNotNull(instanceName);
        Assert.assertNotSame(instanceName, "localhost_12918");
        String state = ev.getStateMap(partitionName).get(instanceName);
        Assert.assertNotNull(state);
        Assert.assertEquals(state, "MASTER");
      }
    }

    // Flush queue and check cleanup
    _driver.flushQueue(queueName);
    Assert.assertNull(accessor.getProperty(keyBuilder.idealStates(namespacedJob1)));
    Assert.assertNull(accessor.getProperty(keyBuilder.resourceConfig(namespacedJob1)));
    Assert.assertNull(accessor.getProperty(keyBuilder.idealStates(namespacedJob2)));
    Assert.assertNull(accessor.getProperty(keyBuilder.resourceConfig(namespacedJob2)));
    WorkflowConfig workflowCfg = TaskUtil.getWorkflowCfg(_manager, queueName);
    JobDag dag = workflowCfg.getJobDag();
    Assert.assertFalse(dag.getAllNodes().contains(namespacedJob1));
    Assert.assertFalse(dag.getAllNodes().contains(namespacedJob2));
    Assert.assertFalse(dag.getChildrenToParents().containsKey(namespacedJob1));
    Assert.assertFalse(dag.getChildrenToParents().containsKey(namespacedJob2));
    Assert.assertFalse(dag.getParentsToChildren().containsKey(namespacedJob1));
    Assert.assertFalse(dag.getParentsToChildren().containsKey(namespacedJob2));
  }
}
