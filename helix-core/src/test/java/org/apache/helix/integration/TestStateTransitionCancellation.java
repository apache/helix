package org.apache.helix.integration;

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

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixRollbackException;
import org.apache.helix.NotificationContext;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.mock.participant.MockDelayMSStateModel;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.Message;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.tools.ClusterSetup;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestStateTransitionCancellation extends TaskTestBase {
  // TODO: Replace the thread sleep with synchronized condition check
  private ConfigAccessor _configAccessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numDbs = 1;
    _numParitions = 20;
    _numNodes = 2;
    _numReplicas = 2;
    _participants = new MockParticipantManager[_numNodes];
    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }

    _setupTool = new ClusterSetup(ZK_ADDR);
    _setupTool.addCluster(CLUSTER_NAME, true);
    setupParticipants();
    setupDBs();

    registerParticipants(_participants, _numNodes, _startPort, 0, -3000000L);

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    createManagers();
    _configAccessor = new ConfigAccessor(_gZkClient);
  }

  @Test
  public void testCancellationWhenDisableResource() throws InterruptedException {
    // Enable cancellation
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.stateTransitionCancelEnabled(true);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    // Wait for assignment done
    Thread.sleep(2000);

    // Disable the resource
    _setupTool.getClusterManagementTool()
        .enableResource(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, false);


    // Wait for pipeline reaching final stage
    Thread.sleep(2000L);
    ExternalView externalView = _setupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    for (String partition : externalView.getPartitionSet()) {
      for (String currentState : externalView.getStateMap(partition).values()) {
        Assert.assertEquals(currentState, "OFFLINE");
      }
    }
  }

  @Test
  public void testDisableCancellationWhenDisableResource() throws InterruptedException {
    // Disable cancellation
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.stateTransitionCancelEnabled(false);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    // Reenable resource
    stateCleanUp();
    _setupTool.getClusterManagementTool()
        .enableResource(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, true);

    // Wait for assignment done
    Thread.sleep(2000);

    // Disable the resource
    _setupTool.getClusterManagementTool()
        .enableResource(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, false);

    // Wait for pipeline reaching final stage
    Thread.sleep(2000L);
    ExternalView externalView = _setupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    for (String partition : externalView.getPartitionSet()) {
      Assert.assertTrue(externalView.getStateMap(partition).values().contains("SLAVE"));
    }
  }

  @Test
  public void testRebalancingCauseCancellation() throws InterruptedException {
    // Enable cancellation
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.stateTransitionCancelEnabled(true);
    clusterConfig.setPersistBestPossibleAssignment(true);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    // Reenable resource
    stateCleanUp();
    _setupTool.getClusterManagementTool()
        .enableResource(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, true);

    // Wait for assignment done
    Thread.sleep(2000);
    int numNodesToStart = 10;
    for (int i = 0; i < numNodesToStart; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (_startPort + _numNodes + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }
    MockParticipantManager[] newParticipants = new MockParticipantManager[numNodesToStart];
    registerParticipants(newParticipants, numNodesToStart, _startPort + _numNodes, 1000, -3000000L);

    // Wait for pipeline reaching final stage
    Thread.sleep(2000L);
    int numOfMasters = 0;
    ExternalView externalView = _setupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    for (String partition : externalView.getPartitionSet()) {
      if (externalView.getStateMap(partition).values().contains("MASTER")) {
        numOfMasters++;
      }
    }

    for (MockParticipantManager participant : newParticipants) {
      participant.syncStop();
    }

    // Only partial of state transition has been cancelled
    Assert.assertTrue((numOfMasters > 0 && numOfMasters < _numParitions));
  }

  private void stateCleanUp() {
    InternalMockDelayMSStateModel._cancelledFirstTime = true;
    InternalMockDelayMSStateModel._cancelledStatic = false;
  }

  @StateModelInfo(initialState = "OFFLINE", states = { "MASTER", "SLAVE", "ERROR"
  })
  public static class InternalMockDelayMSStateModel extends StateModel {
    private static Logger LOG = Logger.getLogger(MockDelayMSStateModel.class);
    private long _delay;
    public static boolean _cancelledStatic;
    public static boolean _cancelledFirstTime;

    public InternalMockDelayMSStateModel(long delay) {
      _delay = delay;
      _cancelledStatic = false;
      _cancelledFirstTime = true;
    }

    @Transition(to = "SLAVE", from = "OFFLINE") public void onBecomeSlaveFromOffline(
        Message message, NotificationContext context) {
      if (_delay > 0) {
        try {
          Thread.sleep(_delay);
        } catch (InterruptedException e) {
          LOG.error("Failed to sleep for " + _delay);
        }
      }
      LOG.info("Become SLAVE from OFFLINE");
    }

    @Transition(to = "MASTER", from = "SLAVE") public void onBecomeMasterFromSlave(Message message,
        NotificationContext context) throws InterruptedException, HelixRollbackException {
      if (_cancelledFirstTime && _delay < 0) {
        while (!_cancelledStatic) {
          Thread.sleep(Math.abs(1000L));
        }
        _cancelledFirstTime = false;
        throw new HelixRollbackException("EX");
      }
      LOG.error("Become MASTER from SLAVE");
    }

    @Transition(to = "SLAVE", from = "MASTER") public void onBecomeSlaveFromMaster(Message message,
        NotificationContext context) {
      LOG.info("Become Slave from Master");
    }

    @Transition(to = "OFFLINE", from = "SLAVE") public void onBecomeOfflineFromSlave(
        Message message, NotificationContext context) {
      LOG.info("Become OFFLINE from SLAVE");
    }

    @Transition(to = "DROPPED", from = "OFFLINE") public void onBecomeDroppedFromOffline(
        Message message, NotificationContext context) {
      LOG.info("Become DROPPED FROM OFFLINE");
    }

    @Override
    public void cancel() {
      _cancelledStatic = true;
    }

    @Override
    public boolean isCancelled() {
      return _cancelledStatic;
    }
  }

  public class InMockDelayMSStateModelFactory
      extends StateModelFactory<InternalMockDelayMSStateModel> {
    private long _delay;

    @Override
    public InternalMockDelayMSStateModel createNewStateModel(String resourceName,
        String partitionKey) {
      InternalMockDelayMSStateModel model = new InternalMockDelayMSStateModel(_delay);
      return model;
    }

    public InMockDelayMSStateModelFactory setDelay(long delay) {
      _delay = delay;
      return this;
    }
  }

  private void registerParticipants(MockParticipantManager[] participants, int numNodes,
      int startPort, long sleepTime, long delay) throws InterruptedException {
    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
    taskFactoryReg.put(MockTask.TASK_COMMAND, new TaskFactory() {
      @Override
      public Task createNewTask(TaskCallbackContext context) {
        return new MockTask(context);
      }
    });

    for (int i = 0; i < numNodes; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (startPort + i);
      participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);

      // add a state model with non-OFFLINE initial state
      StateMachineEngine stateMach = participants[i].getStateMachineEngine();
      stateMach.registerStateModelFactory("Task",
          new TaskStateModelFactory(participants[i], taskFactoryReg));
      InMockDelayMSStateModelFactory delayFactory =
          new InMockDelayMSStateModelFactory().setDelay(delay);
      stateMach.registerStateModelFactory(MASTER_SLAVE_STATE_MODEL, delayFactory);

      participants[i].syncStart();

      if (sleepTime > 0) {
        Thread.sleep(sleepTime);
      }
    }
  }
}
