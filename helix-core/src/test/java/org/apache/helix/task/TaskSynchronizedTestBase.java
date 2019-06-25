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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.model.IdealState;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class TaskSynchronizedTestBase extends ZkTestBase {
  protected int _numNodes = 5;
  protected int _startPort = 12918;
  protected int _numPartitions = 20;
  protected int _numReplicas = 3;
  protected int _numDbs = 1;

  protected Boolean _partitionVary = true;
  protected Boolean _instanceGroupTag = false;

  protected ClusterControllerManager _controller;
  protected HelixManager _manager;
  protected TaskDriver _driver;

  protected List<String> _testDbs = new ArrayList<>();

  protected final String MASTER_SLAVE_STATE_MODEL = "MasterSlave";
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + getShortClassName();
  protected MockParticipantManager[] _participants;

  protected ZkHelixClusterVerifier _clusterVerifier;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    _participants = new MockParticipantManager[_numNodes];
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    setupParticipants();
    setupDBs();
    startParticipants();
    createManagers();
    _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient).build();
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }
    if (_manager != null && _manager.isConnected()) {
      _manager.disconnect();
    }
    stopParticipants();
    deleteCluster(CLUSTER_NAME);
  }

  protected void setupDBs() {
    setupDBs(_gSetupTool);
  }

  protected void setupDBs(ClusterSetup clusterSetup) {
    // Set up target db
    if (_numDbs > 1) {
      for (int i = 0; i < _numDbs; i++) {
        int varyNum = _partitionVary ? 10 * i : 0;
        String db = WorkflowGenerator.DEFAULT_TGT_DB + i;
        clusterSetup.addResourceToCluster(CLUSTER_NAME, db, _numPartitions + varyNum,
            MASTER_SLAVE_STATE_MODEL, IdealState.RebalanceMode.FULL_AUTO.toString());
        clusterSetup.rebalanceStorageCluster(CLUSTER_NAME, db, _numReplicas);
        _testDbs.add(db);
      }
    } else {
      if (_instanceGroupTag) {
        clusterSetup.addResourceToCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB,
            _numPartitions, "OnlineOffline", IdealState.RebalanceMode.FULL_AUTO.name());
        IdealState idealState = clusterSetup.getClusterManagementTool()
            .getResourceIdealState(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
        idealState.setInstanceGroupTag("TESTTAG0");
        clusterSetup.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME,
            WorkflowGenerator.DEFAULT_TGT_DB, idealState);
      } else {
        clusterSetup.addResourceToCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB,
            _numPartitions, MASTER_SLAVE_STATE_MODEL, IdealState.RebalanceMode.FULL_AUTO.name());
      }
      clusterSetup.rebalanceStorageCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB,
          _numReplicas);
    }
  }

  protected void setupParticipants() {
    setupParticipants(_gSetupTool);
  }

  protected void setupParticipants(ClusterSetup setupTool) {
    _participants = new MockParticipantManager[_numNodes];
    for (int i = 0; i < _numNodes; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
      if (_instanceGroupTag) {
        setupTool.addInstanceTag(CLUSTER_NAME, storageNodeName, "TESTTAG" + i);
      }
    }
  }

  protected void startParticipants() {
    startParticipants(ZK_ADDR, _numNodes);
  }

  protected void startParticipants(String zkAddr) {
    startParticipants(zkAddr, _numNodes);
  }

  protected void startParticipants(int numNodes) {
    for (int i = 0; i < numNodes; i++) {
      startParticipant(ZK_ADDR, i);
    }
  }

  protected void startParticipants(String zkAddr, int numNodes) {
    for (int i = 0; i < numNodes; i++) {
      startParticipant(zkAddr, i);
    }
  }

  protected void startParticipant(int i) {
    startParticipant(ZK_ADDR, i);
  }

  protected void startParticipant(String zkAddr, int i) {
    Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
    taskFactoryReg.put(MockTask.TASK_COMMAND, MockTask::new);
    String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
    _participants[i] = new MockParticipantManager(zkAddr, CLUSTER_NAME, instanceName);

    // Register a Task state model factory.
    StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
    stateMachine.registerStateModelFactory("Task",
        new TaskStateModelFactory(_participants[i], taskFactoryReg));
    _participants[i].syncStart();
  }

  protected void stopParticipants() {
    for (int i = 0; i < _numNodes; i++) {
      stopParticipant(i);
    }
  }

  protected void stopParticipant(int i) {
    if (_participants.length <= i) {
      throw new HelixException(
          String.format("Can't stop participant %s, only %s participants" + "were set up.", i,
              _participants.length));
    }
    if (_participants[i] != null && _participants[i].isConnected()) {
      _participants[i].syncStop();
    }
  }

  protected void createManagers() throws Exception {
    createManagers(ZK_ADDR, CLUSTER_NAME);
  }

  protected void createManagers(String zkAddr, String clusterName) throws Exception {
    _manager = HelixManagerFactory.getZKHelixManager(clusterName, "Admin",
        InstanceType.ADMINISTRATOR, zkAddr);
    _manager.connect();
    _driver = new TaskDriver(_manager);
  }

  public void setSingleTestEnvironment() {
    _numDbs = 1;
    _numNodes = 1;
    _numPartitions = 1;
    _numReplicas = 1;
  }
}
