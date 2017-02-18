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

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.integration.common.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.model.IdealState;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.tools.ClusterSetup;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class TaskSynchronizedTestBase extends ZkIntegrationTestBase {
  protected int _numNodes = 5;
  protected int _startPort = 12918;
  protected int _numParitions = 20;
  protected int _numReplicas = 3;
  protected int _numDbs = 1;

  protected Boolean _partitionVary = true;
  protected Boolean _instanceGroupTag = false;

  protected HelixManager _manager;
  protected TaskDriver _driver;
  protected ClusterSetup _setupTool;

  protected List<String> _testDbs = new ArrayList<String>();

  protected final String MASTER_SLAVE_STATE_MODEL = "MasterSlave";
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + getShortClassName();
  protected final MockParticipantManager[] _participants = new MockParticipantManager[_numNodes];

  @BeforeClass
  public void beforeClass() throws Exception {
    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }

    _setupTool = new ClusterSetup(ZK_ADDR);
    _setupTool.addCluster(CLUSTER_NAME, true);
    setupParticipants();
    setupDBs();
    startParticipants();
    createManagers();
  }


  @AfterClass
  public void afterClass() throws Exception {
    _manager.disconnect();

    for (int i = 0; i < _numNodes; i++) {
      _participants[i].syncStop();
    }
  }

  protected void setupDBs() {
    // Set up target db
    if (_numDbs > 1) {
      for (int i = 0; i < _numDbs; i++) {
        int varyNum = _partitionVary == true ? 10 * i : 0;
        String db = WorkflowGenerator.DEFAULT_TGT_DB + i;
        _setupTool
            .addResourceToCluster(CLUSTER_NAME, db, _numParitions + varyNum, MASTER_SLAVE_STATE_MODEL,
                IdealState.RebalanceMode.FULL_AUTO.toString());
        _setupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _numReplicas);
        _testDbs.add(db);
      }
    } else {
      if (_instanceGroupTag) {
        _setupTool
            .addResourceToCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, _numParitions,
                "OnlineOffline", IdealState.RebalanceMode.FULL_AUTO.name());
        IdealState idealState = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
        idealState.setInstanceGroupTag("TESTTAG0");
        _setupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, idealState);
      } else {
        _setupTool
            .addResourceToCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, _numParitions,
                MASTER_SLAVE_STATE_MODEL, IdealState.RebalanceMode.FULL_AUTO.name());
      }
      _setupTool.rebalanceStorageCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, _numReplicas);
    }
  }

  protected void setupParticipants() {
    for (int i = 0; i < _numNodes; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
      if (_instanceGroupTag) {
        _setupTool.addInstanceTag(CLUSTER_NAME, storageNodeName, "TESTTAG" + i);
      }
    }
  }

  protected void startParticipants() {
    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
    taskFactoryReg.put(MockTask.TASK_COMMAND, new TaskFactory() {
      @Override public Task createNewTask(TaskCallbackContext context) {
        return new MockTask(context);
      }
    });

    // start dummy participants
    for (int i = 0; i < _numNodes; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);

      // Register a Task state model factory.
      StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
      stateMachine.registerStateModelFactory("Task",
          new TaskStateModelFactory(_participants[i], taskFactoryReg));
      _participants[i].syncStart();
    }
  }


  protected void createManagers() throws Exception {
    _manager = HelixManagerFactory
        .getZKHelixManager(CLUSTER_NAME, "Admin", InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();
    _driver = new TaskDriver(_manager);
  }

  public void setSingleTestEnvironment() {
    _numDbs = 1;
    _numNodes = 1;
    _numParitions = 1;
    _numReplicas = 1;
  }
}
