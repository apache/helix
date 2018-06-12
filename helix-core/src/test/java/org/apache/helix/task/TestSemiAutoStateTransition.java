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

import java.util.Map;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.mock.participant.MockDelayMSStateModelFactory;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.participant.StateMachineEngine;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestSemiAutoStateTransition extends TaskTestBase {

  protected HelixDataAccessor _accessor;
  protected PropertyKey.Builder _keyBuilder;

  @BeforeClass
  public void beforeClass() throws Exception {
    _participants =  new MockParticipantManager[_numNodes];
    _numParitions = 1;

    _gSetupTool.addCluster(CLUSTER_NAME, true);
    _accessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    _keyBuilder = _accessor.keyBuilder();
    setupParticipants();

    for (int i = 0; i < _numDbs; i++) {
      String db = WorkflowGenerator.DEFAULT_TGT_DB + i;
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, db, _numParitions, MASTER_SLAVE_STATE_MODEL,
          IdealState.RebalanceMode.SEMI_AUTO.toString());
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, _numReplicas);
      _testDbs.add(db);
    }

    startParticipants();
    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();
    Thread.sleep(2000L);

    createManagers();
  }

  @Test public void testOfflineToSecondTopState() throws Exception {
    _participants[0].syncStop();
    Thread.sleep(2000L);

    ExternalView externalView =
        _accessor.getProperty(_keyBuilder.externalView(WorkflowGenerator.DEFAULT_TGT_DB + "0"));
    Map<String, String> stateMap =
        externalView.getStateMap(WorkflowGenerator.DEFAULT_TGT_DB + "0_0");
    Assert.assertEquals("MASTER", stateMap.get(PARTICIPANT_PREFIX + "_" + (_startPort + 1)));
    Assert.assertEquals("SLAVE", stateMap.get(PARTICIPANT_PREFIX + "_" + (_startPort + 2)));

    String instanceName = PARTICIPANT_PREFIX + "_" + _startPort;
    _participants[0] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);

    // add a state model with non-OFFLINE initial state
    StateMachineEngine stateMach = _participants[0].getStateMachineEngine();
    MockDelayMSStateModelFactory delayFactory =
        new MockDelayMSStateModelFactory().setDelay(300000L);
    stateMach.registerStateModelFactory(MASTER_SLAVE_STATE_MODEL, delayFactory);

    _participants[0].syncStart();
    Thread.sleep(2000L);

    externalView =
        _accessor.getProperty(_keyBuilder.externalView(WorkflowGenerator.DEFAULT_TGT_DB + "0"));
    stateMap = externalView.getStateMap(WorkflowGenerator.DEFAULT_TGT_DB + "0_0");
    Assert.assertEquals("OFFLINE", stateMap.get(PARTICIPANT_PREFIX + "_" + _startPort));
    Assert.assertEquals("MASTER", stateMap.get(PARTICIPANT_PREFIX + "_" + (_startPort + 1)));
    Assert.assertEquals("SLAVE", stateMap.get(PARTICIPANT_PREFIX + "_" + (_startPort + 2)));
  }
}
