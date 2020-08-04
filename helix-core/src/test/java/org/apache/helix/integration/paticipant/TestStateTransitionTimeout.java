package org.apache.helix.integration.paticipant;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.messaging.handling.MessageHandler.ErrorCode;
import org.apache.helix.mock.participant.MockMSStateModel;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.mock.participant.SleepTransition;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.StateTransitionError;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.MasterNbInExtViewVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestStateTransitionTimeout extends ZkStandAloneCMTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestStateTransitionTimeout.class);

  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    // setup storage cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB, _PARTITIONS, STATE_MODEL);

    for (int i = 0; i < NODE_NR; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, 3);

    // Set the timeout values
    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, TEST_DB);
    String stateTransition = "SLAVE" + "-" + "MASTER" + "_" + Message.Attributes.TIMEOUT;
    idealState.getRecord().setSimpleField(stateTransition, "300");

    String command =
        "-zkSvr " + ZK_ADDR + " -addResourceProperty " + CLUSTER_NAME + " " + TEST_DB + " "
            + stateTransition + " 200";
    ClusterSetup.processCommandLineArgs(command.split(" "));
  }

  @StateModelInfo(initialState = "OFFLINE", states = {
      "MASTER", "SLAVE", "ERROR"
  })
  public static class TimeOutStateModel extends MockMSStateModel {
    boolean _sleep = false;
    StateTransitionError _error;
    int _errorCallcount = 0;

    public TimeOutStateModel(MockTransition transition, boolean sleep) {
      super(transition);
      _sleep = sleep;
    }

    @Transition(to = "MASTER", from = "SLAVE")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context)
        throws InterruptedException {
      LOG.info("Become MASTER from SLAVE");
      if (_transition != null && _sleep) {
        _transition.doTransition(message, context);
      }
    }

    @Override
    public void rollbackOnError(Message message, NotificationContext context,
        StateTransitionError error) {
      _error = error;
      _errorCallcount++;
    }
  }

  public static class SleepStateModelFactory extends StateModelFactory<TimeOutStateModel> {
    Set<String> partitionsToSleep = new HashSet<String>();
    int _sleepTime;

    public SleepStateModelFactory(int sleepTime) {
      _sleepTime = sleepTime;
    }

    public void setPartitions(Collection<String> partitions) {
      partitionsToSleep.addAll(partitions);
    }

    public void addPartition(String partition) {
      partitionsToSleep.add(partition);
    }

    @Override
    public TimeOutStateModel createNewStateModel(String resource, String stateUnitKey) {
      return new TimeOutStateModel(new SleepTransition(_sleepTime),
          partitionsToSleep.contains(stateUnitKey));
    }
  }

  @Test
  public void testStateTransitionTimeOut() throws Exception {
    Map<String, SleepStateModelFactory> factories = new HashMap<String, SleepStateModelFactory>();
    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, TEST_DB);
    for (int i = 0; i < NODE_NR; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      SleepStateModelFactory factory = new SleepStateModelFactory(1000);
      factories.put(instanceName, factory);
      for (String p : idealState.getPartitionSet()) {
        if (idealState.getPreferenceList(p).get(0).equals(instanceName)) {
          factory.addPartition(p);
        }
      }

      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].getStateMachineEngine().registerStateModelFactory("MasterSlave", factory);
      _participants[i].syncStart();
    }
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller =
        new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    boolean result =
        ClusterStateVerifier
            .verifyByPolling(new MasterNbInExtViewVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);
    HelixDataAccessor accessor = _participants[0].getHelixDataAccessor();

    TestHelper.verify(() -> verify(accessor, idealState, factories), 5000);
    Assert.assertTrue(verify(accessor, idealState, factories));
  }

  private boolean verify(HelixDataAccessor accessor, IdealState idealState,
      Map<String, SleepStateModelFactory> factoryMap) {
    Builder kb = accessor.keyBuilder();
    ExternalView ev = accessor.getProperty(kb.externalView(TEST_DB));
    for (String p : idealState.getPartitionSet()) {
      String idealMaster = idealState.getPreferenceList(p).get(0);
      if (!ev.getStateMap(p).get(idealMaster).equals("ERROR")) {
        return false;
      }

      TimeOutStateModel model = factoryMap.get(idealMaster).getStateModel(TEST_DB, p);
      if (model._errorCallcount != 1 || model._error.getCode() != ErrorCode.TIMEOUT) {
        return false;
      }
    }

    return true;
  }
}
