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
import org.apache.helix.TestHelper.StartCMResult;
import org.apache.helix.api.State;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.messaging.handling.MessageHandler.ErrorCode;
import org.apache.helix.mock.participant.MockMSStateModel;
import org.apache.helix.mock.participant.MockParticipant;
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
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestStateTransitionTimeout extends ZkStandAloneCMTestBase {
  private static Logger LOG = Logger.getLogger(TestStateTransitionTimeout.class);

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    _zkClient = new ZkClient(ZK_ADDR);
    _zkClient.setZkSerializer(new ZNRecordSerializer());
    String namespace = "/" + CLUSTER_NAME;
    if (_zkClient.exists(namespace)) {
      _zkClient.deleteRecursive(namespace);
    }
    _setupTool = new ClusterSetup(ZK_ADDR);

    // setup storage cluster
    _setupTool.addCluster(CLUSTER_NAME, true);
    _setupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB, _PARTITIONS, STATE_MODEL);

    for (int i = 0; i < NODE_NR; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, 3);

    // Set the timeout values
    IdealState idealState =
        _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, TEST_DB);
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

    @Transition(to = "SLAVE", from = "OFFLINE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
      LOG.info("Become SLAVE from OFFLINE");

    }

    @Transition(to = "MASTER", from = "SLAVE")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context)
        throws InterruptedException {
      LOG.info("Become MASTER from SLAVE");
      if (_transition != null && _sleep) {
        _transition.doTransition(message, context);
      }
    }

    @Transition(to = "SLAVE", from = "MASTER")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
      LOG.info("Become SLAVE from MASTER");
    }

    @Transition(to = "OFFLINE", from = "SLAVE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
      LOG.info("Become OFFLINE from SLAVE");

    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      LOG.info("Become DROPPED from OFFLINE");

    }

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
    public TimeOutStateModel createNewStateModel(String stateUnitKey) {
      return new TimeOutStateModel(new SleepTransition(_sleepTime),
          partitionsToSleep.contains(stateUnitKey));
    }
  }

  @Test
  public void testStateTransitionTimeOut() throws Exception {
    Map<String, SleepStateModelFactory> factories = new HashMap<String, SleepStateModelFactory>();
    MockParticipant[] participants = new MockParticipant[NODE_NR];
    IdealState idealState =
        _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, TEST_DB);
    for (int i = 0; i < NODE_NR; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      SleepStateModelFactory factory = new SleepStateModelFactory(1000);
      factories.put(instanceName, factory);
      for (PartitionId p : idealState.getPartitionIdSet()) {
        if (idealState.getPreferenceList(p).get(0).equals(ParticipantId.from(instanceName))) {
          factory.addPartition(p.stringify());
        }
      }

      participants[i] = new MockParticipant(factory, CLUSTER_NAME, instanceName, ZK_ADDR, null);
      participants[i].syncStart();
    }
    String controllerName = CONTROLLER_PREFIX + "_0";
    StartCMResult startResult =
        TestHelper.startController(CLUSTER_NAME, controllerName, ZK_ADDR,
            HelixControllerMain.STANDALONE);
    _startCMResultMap.put(controllerName, startResult);
    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new MasterNbInExtViewVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);
    HelixDataAccessor accessor = participants[0].getManager().getHelixDataAccessor();

    Builder kb = accessor.keyBuilder();
    ExternalView ev = accessor.getProperty(kb.externalView(TEST_DB));
    for (PartitionId p : idealState.getPartitionIdSet()) {
      ParticipantId idealMaster = idealState.getPreferenceList(p).get(0);
      Assert.assertTrue(ev.getStateMap(p).get(idealMaster).equals(State.from("ERROR")));

      TimeOutStateModel model = factories.get(idealMaster.stringify()).getStateModel(p.stringify());
      Assert.assertEquals(model._errorCallcount, 1);
      Assert.assertEquals(model._error.getCode(), ErrorCode.TIMEOUT);
    }
  }
}
