package org.apache.helix.integration.rebalancer;

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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.controller.rebalancer.strategy.MultiRoundCrushRebalanceStrategy;
import org.apache.helix.controller.rebalancer.util.RebalanceScheduler;
import org.apache.helix.integration.common.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.mock.participant.DummyProcess;
import org.apache.helix.mock.participant.MockMSModelFactory;
import org.apache.helix.mock.participant.MockMSStateModel;
import org.apache.helix.mock.participant.MockSchemataModelFactory;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestMixedModeAutoRebalance extends ZkIntegrationTestBase {
  private final int NUM_NODE = 5;
  private static final int START_PORT = 12918;
  private static final int _PARTITIONS = 5;

  private final String CLASS_NAME = getShortClassName();
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  private ClusterControllerManager _controller;

  private List<MockParticipantManager> _participants = new ArrayList<>();
  private int _replica = 3;
  private HelixClusterVerifier _clusterVerifier;
  private ConfigAccessor _configAccessor;
  private HelixDataAccessor _dataAccessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);

      // start dummy participants
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, storageNodeName);
      participant.syncStart();
      _participants.add(participant);
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR).build();

    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);

    _configAccessor = new ConfigAccessor(_gZkClient);
    _dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
  }

  @DataProvider(name = "stateModels")
  public static Object [][] stateModels() {
    return new Object[][] { {BuiltInStateModelDefinitions.MasterSlave.name(), true},
        {BuiltInStateModelDefinitions.OnlineOffline.name(), true},
        {BuiltInStateModelDefinitions.LeaderStandby.name(), true},
        {BuiltInStateModelDefinitions.MasterSlave.name(), false},
        {BuiltInStateModelDefinitions.OnlineOffline.name(), false},
        {BuiltInStateModelDefinitions.LeaderStandby.name(), false},
    };
  }

  @Test(dataProvider = "stateModels")
  public void testUserDefinedPreferenceListsInFullAuto(
      String stateModel, boolean delayEnabled) throws Exception {
    String db = "Test-DB-" + stateModel;
    if (delayEnabled) {
      createResourceWithDelayedRebalance(CLUSTER_NAME, db, stateModel, _PARTITIONS, _replica,
          _replica - 1, 200, CrushRebalanceStrategy.class.getName());
    } else {
      createResourceWithDelayedRebalance(CLUSTER_NAME, db, stateModel, _PARTITIONS, _replica,
          _replica, 0, CrushRebalanceStrategy.class.getName());
    }
    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
    Map<String, List<String>> userDefinedPreferenceLists = idealState.getPreferenceLists();
    List<String> userDefinedPartitions = new ArrayList<>();
    for (String partition : userDefinedPreferenceLists.keySet()) {
      List<String> preferenceList = new ArrayList<>();
      for (int k = _replica; k >= 0; k--) {
        String instance = _participants.get(k).getInstanceName();
        preferenceList.add(instance);
      }
      userDefinedPreferenceLists.put(partition, preferenceList);
      userDefinedPartitions.add(partition);
    }

    ResourceConfig resourceConfig =
        new ResourceConfig.Builder(db).setPreferenceLists(userDefinedPreferenceLists).build();
    _configAccessor.setResourceConfig(CLUSTER_NAME, db, resourceConfig);

    //TODO: Trigger rebalancer, remove this once Helix controller is listening on resource config changes.
    RebalanceScheduler.invokeRebalance(_dataAccessor, db);

    while (userDefinedPartitions.size() > 0) {
      Thread.sleep(100);
      Assert.assertTrue(_clusterVerifier.verify());
      verifyUserDefinedPreferenceLists(db, userDefinedPreferenceLists, userDefinedPartitions);
      removePartitionFromUserDefinedList(db, userDefinedPartitions);
    }
  }

  @Test
  public void testUserDefinedPreferenceListsInFullAutoWithErrors() throws Exception {
    String db = "Test-DB-1";
    createResourceWithDelayedRebalance(CLUSTER_NAME, db,
        BuiltInStateModelDefinitions.MasterSlave.name(), 5, _replica, _replica, 0,
        CrushRebalanceStrategy.class.getName());

    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
    Map<String, List<String>> userDefinedPreferenceLists = idealState.getPreferenceLists();

    List<String> newNodes = new ArrayList<>();
    for (int i = NUM_NODE; i < NUM_NODE + _replica; i++) {
      String instance = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instance);

      // start dummy participants
      MockParticipantManager participant =
          new TestMockParticipantManager(ZK_ADDR, CLUSTER_NAME, instance);
      participant.syncStart();
      _participants.add(participant);
      newNodes.add(instance);
    }

    List<String> userDefinedPartitions = new ArrayList<>();
    for (String partition : userDefinedPreferenceLists.keySet()) {
      userDefinedPreferenceLists.put(partition, newNodes);
      userDefinedPartitions.add(partition);
    }

    ResourceConfig resourceConfig =
        new ResourceConfig.Builder(db).setPreferenceLists(userDefinedPreferenceLists).build();
    _configAccessor.setResourceConfig(CLUSTER_NAME, db, resourceConfig);

    //TODO: Trigger rebalancer, remove this once Helix controller is listening on resource config changes.
    RebalanceScheduler.invokeRebalance(_dataAccessor, db);

    Thread.sleep(1000);
    ExternalView ev =
        _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
    IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
    validateMinActiveAndTopStateReplica(is, ev, _replica, NUM_NODE);
  }

  private void verifyUserDefinedPreferenceLists(String db,
      Map<String, List<String>> userDefinedPreferenceLists, List<String> userDefinedPartitions)
      throws InterruptedException {
    IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
    for (String p : userDefinedPreferenceLists.keySet()) {
      List<String> userDefined = userDefinedPreferenceLists.get(p);
      List<String> preferenceListInIs = is.getPreferenceList(p);
      if (userDefinedPartitions.contains(p)) {
        Assert.assertTrue(userDefined.equals(preferenceListInIs));
      } else {
        if (userDefined.equals(preferenceListInIs)) {
          System.out.println("Something is not good!");
          Thread.sleep(10000000);
        }
        Assert.assertFalse(userDefined.equals(preferenceListInIs), String
            .format("Partition %s, List in Is: %s, List as defined in config: %s", p, preferenceListInIs,
                userDefined));
      }
    }
  }

  private void removePartitionFromUserDefinedList(String db, List<String> userDefinedPartitions) {
    ResourceConfig resourceConfig = _configAccessor.getResourceConfig(CLUSTER_NAME, db);
    Map<String, List<String>> lists = resourceConfig.getPreferenceLists();
    lists.remove(userDefinedPartitions.get(0));
    resourceConfig.setPreferenceLists(lists);
    userDefinedPartitions.remove(0);
    _configAccessor.setResourceConfig(CLUSTER_NAME, db, resourceConfig);

    //TODO: Touch IS, remove this once Helix controller is listening on resource config changes.
    RebalanceScheduler.invokeRebalance(_dataAccessor, db);
  }

  @AfterClass
  public void afterClass() throws Exception {
    /**
     * shutdown order: 1) disconnect the controller 2) disconnect participants
     */
    _controller.syncStop();
    for (MockParticipantManager participant : _participants) {
      participant.syncStop();
    }
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  public static class TestMockParticipantManager extends MockParticipantManager {
    public TestMockParticipantManager(String zkAddr, String clusterName, String instanceName) {
      super(zkAddr, clusterName, instanceName);
      _msModelFactory = new MockDelayMSStateModelFactory();
    }
  }

  public static class MockDelayMSStateModelFactory extends MockMSModelFactory {
    @Override
    public MockDelayMSStateModel createNewStateModel(String resourceName,
        String partitionKey) {
      MockDelayMSStateModel model = new MockDelayMSStateModel(null);
      return model;
    }
  }

  // mock delay master-slave state model
  @StateModelInfo(initialState = "OFFLINE", states = { "MASTER", "SLAVE", "ERROR" })
  public static class MockDelayMSStateModel extends MockMSStateModel {
    public MockDelayMSStateModel(MockTransition transition) {
      super(transition);
    }

    @Transition(to = "*", from = "*")
    public void generalTransitionHandle(Message message, NotificationContext context) {
      throw new IllegalArgumentException("AAA");
    }
  }
}
