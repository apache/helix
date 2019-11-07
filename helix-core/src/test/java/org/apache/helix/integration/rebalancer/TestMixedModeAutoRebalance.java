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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.mock.participant.MockMSModelFactory;
import org.apache.helix.mock.participant.MockMSStateModel;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestMixedModeAutoRebalance extends ZkTestBase {
  private final int NUM_NODE = 5;
  private static final int START_PORT = 12918;
  private static final int _PARTITIONS = 5;

  private final String CLASS_NAME = getShortClassName();
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  protected static final String DB_NAME = "Test-DB";

  private ClusterControllerManager _controller;
  private List<MockParticipantManager> _participants = new ArrayList<>();
  private int _replica = 3;
  private ZkHelixClusterVerifier _clusterVerifier;
  private ConfigAccessor _configAccessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

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
  }

  @DataProvider(name = "stateModels")
  public static Object [][] stateModels() {
    return new Object[][] { {BuiltInStateModelDefinitions.MasterSlave.name(), true, CrushRebalanceStrategy.class.getName()},
        {BuiltInStateModelDefinitions.OnlineOffline.name(), true, CrushRebalanceStrategy.class.getName()},
        {BuiltInStateModelDefinitions.LeaderStandby.name(), true, CrushRebalanceStrategy.class.getName()},
        {BuiltInStateModelDefinitions.MasterSlave.name(), false, CrushRebalanceStrategy.class.getName()},
        {BuiltInStateModelDefinitions.OnlineOffline.name(), false, CrushRebalanceStrategy.class.getName()},
        {BuiltInStateModelDefinitions.LeaderStandby.name(), false, CrushRebalanceStrategy.class.getName()},
        {BuiltInStateModelDefinitions.MasterSlave.name(), true, CrushEdRebalanceStrategy.class.getName()},
        {BuiltInStateModelDefinitions.OnlineOffline.name(), true, CrushEdRebalanceStrategy.class.getName()}
    };
  }

  protected void createResource(String stateModel, int numPartition, int replica,
      boolean delayEnabled, String rebalanceStrategy) {
    if (delayEnabled) {
      createResourceWithDelayedRebalance(CLUSTER_NAME, DB_NAME, stateModel, numPartition, replica,
          replica - 1, 200, rebalanceStrategy);
    } else {
      createResourceWithDelayedRebalance(CLUSTER_NAME, DB_NAME, stateModel, numPartition, replica,
          replica, 0, rebalanceStrategy);
    }
  }

  @Test(dataProvider = "stateModels")
  public void testUserDefinedPreferenceListsInFullAuto(String stateModel, boolean delayEnabled,
      String rebalanceStrateyName) throws Exception {
    createResource(stateModel, _PARTITIONS, _replica, delayEnabled,
        rebalanceStrateyName);
    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, DB_NAME);
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
        new ResourceConfig.Builder(DB_NAME).setPreferenceLists(userDefinedPreferenceLists).build();
    _configAccessor.setResourceConfig(CLUSTER_NAME, DB_NAME, resourceConfig);

    // TODO remove this sleep after fix https://github.com/apache/helix/issues/526
    Thread.sleep(500);

    Assert.assertTrue(_clusterVerifier.verify(3000));
    verifyUserDefinedPreferenceLists(DB_NAME, userDefinedPreferenceLists, userDefinedPartitions);

    while (userDefinedPartitions.size() > 0) {
      IdealState originIS = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME,
          DB_NAME);
      Set<String> nonUserDefinedPartitions = new HashSet<>(originIS.getPartitionSet());
      nonUserDefinedPartitions.removeAll(userDefinedPartitions);

      removePartitionFromUserDefinedList(DB_NAME, userDefinedPartitions);
      // TODO: Remove wait once we enable the BestPossibleExternalViewVerifier for the WAGED rebalancer.
      Thread.sleep(1000);
      Assert.assertTrue(_clusterVerifier.verify(3000));
      verifyUserDefinedPreferenceLists(DB_NAME, userDefinedPreferenceLists, userDefinedPartitions);
      verifyNonUserDefinedAssignment(DB_NAME, originIS, nonUserDefinedPartitions);
    }
  }

  @Test
  public void testUserDefinedPreferenceListsInFullAutoWithErrors() throws Exception {
    createResource(BuiltInStateModelDefinitions.MasterSlave.name(), 5, _replica,
        false, CrushRebalanceStrategy.class.getName());

    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, DB_NAME);
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
        new ResourceConfig.Builder(DB_NAME).setPreferenceLists(userDefinedPreferenceLists).build();
    _configAccessor.setResourceConfig(CLUSTER_NAME, DB_NAME, resourceConfig);

    TestHelper.verify(() -> {
      ExternalView ev =
          _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, DB_NAME);
      if (ev != null) {
        for (String partition : ev.getPartitionSet()) {
          Map<String, String> stateMap = ev.getStateMap(partition);
          if (stateMap.values().contains("ERROR")) {
            return true;
          }
        }
      }
      return false;
    }, 2000);

    ExternalView ev =
        _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, DB_NAME);
    IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME,
        DB_NAME);
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
          Assert.fail("Something is not good!");
        }
        Assert.assertFalse(userDefined.equals(preferenceListInIs), String
            .format("Partition %s, List in Is: %s, List as defined in config: %s", p, preferenceListInIs,
                userDefined));
      }
    }
  }

  private void verifyNonUserDefinedAssignment(String db, IdealState originIS, Set<String> nonUserDefinedPartitions)
      throws InterruptedException {
    IdealState newIS = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
    Assert.assertEquals(originIS.getPartitionSet(), newIS.getPartitionSet());
    for (String p : newIS.getPartitionSet()) {
      if (nonUserDefinedPartitions.contains(p)) {
        // for non user defined partition, mapping should keep the same
        Assert.assertEquals(newIS.getPreferenceList(p), originIS.getPreferenceList(p));
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
  }

  @AfterMethod
  public void afterMethod() {
    _gSetupTool.getClusterManagementTool().dropResource(CLUSTER_NAME, DB_NAME);
    _clusterVerifier.verify(5000);
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
    deleteCluster(CLUSTER_NAME);
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
