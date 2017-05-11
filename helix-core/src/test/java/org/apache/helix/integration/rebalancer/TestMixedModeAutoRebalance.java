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
import org.apache.helix.ConfigAccessor;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.controller.rebalancer.strategy.MultiRoundCrushRebalanceStrategy;
import org.apache.helix.integration.common.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceConfig;
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

  private ClusterSetup _setupTool = null;
  private List<MockParticipantManager> _participants = new ArrayList<>();
  private int _replica = 3;
  private HelixClusterVerifier _clusterVerifier;
  private List<String> _testDBs = new ArrayList<>();
  private ConfigAccessor _configAccessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }
    _setupTool = new ClusterSetup(_gZkClient);
    _setupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);

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
  public static String [][] stateModels() {
    return new String[][] { {BuiltInStateModelDefinitions.MasterSlave.name()},
        {BuiltInStateModelDefinitions.OnlineOffline.name()},
        {BuiltInStateModelDefinitions.LeaderStandby.name()}
    };
  }

  @Test(dataProvider = "stateModels")
  public void testUserDefinedPreferenceListsInFullAuto(String stateModel)
      throws Exception {
    String db = "Test-DB-" + stateModel;
    createResourceWithDelayedRebalance(CLUSTER_NAME, db, stateModel, _PARTITIONS, _replica,
        _replica, 0, CrushRebalanceStrategy.class.getName());
    IdealState idealState =
        _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
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

    while (userDefinedPartitions.size() > 0) {
      Thread.sleep(100);
      Assert.assertTrue(_clusterVerifier.verify());
      verifyUserDefinedPreferenceLists(db, userDefinedPreferenceLists, userDefinedPartitions);
      removePartitionFromUserDefinedList(db, userDefinedPartitions);
    }
  }

  private void verifyUserDefinedPreferenceLists(String db,
      Map<String, List<String>> userDefinedPreferenceLists, List<String> userDefinedPartitions)
      throws InterruptedException {
    IdealState is = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
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
    IdealState idealState =
        _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
    _setupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, db, idealState);
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
}
