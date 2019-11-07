package org.apache.helix.integration.rebalancer.PartitionMigration;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestFullAutoMigration extends TestPartitionMigrationBase {
  ConfigAccessor _configAccessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    _configAccessor = new ConfigAccessor(_gZkClient);
  }

  @DataProvider(name = "stateModels")
  public static Object [][] stateModels() {
    return new Object[][] { { BuiltInStateModelDefinitions.MasterSlave.name(), true},
        {BuiltInStateModelDefinitions.OnlineOffline.name(), true},
        {BuiltInStateModelDefinitions.LeaderStandby.name(), true},
        {BuiltInStateModelDefinitions.MasterSlave.name(), false},
        {BuiltInStateModelDefinitions.OnlineOffline.name(), false},
        {BuiltInStateModelDefinitions.LeaderStandby.name(), false},
    };
  }

  @Test(dataProvider = "stateModels")
  public void testMigrateToFullAutoWhileExpandCluster(
      String stateModel, boolean delayEnabled) throws Exception {
    String db = "Test-DB-" + stateModel;
    if (delayEnabled) {
      createResourceWithDelayedRebalance(CLUSTER_NAME, db, stateModel, _PARTITIONS, _replica,
          _replica - 1, 200000, CrushRebalanceStrategy.class.getName());
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
      for (int k = _replica; k > 0; k--) {
        String instance = _participants.get(k).getInstanceName();
        preferenceList.add(instance);
      }
      userDefinedPreferenceLists.put(partition, preferenceList);
      userDefinedPartitions.add(partition);
    }

    ResourceConfig resourceConfig =
        new ResourceConfig.Builder(db).setPreferenceLists(userDefinedPreferenceLists).build();
    _configAccessor.setResourceConfig(CLUSTER_NAME, db, resourceConfig);

    // add new instance to the cluster
    int numNodes = _participants.size();
    for (int i = numNodes; i < numNodes + NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      MockParticipantManager participant = createAndStartParticipant(storageNodeName);
      _participants.add(participant);
      Thread.sleep(50);
    }

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    _migrationVerifier =
        new MigrationStateVerifier(Collections.singletonMap(db, idealState), _manager);

    _migrationVerifier.reset();
    _migrationVerifier.start();

    while (userDefinedPartitions.size() > 0) {
      removePartitionFromUserDefinedList(db, userDefinedPartitions);
      Thread.sleep(50);
    }


    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Assert.assertFalse(_migrationVerifier.hasLessReplica());
    Assert.assertFalse(_migrationVerifier.hasMoreReplica());

    _migrationVerifier.stop();
  }

  private void removePartitionFromUserDefinedList(String db, List<String> userDefinedPartitions) {
    ResourceConfig resourceConfig = _configAccessor.getResourceConfig(CLUSTER_NAME, db);
    Map<String, List<String>> lists = resourceConfig.getPreferenceLists();
    lists.remove(userDefinedPartitions.get(0));
    resourceConfig.setPreferenceLists(lists);
    userDefinedPartitions.remove(0);
    _configAccessor.setResourceConfig(CLUSTER_NAME, db, resourceConfig);
  }

  // create test DBs, wait it converged and return externalviews
  protected Map<String, IdealState> createTestDBs(long delayTime) throws InterruptedException {
    Map<String, IdealState> idealStateMap = new HashMap<>();
    int i = 0;
    for (String stateModel : TestStateModels) {
      String db = "Test-DB-" + i++;
      createResourceWithDelayedRebalance(CLUSTER_NAME, db, stateModel, _PARTITIONS, _replica, _minActiveReplica,
          delayTime);
      _testDBs.add(db);
    }
    Thread.sleep(100);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    for (String db : _testDBs) {
      IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      idealStateMap.put(db, is);
    }
    return idealStateMap;
  }
}
