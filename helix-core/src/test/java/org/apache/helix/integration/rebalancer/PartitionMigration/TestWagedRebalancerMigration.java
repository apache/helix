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

import java.util.Collections;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestWagedRebalancerMigration extends TestPartitionMigrationBase {
  ConfigAccessor _configAccessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    _configAccessor = new ConfigAccessor(_gZkClient);
  }

  @DataProvider(name = "stateModels")
  public static Object[][] stateModels() {
    return new Object[][] { { BuiltInStateModelDefinitions.MasterSlave.name(), true },
        { BuiltInStateModelDefinitions.OnlineOffline.name(), true },
        { BuiltInStateModelDefinitions.LeaderStandby.name(), true },
        { BuiltInStateModelDefinitions.MasterSlave.name(), false },
        { BuiltInStateModelDefinitions.OnlineOffline.name(), false },
        { BuiltInStateModelDefinitions.LeaderStandby.name(), false },
    };
  }

  // TODO check the movements in between
  @Test(dataProvider = "stateModels")
  public void testMigrateToWagedRebalancerWhileExpandCluster(String stateModel,
      boolean delayEnabled) throws Exception {
    String db = "Test-DB-" + stateModel;
    if (delayEnabled) {
      createResourceWithDelayedRebalance(CLUSTER_NAME, db, stateModel, _PARTITIONS, _replica,
          _replica - 1, 3000000, CrushRebalanceStrategy.class.getName());
    } else {
      createResourceWithDelayedRebalance(CLUSTER_NAME, db, stateModel, _PARTITIONS, _replica,
          _replica, 0, CrushRebalanceStrategy.class.getName());
    }
    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
    ClusterConfig config = _configAccessor.getClusterConfig(CLUSTER_NAME);
    config.setDelayRebalaceEnabled(delayEnabled);
    config.setRebalanceDelayTime(3000000);
    _configAccessor.setClusterConfig(CLUSTER_NAME, config);

    // add new instance to the cluster
    int numNodes = _participants.size();
    for (int i = numNodes; i < numNodes + NUM_NODE; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      MockParticipantManager participant = createAndStartParticipant(storageNodeName);
      _participants.add(participant);
      Thread.sleep(100);
    }
    Thread.sleep(2000);
    ZkHelixClusterVerifier clusterVerifier =
        new StrictMatchExternalViewVerifier.Builder(CLUSTER_NAME)
            .setResources(Collections.singleton(db)).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(clusterVerifier.verifyByPolling());

    _migrationVerifier =
        new MigrationStateVerifier(Collections.singletonMap(db, idealState), _manager);

    _migrationVerifier.reset();
    _migrationVerifier.start();

    IdealState currentIdealState = _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
    currentIdealState.setRebalancerClassName(WagedRebalancer.class.getName());
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, db, currentIdealState);
    Thread.sleep(2000);
    Assert.assertTrue(clusterVerifier.verifyByPolling());

    Assert.assertFalse(_migrationVerifier.hasLessReplica());
    Assert.assertFalse(_migrationVerifier.hasMoreReplica());

    _migrationVerifier.stop();
  }
}
