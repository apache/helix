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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.List;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestClusterInMaintenanceModeWhenReachingMaxPartition extends ZkTestBase {
  private final int NUM_NODE = 5;
  protected static final int START_PORT = 12918;
  protected static final int _PARTITIONS = 5;

  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  protected ClusterControllerManager _controller;

  private List<MockParticipantManager> _participants = new ArrayList<>();
  private ZkHelixClusterVerifier _clusterVerifier;
  private List<String> _testDBs = new ArrayList<>();
  private HelixDataAccessor _dataAccessor;

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
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
            .build();

    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);
    _dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
  }

  private String[] TestStateModels = {
      BuiltInStateModelDefinitions.MasterSlave.name(),
      BuiltInStateModelDefinitions.OnlineOffline.name(),
      BuiltInStateModelDefinitions.LeaderStandby.name()
  };

  @Test
  public void testDisableCluster() throws Exception {
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setMaxPartitionsPerInstance(10);
    configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    int i = 0;
    for (String stateModel : TestStateModels) {
      String db = "Test-DB-" + i++;
      int replica = 3;
      createResourceWithDelayedRebalance(CLUSTER_NAME, db, stateModel, _PARTITIONS, replica,
          replica, -1);
      _testDBs.add(db);
    }

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    MaintenanceSignal maintenanceSignal =
        _dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance());
    Assert.assertNull(maintenanceSignal);

    for (i = 2; i < NUM_NODE; i++) {
      _participants.get(i).syncStop();
    }

    Thread.sleep(1000L);
    maintenanceSignal = _dataAccessor.getProperty(_dataAccessor.keyBuilder().maintenance());
    Assert.assertNotNull(maintenanceSignal);
    Assert.assertNotNull(maintenanceSignal.getReason());
  }

  @AfterClass
  public void afterClass() throws Exception {
    /*
     * shutdown order: 1) disconnect the controller 2) disconnect participants
     */
    _controller.syncStop();
    for (MockParticipantManager participant : _participants) {
      participant.syncStop();
    }
    deleteCluster(CLUSTER_NAME);
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }
}
