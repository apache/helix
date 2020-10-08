package org.apache.helix.integration.spectator;

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

import java.util.List;
import java.util.Set;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.spectator.RoutingTableSnapshot;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestRoutingTableSnapshot extends ZkTestBase {
  private HelixManager _manager;
  private final int NUM_NODES = 10;
  protected int NUM_PARTITIONS = 20;
  protected int NUM_REPLICAS = 3;
  private final int START_PORT = 12918;
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + getShortClassName();
  private MockParticipantManager[] _participants;
  private ClusterControllerManager _controller;

  @BeforeClass
  public void beforeClass() throws Exception {
    _participants = new MockParticipantManager[NUM_NODES];
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    _participants = new MockParticipantManager[NUM_NODES];
    for (int i = 0; i < NUM_NODES; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    for (int i = 0; i < NUM_NODES; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
    }

    _manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin",
        InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();

    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (_manager != null && _manager.isConnected()) {
      _manager.disconnect();
    }
    for (int i = 0; i < NUM_NODES; i++) {
      if (_participants[i] != null && _participants[i].isConnected()) {
        _participants[i].reset();
      }
    }
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }
    deleteCluster(CLUSTER_NAME);
  }

  @Test
  public void testRoutingTableSnapshot() throws InterruptedException {
    RoutingTableProvider routingTableProvider =
        new RoutingTableProvider(_manager, PropertyType.EXTERNALVIEW);

    try {
      String db1 = "TestDB-1";
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, db1, NUM_PARTITIONS, "MasterSlave",
          IdealState.RebalanceMode.FULL_AUTO.name(), CrushEdRebalanceStrategy.class.getName());
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db1, NUM_REPLICAS);

      Thread.sleep(200);
      ZkHelixClusterVerifier clusterVerifier =
          new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient)
              .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
              .build();
      Assert.assertTrue(clusterVerifier.verifyByPolling());

      IdealState idealState1 =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db1);

      RoutingTableSnapshot routingTableSnapshot = routingTableProvider.getRoutingTableSnapshot();
      validateMapping(idealState1, routingTableSnapshot);

      Assert.assertEquals(routingTableSnapshot.getInstanceConfigs().size(), NUM_NODES);
      Assert.assertEquals(routingTableSnapshot.getResources().size(), 1);
      Assert.assertEquals(routingTableSnapshot.getLiveInstances().size(), NUM_NODES);
      Assert.assertEquals(routingTableSnapshot.getExternalViews().size(), 1); // 1 DB

      // add new DB and shutdown an instance
      String db2 = "TestDB-2";
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, db2, NUM_PARTITIONS, "MasterSlave",
          IdealState.RebalanceMode.FULL_AUTO.name(), CrushEdRebalanceStrategy.class.getName());
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db2, NUM_REPLICAS);

      // shutdown an instance
      _participants[0].syncStop();
      Assert.assertTrue(clusterVerifier.verifyByPolling());

      // the original snapshot should not change
      Assert.assertEquals(routingTableSnapshot.getInstanceConfigs().size(), NUM_NODES);
      Assert.assertEquals(routingTableSnapshot.getResources().size(), 1);
      Assert.assertEquals(routingTableSnapshot.getLiveInstances().size(), NUM_NODES);

      RoutingTableSnapshot newRoutingTableSnapshot = routingTableProvider.getRoutingTableSnapshot();

      Assert.assertEquals(newRoutingTableSnapshot.getInstanceConfigs().size(), NUM_NODES);
      Assert.assertEquals(newRoutingTableSnapshot.getResources().size(), 2);
      Assert.assertEquals(newRoutingTableSnapshot.getLiveInstances().size(), NUM_NODES - 1);
      Assert.assertEquals(newRoutingTableSnapshot.getExternalViews().size(), 2); // 2 DBs
    } finally {
      routingTableProvider.shutdown();
    }
  }

  private void validateMapping(IdealState idealState, RoutingTableSnapshot routingTableSnapshot) {
    String db = idealState.getResourceName();
    Set<String> partitions = idealState.getPartitionSet();
    for (String partition : partitions) {
      List<InstanceConfig> masterInsEv =
          routingTableSnapshot.getInstancesForResource(db, partition, "MASTER");
      Assert.assertEquals(masterInsEv.size(), 1);

      List<InstanceConfig> slaveInsEv =
          routingTableSnapshot.getInstancesForResource(db, partition, "SLAVE");
      Assert.assertEquals(slaveInsEv.size(), 2);
    }
  }
}
