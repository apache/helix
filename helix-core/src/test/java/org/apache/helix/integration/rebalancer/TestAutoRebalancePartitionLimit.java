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

import java.util.Date;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.common.zkVerifiers.ExternalViewBalancedVerifierWithMaxPartitions;
import org.apache.helix.controller.rebalancer.AutoRebalancer;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.tools.ClusterStateVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestAutoRebalancePartitionLimit extends ZkStandAloneCMTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestAutoRebalancePartitionLimit.class);

  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    // setup storage cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    _gSetupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB, 100, "OnlineOffline",
        RebalanceMode.FULL_AUTO.name(), 0, 25);

    // Ensure that we are testing the AutoRebalancer.
    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, TEST_DB);
    idealState.setRebalancerClassName(AutoRebalancer.class.getName());
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, TEST_DB, idealState);

    for (int i = 0; i < NODE_NR; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, 1);

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();
  }

  @Test
  public void testAutoRebalanceWithMaxParticipantCapacity() throws InterruptedException {
    HelixManager manager = _controller;
    HelixDataAccessor accessor = manager.getHelixDataAccessor();

    // start dummy participants
    for (int i = 0; i < NODE_NR; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
      Thread.sleep(100);
      boolean result = ClusterStateVerifier.verifyByPolling(
          new ExternalViewBalancedVerifierWithMaxPartitions(_gZkClient, CLUSTER_NAME, TEST_DB), 10000, 100);
      Assert.assertTrue(result);
      ExternalView ev =
          manager.getHelixDataAccessor().getProperty(accessor.keyBuilder().externalView(TEST_DB));
      if (i < 3) {
        Assert.assertEquals(ev.getPartitionSet().size(), 25 * (i + 1));
      } else {
        Assert.assertEquals(ev.getPartitionSet().size(), 100);
      }
    }

    boolean result = ClusterStateVerifier
        .verifyByZkCallback(new ExternalViewBalancedVerifierWithMaxPartitions(_gZkClient, CLUSTER_NAME, TEST_DB));

    Assert.assertTrue(result);
  }

  @Test()
  public void testAutoRebalanceWithMaxPartitionPerNode() {
    HelixManager manager = _controller;
    // kill 1 node
    _participants[0].syncStop();

    boolean result = ClusterStateVerifier
        .verifyByZkCallback(new ExternalViewBalancedVerifierWithMaxPartitions(_gZkClient, CLUSTER_NAME, TEST_DB));
    Assert.assertTrue(result);
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    ExternalView ev =
        manager.getHelixDataAccessor().getProperty(accessor.keyBuilder().externalView(TEST_DB));
    Assert.assertEquals(ev.getPartitionSet().size(), 100);

    _participants[1].syncStop();

    result = ClusterStateVerifier
        .verifyByPolling(new ExternalViewBalancedVerifierWithMaxPartitions(_gZkClient, CLUSTER_NAME, TEST_DB));
    Assert.assertTrue(result);
    ev = manager.getHelixDataAccessor().getProperty(accessor.keyBuilder().externalView(TEST_DB));
    Assert.assertEquals(ev.getPartitionSet().size(), 75);

    // add 2 nodes
    MockParticipantManager[] newParticipants = new MockParticipantManager[2];
    for (int i = 0; i < 2; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (1000 + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);

      String newInstanceName = storageNodeName.replace(':', '_');
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, newInstanceName);
      newParticipants[i] = participant;
      participant.syncStart();
    }

    Assert.assertTrue(ClusterStateVerifier.verifyByPolling(
        new ExternalViewBalancedVerifierWithMaxPartitions(_gZkClient, CLUSTER_NAME, TEST_DB), 10000, 100));

    // Clean up the extra mock participants
    for (MockParticipantManager participant : newParticipants) {
      if (participant != null && participant.isConnected()) {
        participant.syncStop();
      }
    }
  }

}
