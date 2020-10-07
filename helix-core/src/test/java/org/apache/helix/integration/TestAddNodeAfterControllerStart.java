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
import java.util.List;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.ClusterDistributedController;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.CallbackHandler;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAddNodeAfterControllerStart extends ZkTestBase {
  final String className = getShortClassName();

  @Test
  public void testStandalone() throws Exception {
    String clusterName = className + "_standalone";
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    final int nodeNr = 5;

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, "localhost", "TestDB", 1, 20, nodeNr - 1,
        3, "MasterSlave", true);

    MockParticipantManager[] participants = new MockParticipantManager[nodeNr];
    for (int i = 0; i < nodeNr - 1; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    boolean result;
    result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);
    String msgPath = PropertyPathBuilder.instanceMessage(clusterName, "localhost_12918");
    result = checkHandlers(controller.getHandlers(), msgPath);
    Assert.assertTrue(result);

    _gSetupTool.addInstanceToCluster(clusterName, "localhost_12922");
    _gSetupTool.rebalanceStorageCluster(clusterName, "TestDB0", 3);

    participants[nodeNr - 1] = new MockParticipantManager(ZK_ADDR, clusterName, "localhost_12922");
    new Thread(participants[nodeNr - 1]).start();
    result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);
    msgPath = PropertyPathBuilder.instanceMessage(clusterName, "localhost_12922");
    result = checkHandlers(controller.getHandlers(), msgPath);
    Assert.assertTrue(result);

    // clean up
    controller.syncStop();
    for (int i = 0; i < nodeNr; i++) {
      participants[i].syncStop();
    }
    deleteCluster(clusterName);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testDistributed() throws Exception {
    String clusterName = className + "_distributed";
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // setup grand cluster
    final String grandClusterName = "GRAND_" + clusterName;
    TestHelper.setupCluster(grandClusterName, ZK_ADDR, 0, "controller", null, 0, 0, 1, 0, null,
        true);

    ClusterDistributedController distController =
        new ClusterDistributedController(ZK_ADDR, grandClusterName, "controller_0");
    distController.syncStart();

    // setup cluster
    _gSetupTool.addCluster(clusterName, true);
    _gSetupTool.activateCluster(clusterName, "GRAND_" + clusterName, true); // addCluster2

    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(grandClusterName)
            .setZkClient(_gZkClient)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
            .build();
    Assert.assertTrue(verifier.verifyByPolling());

    // add node/resource, and do rebalance
    final int nodeNr = 2;
    for (int i = 0; i < nodeNr - 1; i++) {
      int port = 12918 + i;
      _gSetupTool.addInstanceToCluster(clusterName, "localhost_" + port);
    }

    _gSetupTool.addResourceToCluster(clusterName, "TestDB0", 1, "LeaderStandby");
    _gSetupTool.rebalanceStorageCluster(clusterName, "TestDB0", 1);
    BestPossibleExternalViewVerifier verifier2 =
        new BestPossibleExternalViewVerifier.Builder(clusterName)
            .setZkClient(_gZkClient)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
            .build();
    Assert.assertTrue(verifier2.verifyByPolling());

    MockParticipantManager[] participants = new MockParticipantManager[nodeNr];
    for (int i = 0; i < nodeNr - 1; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    // Make sure new participants are connected
    boolean result = TestHelper.verify(() -> {
      List<String> liveInstances = accessor.getChildNames(accessor.keyBuilder().liveInstances());
      for (int i = 0; i < nodeNr - 1; i++) {
        if (!participants[i].isConnected()
            || !liveInstances.contains(participants[i].getInstanceName())) {
          return false;
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);
    Assert.assertTrue(verifier2.verifyByPolling());

    // check if controller_0 has message listener for localhost_12918
    String msgPath = PropertyPathBuilder.instanceMessage(clusterName, "localhost_12918");
    int numberOfListeners = ZkTestHelper.numberOfListeners(ZK_ADDR, msgPath);
    // System.out.println("numberOfListeners(" + msgPath + "): " + numberOfListeners);
    Assert.assertEquals(numberOfListeners, 2); // 1 of participant, and 1 of controller

    _gSetupTool.addInstanceToCluster(clusterName, "localhost_12919");
    _gSetupTool.rebalanceStorageCluster(clusterName, "TestDB0", 2);

    participants[nodeNr - 1] = new MockParticipantManager(ZK_ADDR, clusterName, "localhost_12919");
    participants[nodeNr - 1].syncStart();

    Assert.assertTrue(verifier2.verifyByPolling());
    // check if controller_0 has message listener for localhost_12919
    msgPath = PropertyPathBuilder.instanceMessage(clusterName, "localhost_12919");
    numberOfListeners = ZkTestHelper.numberOfListeners(ZK_ADDR, msgPath);
    // System.out.println("numberOfListeners(" + msgPath + "): " + numberOfListeners);
    Assert.assertEquals(numberOfListeners, 2); // 1 of participant, and 1 of controller

    // clean up
    distController.syncStop();
    for (int i = 0; i < nodeNr; i++) {
      participants[i].syncStop();
    }

    // Check that things have been cleaned up
    result = TestHelper.verify(() -> {
      if (distController.isConnected()
          || accessor.getPropertyStat(accessor.keyBuilder().controllerLeader()) != null) {
        return false;
      }
      List<String> liveInstances = accessor.getChildNames(accessor.keyBuilder().liveInstances());
      for (int i = 0; i < nodeNr - 1; i++) {
        if (participants[i].isConnected()
            || liveInstances.contains(participants[i].getInstanceName())) {
          return false;
        }
      }
      return true;
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(result);

    deleteCluster(clusterName);
    deleteCluster(grandClusterName);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  private boolean checkHandlers(List<CallbackHandler> handlers, String path) {
    for (CallbackHandler handler : handlers) {
      if (handler.getPath().equals(path)) {
        return true;
      }
    }
    return false;
  }
}
