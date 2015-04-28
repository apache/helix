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

import java.util.Date;
import java.util.List;

import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockMultiClusterController;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.ZkCallbackHandler;
import org.apache.helix.testutil.TestUtil;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAddNodeAfterControllerStart extends ZkTestBase {
  private static Logger LOG = Logger.getLogger(TestAddNodeAfterControllerStart.class);
  // final String className = getShortClassName();

  @Test
  public void testStandalone() throws Exception {
    String testName = TestUtil.getTestName();
    String clusterName = testName;  // className + "_standalone";
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    final int nodeNr = 5;

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, "localhost", "TestDB", 1, 20, nodeNr - 1,
        3, "MasterSlave", true);

    MockParticipant[] participants = new MockParticipant[nodeNr];
    for (int i = 0; i < nodeNr - 1; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_0");
    controller.syncStart();

    boolean result;
    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, clusterName));
    Assert.assertTrue(result);
    String msgPath =
        PropertyPathConfig.getPath(PropertyType.MESSAGES, clusterName, "localhost_12918");
    result = checkHandlers(controller.getHandlers(), msgPath);
    Assert.assertTrue(result);

    _setupTool.addInstanceToCluster(clusterName, "localhost_12922");
    _setupTool.rebalanceStorageCluster(clusterName, "TestDB0", 3);

    participants[nodeNr - 1] = new MockParticipant(_zkaddr, clusterName, "localhost_12922");
    new Thread(participants[nodeNr - 1]).start();
    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, clusterName));
    Assert.assertTrue(result);
    msgPath = PropertyPathConfig.getPath(PropertyType.MESSAGES, clusterName, "localhost_12922");
    result = checkHandlers(controller.getHandlers(), msgPath);
    Assert.assertTrue(result);

    // clean up
    controller.syncStop();
    for (int i = 0; i < nodeNr; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testMultiCluster() throws Exception {
    String testName = TestUtil.getTestName();
    String clusterName = testName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    // setup grand cluster
    final String grandClusterName = "GRAND_" + clusterName;
    TestHelper.setupCluster(grandClusterName, _zkaddr, 0, "controller", null, 0, 0, 1, 0, null,
        true);

    MockMultiClusterController multiClusterController =
        new MockMultiClusterController(_zkaddr, grandClusterName, "controller_0");
    multiClusterController.syncStart();

    // setup cluster
    _setupTool.addCluster(clusterName, true);
    _setupTool.activateCluster(clusterName, "GRAND_" + clusterName, true); // addCluster2

    boolean result;
    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, "GRAND_" + clusterName));
    Assert.assertTrue(result);

    // add node/resource, and do rebalance
    final int nodeNr = 2;
    for (int i = 0; i < nodeNr - 1; i++) {
      int port = 12918 + i;
      _setupTool.addInstanceToCluster(clusterName, "localhost_" + port);
    }

    _setupTool.addResourceToCluster(clusterName, "TestDB0", 1, "LeaderStandby");
    _setupTool.rebalanceStorageCluster(clusterName, "TestDB0", 1);

    MockParticipant[] participants = new MockParticipant[nodeNr];
    for (int i = 0; i < nodeNr - 1; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, clusterName));
    Assert.assertTrue(result);

    // check if controller_0 has message listener for localhost_12918
    String msgPath =
        PropertyPathConfig.getPath(PropertyType.MESSAGES, clusterName, "localhost_12918");
    int numberOfListeners = ZkTestHelper.numberOfListeners(_zkaddr, msgPath);
    // System.out.println("numberOfListeners(" + msgPath + "): " + numberOfListeners);
    Assert.assertEquals(numberOfListeners, 2); // 1 of participant, and 1 of controller

    _setupTool.addInstanceToCluster(clusterName, "localhost_12919");
    _setupTool.rebalanceStorageCluster(clusterName, "TestDB0", 2);

    participants[nodeNr - 1] = new MockParticipant(_zkaddr, clusterName, "localhost_12919");
    participants[nodeNr - 1].syncStart();
    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, clusterName));
    Assert.assertTrue(result);
    // check if controller_0 has message listener for localhost_12919
    msgPath = PropertyPathConfig.getPath(PropertyType.MESSAGES, clusterName, "localhost_12919");
    numberOfListeners = ZkTestHelper.numberOfListeners(_zkaddr, msgPath);
    // System.out.println("numberOfListeners(" + msgPath + "): " + numberOfListeners);
    Assert.assertEquals(numberOfListeners, 2); // 1 of participant, and 1 of controller

    // clean up
    multiClusterController.syncStop();
    for (int i = 0; i < nodeNr; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  boolean checkHandlers(List<ZkCallbackHandler> handlers, String path) {
    // System.out.println(handlers.size() + " handlers: ");
    for (ZkCallbackHandler handler : handlers) {
      // System.out.println(handler.getPath());
      if (handler.getPath().equals(path)) {
        return true;
      }
    }
    return false;
  }

  // TODO: need to add a test case for ParticipantCodeRunner
}
