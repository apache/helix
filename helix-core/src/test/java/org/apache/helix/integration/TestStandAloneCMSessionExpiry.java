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
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestStandAloneCMSessionExpiry extends ZkTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestStandAloneCMSessionExpiry.class);

  @Test()
  public void testStandAloneCMSessionExpiry() throws Exception {
    // Logger.getRootLogger().setLevel(Level.DEBUG);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, PARTICIPANT_PREFIX, "TestDB", 1, 20, 5, 3,
        "MasterSlave", true);

    MockParticipantManager[] participants = new MockParticipantManager[5];
    for (int i = 0; i < 5; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    boolean result;
    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // participant session expiry
    MockParticipantManager participantToExpire = participants[1];

    System.out.println("Expire participant session");
    String oldSessionId = participantToExpire.getSessionId();

    ZkTestHelper.expireSession(participantToExpire.getZkClient());
    String newSessionId = participantToExpire.getSessionId();
    System.out.println("oldSessionId: " + oldSessionId + ", newSessionId: " + newSessionId);
    Assert.assertTrue(newSessionId.compareTo(oldSessionId) > 0,
        "Session id should be increased after expiry");

    ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);
    setupTool.addResourceToCluster(clusterName, "TestDB1", 10, "MasterSlave");
    setupTool.rebalanceStorageCluster(clusterName, "TestDB1", 3);

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // controller session expiry
    System.out.println("Expire controller session");
    oldSessionId = controller.getSessionId();
    ZkTestHelper.expireSession(controller.getZkClient());
    newSessionId = controller.getSessionId();
    System.out.println("oldSessionId: " + oldSessionId + ", newSessionId: " + newSessionId);
    Assert.assertTrue(newSessionId.compareTo(oldSessionId) > 0,
        "Session id should be increased after expiry");

    setupTool.addResourceToCluster(clusterName, "TestDB2", 8, "MasterSlave");
    setupTool.rebalanceStorageCluster(clusterName, "TestDB2", 3);

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    // clean up
    System.out.println("Clean up ...");
    // Logger.getRootLogger().setLevel(Level.DEBUG);
    controller.syncStop();
    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

}
