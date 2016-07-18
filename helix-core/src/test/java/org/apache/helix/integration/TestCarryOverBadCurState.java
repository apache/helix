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

import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.tools.ClusterStateVerifier.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.tools.ClusterStateVerifier.ClusterStateVerifier.MasterNbInExtViewVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCarryOverBadCurState extends ZkIntegrationTestBase {
  @Test
  public void testCarryOverBadCurState() throws Exception {
    System.out.println("START testCarryOverBadCurState at " + new Date(System.currentTimeMillis()));

    String clusterName = getShortClassName();
    MockParticipantManager[] participants = new MockParticipantManager[5];

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    // add a bad current state
    ZNRecord badCurState = new ZNRecord("TestDB0");
    String path = PropertyPathBuilder.instanceCurrentState(clusterName, "localhost_12918", "session_0", "TestDB0");
    _gZkClient.createPersistent(path, true);
    _gZkClient.writeData(path, badCurState);

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    for (int i = 0; i < 5; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new MasterNbInExtViewVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName));
    Assert.assertTrue(result);

    // clean up
    controller.syncStop();
    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }
    System.out.println("END testCarryOverBadCurState at " + new Date(System.currentTimeMillis()));

  }
}
