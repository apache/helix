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

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.helix.TestHelper;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.mock.participant.ErrTransition;
import org.apache.helix.mock.participant.MockParticipant;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestErrorPartition extends ZkIntegrationTestBase {
  @Test()
  public void testErrorPartition() throws Exception {
    String clusterName = getShortClassName();
    MockParticipant[] participants = new MockParticipant[5];

    System.out.println("START testErrorPartition() at " + new Date(System.currentTimeMillis()));
    ZKHelixAdmin tool = new ZKHelixAdmin(_gZkClient);

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, "localhost", "TestDB", 1, 10, 5, 3,
        "MasterSlave", true);

    TestHelper
        .startController(clusterName, "controller_0", ZK_ADDR, HelixControllerMain.STANDALONE);
    for (int i = 0; i < 5; i++) {
      String instanceName = "localhost_" + (12918 + i);

      if (i == 0) {
        Map<String, Set<String>> errPartitions = new HashMap<String, Set<String>>() {
          {
            put("SLAVE-MASTER", TestHelper.setOf("TestDB0_4"));
          }
        };
        participants[i] =
            new MockParticipant(clusterName, instanceName, ZK_ADDR,
                new ErrTransition(errPartitions));
      } else {
        participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR);
      }
      participants[i].syncStart();
      // new Thread(participants[i]).start();
    }

    Map<String, Map<String, String>> errStates = new HashMap<String, Map<String, String>>();
    errStates.put("TestDB0", new HashMap<String, String>());
    errStates.get("TestDB0").put("TestDB0_4", "localhost_12918");
    boolean result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            ZK_ADDR, clusterName, errStates));
    Assert.assertTrue(result);

    Map<String, Set<String>> errorStateMap = new HashMap<String, Set<String>>() {
      {
        put("TestDB0_4", TestHelper.setOf("localhost_12918"));
      }
    };

    // verify "TestDB0_0", "localhost_12918" is in ERROR state
    TestHelper.verifyState(clusterName, ZK_ADDR, errorStateMap, "ERROR");

    // disable a partition on a node with error state
    tool.enablePartition(false, clusterName, "localhost_12918", "TestDB0",
        Arrays.asList("TestDB0_4"));

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            ZK_ADDR, clusterName, errStates));
    Assert.assertTrue(result);

    TestHelper.verifyState(clusterName, ZK_ADDR, errorStateMap, "ERROR");

    // disable a node with error state
    tool.enableInstance(clusterName, "localhost_12918", false);

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            ZK_ADDR, clusterName, errStates));
    Assert.assertTrue(result);

    // make sure after restart stale ERROR state is gone
    tool.enablePartition(true, clusterName, "localhost_12918", "TestDB0",
        Arrays.asList("TestDB0_4"));
    tool.enableInstance(clusterName, "localhost_12918", true);

    participants[0].syncStop();
    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            ZK_ADDR, clusterName));
    Assert.assertTrue(result);
    participants[0] = new MockParticipant(clusterName, "localhost_12918", ZK_ADDR);
    new Thread(participants[0]).start();

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    System.out.println("END testErrorPartition() at " + new Date(System.currentTimeMillis()));
  }
}
