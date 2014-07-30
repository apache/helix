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

import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDropResource extends ZkStandAloneCMTestBase {
  @Test()
  public void testDropResource() throws Exception {
    // add a resource to be dropped
    _setupTool.addResourceToCluster(CLUSTER_NAME, "MyDB", 6, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "MyDB", 3);

    boolean result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, CLUSTER_NAME));
    Assert.assertTrue(result);

    String command = "-zkSvr " + _zkaddr + " -dropResource " + CLUSTER_NAME + " " + "MyDB";
    ClusterSetup.processCommandLineArgs(command.split(" "));

    TestHelper.verifyWithTimeout("verifyEmptyCurStateAndExtView", 30 * 1000, CLUSTER_NAME, "MyDB",
        TestHelper.<String> setOf("localhost_12918", "localhost_12919", "localhost_12920",
            "localhost_12921", "localhost_12922"), _zkaddr);
  }

  @Test()
  public void testDropResourceWhileNodeDead() throws Exception {
    // add a resource to be dropped
    _setupTool.addResourceToCluster(CLUSTER_NAME, "MyDB2", 16, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "MyDB2", 3);

    boolean verifyResult =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, CLUSTER_NAME));
    Assert.assertTrue(verifyResult);

    String hostToKill = "localhost_12920";

    _participants[2].syncStop();
    Thread.sleep(1000);

    String command = "-zkSvr " + _zkaddr + " -dropResource " + CLUSTER_NAME + " " + "MyDB2";
    ClusterSetup.processCommandLineArgs(command.split(" "));

    TestHelper.verifyWithTimeout("verifyEmptyCurStateAndExtView", 30 * 1000, CLUSTER_NAME, "MyDB2",
        TestHelper.<String> setOf("localhost_12918", "localhost_12919",
        /* "localhost_12920", */"localhost_12921", "localhost_12922"), _zkaddr);

    _participants[2] = new MockParticipant(_zkaddr, CLUSTER_NAME, hostToKill);
    _participants[2].syncStart();

    TestHelper.verifyWithTimeout("verifyEmptyCurStateAndExtView", 30 * 1000, CLUSTER_NAME, "MyDB2",
        TestHelper.<String> setOf("localhost_12918", "localhost_12919", "localhost_12920",
            "localhost_12921", "localhost_12922"), _zkaddr);
  }
}
