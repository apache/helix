/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.integration;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.TestHelper.StartCMResult;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.tools.ClusterStateVerifier;

public class TestDropResource extends ZkStandAloneCMTestBaseWithPropertyServerCheck
{
  @Test()
  public void testDropResource() throws Exception
  {
    // add a resource to be dropped
    _setupTool.addResourceToCluster(CLUSTER_NAME, "MyDB", 6, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "MyDB", 3);

    boolean result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    String command = "-zkSvr " + ZK_ADDR + " -dropResource " + CLUSTER_NAME + " " + "MyDB";
    ClusterSetup.processCommandLineArgs(command.split(" "));

    TestHelper.verifyWithTimeout("verifyEmptyCurStateAndExtView",
                                 CLUSTER_NAME,
                                 "MyDB",
                                 TestHelper.<String>setOf("localhost_12918", "localhost_12919",
                                                          "localhost_12920", "localhost_12921",
                                                          "localhost_12922"),
                                 ZK_ADDR);
  }
  
  @Test()
  public void testDropResourceWhileNodeDead() throws Exception
  {
 // add a resource to be dropped
    _setupTool.addResourceToCluster(CLUSTER_NAME, "MyDB2", 16, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "MyDB2", 3);
    
    boolean verifyResult = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(verifyResult);

    String hostToKill = "localhost_12920";
    
    _startCMResultMap.get(hostToKill)._manager.disconnect();
    Thread.sleep(1000);
    _startCMResultMap.get(hostToKill)._thread.interrupt();
    
    String command = "-zkSvr " + ZK_ADDR + " -dropResource " + CLUSTER_NAME + " " + "MyDB2";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    TestHelper.verifyWithTimeout("verifyEmptyCurStateAndExtView",
        CLUSTER_NAME,
        "MyDB2",
        TestHelper.<String>setOf("localhost_12918", "localhost_12919",
                                 /*"localhost_12920",*/ "localhost_12921",
                                 "localhost_12922"),
        ZK_ADDR);
    
    StartCMResult result =
        TestHelper.startDummyProcess(ZK_ADDR, CLUSTER_NAME, hostToKill);
    _startCMResultMap.put(hostToKill, result);
    
    TestHelper.verifyWithTimeout("verifyEmptyCurStateAndExtView",
        CLUSTER_NAME,
        "MyDB2",
        TestHelper.<String>setOf("localhost_12918", "localhost_12919",
                                 "localhost_12920", "localhost_12921",
                                 "localhost_12922"),
        ZK_ADDR);
  }
}
