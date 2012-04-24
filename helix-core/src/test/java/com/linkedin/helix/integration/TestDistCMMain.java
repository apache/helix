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

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.TestHelper.StartCMResult;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.tools.ClusterStateVerifier;

public class TestDistCMMain extends ZkDistCMTestBase
{
  private static Logger LOG = Logger.getLogger(TestDistCMMain.class);

  @Test
  public void testDistCMMain() throws Exception
  {
    LOG.info("RUN testDistCMMain() at " + new Date(System.currentTimeMillis()));

    // verifyClusters();
    boolean verifyResult = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CONTROLLER_CLUSTER));
    Assert.assertTrue(verifyResult);
    
    verifyResult = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_PREFIX + "_" + CLASS_NAME + "_0"));
    Assert.assertTrue(verifyResult);

    // add more controllers to controller cluster
    for (int i = 0; i < NODE_NR; i++)
    {
      String controller = CONTROLLER_PREFIX + ":" + (NODE_NR + i);
      _setupTool.addInstanceToCluster(CONTROLLER_CLUSTER, controller);
    }
    _setupTool.rebalanceStorageCluster(CONTROLLER_CLUSTER,
                                       CLUSTER_PREFIX + "_" + CLASS_NAME, 10);

    // start extra cluster controllers in distributed mode
    for (int i = 0; i < 5; i++)
    {
      String controller = CONTROLLER_PREFIX + "_" + (NODE_NR + i);

      StartCMResult result = TestHelper.startController(CONTROLLER_CLUSTER,
                                                               controller, ZK_ADDR,
                                                               HelixControllerMain.DISTRIBUTED);
      _startCMResultMap.put(controller, result);
    }

    verifyResult = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CONTROLLER_CLUSTER));
    Assert.assertTrue(verifyResult);
    
    verifyResult = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_PREFIX + "_" + CLASS_NAME + "_0"));
    Assert.assertTrue(verifyResult);

    for (int i = 0; i < NODE_NR; i++)
    {
      stopCurrentLeader(_zkClient, CONTROLLER_CLUSTER, _startCMResultMap);
      
      verifyResult = ClusterStateVerifier.verify(
          new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CONTROLLER_CLUSTER));
      Assert.assertTrue(verifyResult);
      
      verifyResult = ClusterStateVerifier.verify(
          new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_PREFIX + "_" + CLASS_NAME + "_0"));
      Assert.assertTrue(verifyResult);
    }

    LOG.info("STOP testDistCMMain() at " + new Date(System.currentTimeMillis()));

  }
}
