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
import com.linkedin.helix.tools.ClusterStateVerifier;

public class TestDistClusterController extends ZkDistCMTestBase
{
  private static Logger LOG = Logger.getLogger(TestDistClusterController.class);

  @Test()
  public void testDistClusterController() throws Exception
  {
    LOG.info("RUN testDistClusterController() at " + new Date(System.currentTimeMillis()));
    stopCurrentLeader(_zkClient, CONTROLLER_CLUSTER, _startCMResultMap);

    // setup the second cluster
    final String secondCluster = CLUSTER_PREFIX + "_" + CLASS_NAME + "_1";
    setupStorageCluster(_setupTool, secondCluster, "MyDB", 10, PARTICIPANT_PREFIX, 13918, STATE_MODEL, 3);

    // start dummy participants on the second cluster
    for (int i = 0; i < NODE_NR; i++)
    {
      String instanceName = "localhost_" + (13918 + i);
      if (_startCMResultMap.get(instanceName) != null)
      {
        LOG.error("fail to start participant:" + instanceName
                  + "(participant with the same name already running)");
      }
      else
      {
        StartCMResult result = TestHelper.startDummyProcess(ZK_ADDR, secondCluster, instanceName);
        _startCMResultMap.put(instanceName, result);
      }
    }

    boolean result = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CONTROLLER_CLUSTER));
    Assert.assertTrue(result);
    
    result = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_PREFIX + "_" + CLASS_NAME + "_0"));
    Assert.assertTrue(result);
    
    result = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_PREFIX + "_" + CLASS_NAME + "_1"));
    Assert.assertTrue(result);
    
    LOG.info("STOP testDistClusterController() at " + new Date(System.currentTimeMillis()));
  }

}
