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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.manager.zk.ZKHelixAdmin;
import com.linkedin.helix.tools.ClusterStateVerifier;

public class TestDisablePartition extends ZkStandAloneCMTestBaseWithPropertyServerCheck
{
  private static Logger LOG = Logger.getLogger(TestDisablePartition.class);

  @Test()
  public void testDisablePartition() throws InterruptedException
  {
    LOG.info("START testDisablePartition() at " + new Date(System.currentTimeMillis()));

    // localhost_12919 is MASTER for TestDB_0
    ZKHelixAdmin tool = new ZKHelixAdmin(_zkClient);
    tool.enablePartition(CLUSTER_NAME, "localhost_12919", "TestDB", "TestDB_0", false);
    Map<String, Set<String>> disabledPartMap = new HashMap<String, Set<String>>()
    {
      {
        put("TestDB_0", TestHelper.setOf("localhost_12919"));
      }
    };

    boolean result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    TestHelper.verifyState(CLUSTER_NAME, ZK_ADDR, disabledPartMap, "OFFLINE");

    tool.enablePartition(CLUSTER_NAME, "localhost_12919", "TestDB", "TestDB_0", true);
    
    result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    LOG.info("STOP testDisablePartition() at " + new Date(System.currentTimeMillis()));

  }

}
