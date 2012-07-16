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

import com.linkedin.helix.manager.zk.ZKHelixAdmin;
import com.linkedin.helix.tools.ClusterStateVerifier;

public class TestDisableNode extends ZkStandAloneCMTestBaseWithPropertyServerCheck
{

  @Test()
  public void testDisableNode() throws InterruptedException
  {
    ZKHelixAdmin tool = new ZKHelixAdmin(_zkClient);
    tool.enableInstance(CLUSTER_NAME, PARTICIPANT_PREFIX + "_12918", false);

    boolean result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    tool.enableInstance(CLUSTER_NAME, PARTICIPANT_PREFIX + "_12918", true);
    
    result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

  }
}
