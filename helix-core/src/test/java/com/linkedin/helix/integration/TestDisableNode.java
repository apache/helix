package com.linkedin.helix.integration;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.manager.zk.ZKHelixAdmin;
import com.linkedin.helix.tools.ClusterStateVerifier;

public class TestDisableNode extends ZkStandAloneCMTestBase
{

  @Test()
  public void testDisableNode() throws InterruptedException
  {
    ZKHelixAdmin tool = new ZKHelixAdmin(_zkClient);
    tool.enableInstance(CLUSTER_NAME, PARTICIPANT_PREFIX + "_12918", false);

    boolean result = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    tool.enableInstance(CLUSTER_NAME, PARTICIPANT_PREFIX + "_12918", true);
    
    result = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

  }
}
