package com.linkedin.helix.integration;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.tools.ClusterStateVerifier;

public class TestDropResource extends ZkStandAloneCMTestBase
{
  @Test()
  public void testDropResource() throws Exception
  {
    // add a resource to be dropped
    _setupTool.addResourceToCluster(CLUSTER_NAME, "MyDB", 6, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "MyDB", 3);

    boolean result = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    _setupTool.dropResourceFromCluster(CLUSTER_NAME, "MyDB");

    TestHelper.verifyWithTimeout("verifyEmptyCurStateAndExtView",
                                 CLUSTER_NAME,
                                 "MyDB",
                                 TestHelper.<String>setOf("localhost_12918", "localhost_12919",
                                                          "localhost_12920", "localhost_12921",
                                                          "localhost_12922"),
                                 ZK_ADDR);
  }
}
