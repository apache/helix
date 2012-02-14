package com.linkedin.helix.integration;

import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;

public class TestDropResource extends ZkStandAloneCMTestBase
{
  @Test()
  public void testDropResource() throws Exception
  {
    // add a resource to be dropped
    _setupTool.addResourceToCluster(CLUSTER_NAME, "MyDB", 6, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "MyDB", 3);
    verifyCluster();
    TestHelper.verifyWithTimeout("verifyBestPossAndExtView",
                                 ZK_ADDR,
                                 TestHelper.<String>setOf(CLUSTER_NAME),
                                 TestHelper.<String>setOf("MyDB"));


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
