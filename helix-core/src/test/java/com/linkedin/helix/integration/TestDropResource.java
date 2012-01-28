package com.linkedin.helix.integration;

import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;

public class TestDropResource extends ZkStandAloneCMTestBase
{
  @Test()
  public void testDropResource() throws Exception
  {
    // add a resource group to be dropped
    _setupTool.addResourceGroupToCluster(CLUSTER_NAME, "MyDB", 6, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "MyDB", 3);
    verifyCluster();
    TestHelper.verifyWithTimeout("verifyBestPossAndExtView",
                                 "MyDB",
                                 6,
                                 "MasterSlave",
                                 TestHelper.<String>setOf(CLUSTER_NAME),
                                 _zkClient);


    _setupTool.dropResourceGroupToCluster(CLUSTER_NAME, "MyDB");

    TestHelper.verifyWithTimeout("verifyEmptyCurStateAndExtView",
                                 CLUSTER_NAME,
                                 "MyDB",
                                 TestHelper.<String>setOf("localhost_12918", "localhost_12919",
                                                          "localhost_12920", "localhost_12921",
                                                          "localhost_12922"),
                                 _zkClient);
  }
}
