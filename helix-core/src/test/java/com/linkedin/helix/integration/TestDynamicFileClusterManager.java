package com.linkedin.helix.integration;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.tools.ClusterStateVerifier;

public class TestDynamicFileClusterManager extends FileCMTestBase
{
  private static Logger LOG = Logger.getLogger(TestDynamicFileClusterManager.class);

  @Test()
  public void testDynamicFileClusterManager()
  throws Exception
  {
    System.out.println("RUN testDynamicFileClusterManager() at " + new Date(System.currentTimeMillis()));

    // add a new db
    _mgmtTool.addResource(CLUSTER_NAME, "MyDB", 6, STATE_MODEL);
    rebalanceStorageCluster(CLUSTER_NAME, "MyDB", 1);

    boolean result = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewFileVerifier(ROOT_PATH, CLUSTER_NAME));
    Assert.assertTrue(result);

    // drop db
    _mgmtTool.dropResource(CLUSTER_NAME, "MyDB");

    TestHelper.verifyWithTimeout("verifyEmptyCurStateFile",
                                 CLUSTER_NAME,
                                 "MyDB",
                                 TestHelper.<String>setOf("localhost_12918", "localhost_12919",
                                                          "localhost_12920", "localhost_12921",
                                                          "localhost_12922"),
                                                          _fileStore);

    System.out.println("STOP testDynamicFileClusterManager() at " + new Date(System.currentTimeMillis()));
  }

}
