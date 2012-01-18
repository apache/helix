package com.linkedin.clustermanager.integration;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.TestHelper;

public class TestDynamicFileClusterManager extends FileCMTestBase
{
  private static Logger LOG = Logger.getLogger(TestDynamicFileClusterManager.class);

  @Test()
  public void testDynamicFileClusterManager()
  throws Exception
  {
    System.out.println("RUN testDynamicFileClusterManager() at " + new Date(System.currentTimeMillis()));

    // add a new db
    _mgmtTool.addResourceGroup(CLUSTER_NAME, "MyDB", 6, STATE_MODEL);
    rebalanceStorageCluster(CLUSTER_NAME, "MyDB", 0);

    verifyCluster();
    TestHelper.verifyWithTimeout("verifyBestPossAndExtViewFile",
                                 "MyDB",
                                 6,
                                 "MasterSlave",
                                 TestHelper.<String>setOf(CLUSTER_NAME),
                                 _fileStore);

    // drop db
    _mgmtTool.dropResourceGroup(CLUSTER_NAME, "MyDB");

    TestHelper.verifyWithTimeout("verifyEmptyCurStateFile",
                                 CLUSTER_NAME,
                                 "MyDB",
                                 TestHelper.<String>setOf("localhost_12918", "localhost_12919",
                                                          "localhost_12920", "localhost_12921",
                                                          "localhost_12922"),
                                                          _fileStore);

    System.out.println("STOP testDynamicFileClusterManager() at " + new Date(System.currentTimeMillis()));
//    super.afterClass();
  }

}
