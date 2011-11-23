package com.linkedin.clustermanager.integration;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.agent.zk.ZKClusterManagementTool;

public class TestDisablePartition extends ZkStandAloneCMTestBase
{
  private static Logger LOG = Logger.getLogger(TestDisablePartition.class);

  @Test()
  public void testDisablePartition() throws InterruptedException
  {
    LOG.info("START testDisablePartition() at " + new Date(System.currentTimeMillis()));

    // localhost_12919 is MASTER for TestDB_0
    ZKClusterManagementTool tool = new ZKClusterManagementTool(_zkClient);
    tool.enablePartition(CLUSTER_NAME, "localhost_12919", "TestDB_0", false);
    verifyCluster();

    tool.enablePartition(CLUSTER_NAME, "localhost_12919", "TestDB_0", true);
    verifyCluster();
    LOG.info("STOP testDisablePartition() at " + new Date(System.currentTimeMillis()));

  }

}
