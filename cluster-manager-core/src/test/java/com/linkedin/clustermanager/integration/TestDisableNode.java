package com.linkedin.clustermanager.integration;

import org.testng.annotations.Test;

import com.linkedin.clustermanager.agent.zk.ZKClusterManagementTool;

public class TestDisableNode extends ZkStandAloneCMTestBase
{

  @Test()
  public void testDisableNode() throws InterruptedException
  {
    ZKClusterManagementTool tool = new ZKClusterManagementTool(_zkClient);
    tool.enableInstance(CLUSTER_NAME, PARTICIPANT_PREFIX + "_12918", false);
    verifyCluster();
  }
}
