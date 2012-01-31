package com.linkedin.helix.integration;

import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.agent.zk.ZKClusterManagementTool;

public class TestDisableNode extends ZkStandAloneCMTestBase
{

  @Test()
  public void testDisableNode() throws InterruptedException
  {
    ZKClusterManagementTool tool = new ZKClusterManagementTool(_zkClient);
    tool.enableInstance(CLUSTER_NAME, PARTICIPANT_PREFIX + "_12918", false);
    TestHelper.verifyWithTimeout("verifyBestPossAndExtViewExtended",
                                 "TestDB",
                                 20,
                                 "MasterSlave",
                                 TestHelper.<String>setOf(CLUSTER_NAME),
                                 ZK_ADDR,
                                 TestHelper.<String>setOf(PARTICIPANT_PREFIX + "_12918"),
                                 null,
                                 null);

    tool.enableInstance(CLUSTER_NAME, PARTICIPANT_PREFIX + "_12918", true);
    verifyCluster();
  }
}
