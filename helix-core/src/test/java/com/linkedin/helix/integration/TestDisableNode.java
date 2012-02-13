package com.linkedin.helix.integration;

import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.manager.zk.ZKHelixAdmin;

public class TestDisableNode extends ZkStandAloneCMTestBase
{

  @Test()
  public void testDisableNode() throws InterruptedException
  {
    ZKHelixAdmin tool = new ZKHelixAdmin(_zkClient);
    tool.enableInstance(CLUSTER_NAME, PARTICIPANT_PREFIX + "_12918", false);
    TestHelper.verifyWithTimeout("verifyBestPossAndExtViewExtended",
                                 ZK_ADDR,
                                 TestHelper.<String>setOf(CLUSTER_NAME),
                                 TestHelper.<String>setOf("TestDB"),
                                 TestHelper.<String>setOf(PARTICIPANT_PREFIX + "_12918"),
                                 null,
                                 null);

    tool.enableInstance(CLUSTER_NAME, PARTICIPANT_PREFIX + "_12918", true);
    verifyCluster();
  }
}
