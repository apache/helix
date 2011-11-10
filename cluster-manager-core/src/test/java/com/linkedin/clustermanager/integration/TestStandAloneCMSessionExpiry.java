package com.linkedin.clustermanager.integration;

import java.io.IOException;
import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.TestHelper;

public class TestStandAloneCMSessionExpiry extends ZkStandAloneCMTestBase
{
  private static Logger LOG = Logger.getLogger(TestStandAloneCMSessionExpiry.class);

  @Test()
  public void testStandAloneCMSessionExpiry()
  throws InterruptedException, IOException
  {
    LOG.info("RUN testStandAloneCMSessionExpiry() at " + new Date(System.currentTimeMillis()));

    simulateSessionExpiry(_participantZkClients[0]);

    _setupTool.addResourceGroupToCluster(CLUSTER_NAME, "MyDB", 10, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "MyDB", 3);
    verifyCluster();
    TestHelper.verifyWithTimeout("verifyBestPossAndExtView",
                                 "MyDB",
                                 10,
                                 "MasterSlave",
                                 TestHelper.<String>setOf(CLUSTER_NAME),
                                 _zkClient);

    simulateSessionExpiry(_controllerZkClient);
    _setupTool.addResourceGroupToCluster(CLUSTER_NAME, "MyDB2", 8, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "MyDB2", 3);
    verifyCluster();
    TestHelper.verifyWithTimeout("verifyBestPossAndExtView",
                                 "MyDB",
                                 10,
                                 "MasterSlave",
                                 TestHelper.<String>setOf(CLUSTER_NAME),
                                 _zkClient);

    TestHelper.verifyWithTimeout("verifyBestPossAndExtView",
                                 "MyDB2",
                                 8,
                                 "MasterSlave",
                                 TestHelper.<String>setOf(CLUSTER_NAME),
                                 _zkClient);

    LOG.info("STOP testStandAloneCMSessionExpiry() at " + new Date(System.currentTimeMillis()));
  }

}
