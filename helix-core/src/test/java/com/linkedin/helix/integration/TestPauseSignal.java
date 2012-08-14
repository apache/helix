package com.linkedin.helix.integration;

import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZKHelixDataAccessor;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.mock.controller.StandaloneController;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.model.PauseSignal;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.tools.ClusterStateVerifier;
import com.linkedin.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;

public class TestPauseSignal extends ZkIntegrationTestBase
{
  @Test()
  public void testPauseSignal() throws Exception
  {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    MockParticipant[] participants = new MockParticipant[5];

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            10, // partitions per resource
                            5, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance

    // start controller
    StandaloneController controller =
        new StandaloneController(clusterName, "controller_0", ZK_ADDR);
    controller.syncStart();

    // start participants
    for (int i = 0; i < 5; i++)
    {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, null);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);

    // pause the cluster and make sure pause is persistent
    ZkClient zkClient = new ZkClient(ZK_ADDR);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    final HelixDataAccessor tmpAccessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));
    tmpAccessor.setProperty(tmpAccessor.keyBuilder().pause(), new PauseSignal("pause"));
    zkClient.close();
    
    // wait for controller to be signaled by pause
    Thread.sleep(1000);

    // add a new resource group
    ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);
    setupTool.addResourceToCluster(clusterName, "TestDB1", 10, "MasterSlave");
    setupTool.rebalanceStorageCluster(clusterName, "TestDB1", 3);

    // make sure TestDB1 external view is empty
    TestHelper.verifyWithTimeout("verifyEmptyCurStateAndExtView",
                                 1000,
                                 clusterName,
                                 "TestDB1",
                                 TestHelper.<String> setOf("localhost_12918",
                                                           "localhost_12919",
                                                           "localhost_12920",
                                                           "localhost_12921",
                                                           "localhost_12922"),
                                 ZK_ADDR);

    // resume controller
    final HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));

    accessor.removeProperty(accessor.keyBuilder().pause());
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);

    // clean up
    for (int i = 0; i < 5; i++)
    {
      participants[i].syncStop();
    }

    Thread.sleep(2000);
    controller.syncStop();

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));
  }
}
