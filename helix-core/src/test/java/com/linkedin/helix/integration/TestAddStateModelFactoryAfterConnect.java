package com.linkedin.helix.integration;

import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZKHelixDataAccessor;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.mock.controller.ClusterController;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.mock.storage.MockParticipant.MockMSModelFactory;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.tools.ClusterStateVerifier;
import com.linkedin.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;

public class TestAddStateModelFactoryAfterConnect extends ZkIntegrationTestBase
{
  @Test
  public void testAddStateModelFactoryAfterConnect() throws Exception
  {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 5;

    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    MockParticipant[] participants = new MockParticipant[n];

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            10, // partitions per resource
                            n, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance

    ClusterController controller =
        new ClusterController(clusterName, "controller_0", ZK_ADDR);
    controller.syncStart();

    // start participants
    for (int i = 0; i < n; i++)
    {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, null);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);

    // add a new idealState without registering message handling factory
    ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);
    setupTool.addResourceToCluster(clusterName, "TestDB1", 16, "MasterSlave");

    ZkBaseDataAccessor<ZNRecord> baseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    Builder keyBuilder = accessor.keyBuilder();
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB1"));
    idealState.setStateModelFactoryName("TestDB1_Factory");
    accessor.setProperty(keyBuilder.idealStates("TestDB1"), idealState);
    setupTool.rebalanceStorageCluster(clusterName, "TestDB1", 3);

    // external view for TestDB1 should be empty
    ExternalView extView = null;
    long start = System.currentTimeMillis();
    while (extView == null)
    {
      Thread.sleep(50);
      extView = accessor.getProperty(keyBuilder.externalView("TestDB1"));

      long now = System.currentTimeMillis();
      if (now - start > 5000)
      {
        Assert.fail("Timeout waiting for an empty external view of TestDB1");
      }
    }
    Assert.assertEquals(extView.getRecord().getMapFields().size(),
                        0,
                        "External view for TestDB1 should be empty since TestDB1 is added without a state model factory");

    // register "TestDB1_Factory" state model factory
    // Logger.getRootLogger().setLevel(Level.INFO);
    for (int i = 0; i < n; i++)
    {
      participants[i].getManager()
                     .getStateMachineEngine()
                     .registerStateModelFactory("MasterSlave",
                                                new MockMSModelFactory(),
                                                "TestDB1_Factory");
    }

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);

    // clean up
    // wait for all zk callbacks done
    Thread.sleep(1000);
    controller.syncStop();
    for (int i = 0; i < 5; i++)
    {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

  }
}
