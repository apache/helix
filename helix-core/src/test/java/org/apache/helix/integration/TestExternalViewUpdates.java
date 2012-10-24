package org.apache.helix.integration;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.storage.MockParticipant;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.tools.ClusterStateVerifier.MasterNbInExtViewVerifier;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestExternalViewUpdates extends ZkIntegrationTestBase
{
  @Test
  public void testExternalViewUpdates() throws Exception
  {
    System.out.println("START testExternalViewUpdates at "
        + new Date(System.currentTimeMillis()));

    String clusterName = getShortClassName();
    MockParticipant[] participants = new MockParticipant[5];
    int resourceNb = 10;
    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            resourceNb, // resources
                            1, // partitions per resource
                            5, // number of nodes
                            1, // replicas
                            "MasterSlave",
                            true); // do rebalance

    TestHelper.startController(clusterName,
                               "controller_0",
                               ZK_ADDR,
                               HelixControllerMain.STANDALONE);
    // start participants
    for (int i = 0; i < 5; i++)
    {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, null);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new MasterNbInExtViewVerifier(ZK_ADDR,
                                                                              clusterName));
    Assert.assertTrue(result);

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);

    // need to verify that each ExternalView's version number is 2
    Builder keyBuilder = new Builder(clusterName);
    ZkBaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    String parentPath = keyBuilder.externalViews().getPath();
    List<String> childNames = accessor.getChildNames(parentPath, 0);
    
    List<String> paths = new ArrayList<String>();
    for (String name : childNames)
    {
      paths.add(parentPath + "/" + name);
    }
    
//    Stat[] stats = accessor.getStats(paths);
    for (String path : paths)
    {
      Stat stat = accessor.getStat(path, 0);
      Assert.assertTrue(stat.getVersion() <= 2, "ExternalView should be updated at most 2 times");
    }
    
    // TODO: need stop controller and participants
    
    System.out.println("END testExternalViewUpdates at "
        + new Date(System.currentTimeMillis()));
  }
}
