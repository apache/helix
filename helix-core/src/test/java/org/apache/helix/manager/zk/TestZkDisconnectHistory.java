package org.apache.helix.manager.zk;

import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.ZkTestHelper.TestZkHelixManager;
import org.apache.helix.integration.ZkIntegrationTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkDisconnectHistory extends ZkIntegrationTestBase
{
  @Test
  public void testDisconnectHistory() throws Exception
  {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName;

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            10, // partitions per resource
                            5, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance
    
    
      String instanceName = "localhost_" + (12918 + 0);
      TestZkHelixManager manager =
          new TestZkHelixManager(clusterName,
                                 instanceName,
                                 InstanceType.PARTICIPANT,
                                 ZK_ADDR);
      manager.connect();
      ZkClient zkClient = manager.getZkClient();
      ZkTestHelper.expireSession(zkClient);
      for(int i = 0;i < 4; i++)
      {
        ZkTestHelper.expireSession(zkClient);
        Thread.sleep(500);
        if(i < 5)
        {
          Assert.assertTrue(manager.isConnected());
        }
      }
      ZkTestHelper.disconnectSession(zkClient);
      for(int i = 0; i < 20; i++)
      {
        Thread.sleep(500);
        if(!manager.isConnected()) break;
      }
      Assert.assertFalse(manager.isConnected());
  }
  
  @Test
  public void testDisconnectFlappingWindow() throws Exception
  {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName;

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            10, // partitions per resource
                            5, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance
    
    
      // flapping time window to 5 sec
      System.setProperty("helixmanager.flappingTimeWindow", "10000");
      String instanceName = "localhost_" + (12918 + 1);
      TestZkHelixManager manager2 =
          new TestZkHelixManager(clusterName,
                                 instanceName,
                                 InstanceType.PARTICIPANT,
                                 ZK_ADDR);
      manager2.connect();
      ZkClient zkClient = manager2.getZkClient();
      for(int i = 0;i < 3; i++)
      {
        ZkTestHelper.expireSession(zkClient);
        Thread.sleep(500);
        Assert.assertTrue(manager2.isConnected());
      }
      Thread.sleep(10000);
      // Old entries should be cleaned up
      for(int i = 0;i < 5; i++)
      {
        ZkTestHelper.expireSession(zkClient);
        Thread.sleep(500);
        Assert.assertTrue(manager2.isConnected());
      }
      ZkTestHelper.disconnectSession(zkClient);
      for(int i = 0; i < 20; i++)
      {
        Thread.sleep(500);
        if(!manager2.isConnected()) break;
      }
      Assert.assertFalse(manager2.isConnected());
      
  }
}
