package com.linkedin.helix.manager.zk;

import java.util.Date;

import org.I0Itec.zkclient.ZkConnection;
import org.apache.zookeeper.ZooKeeper;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.InstanceType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.integration.ZkIntegrationTestBase;

public class TestHandleNewSession extends ZkIntegrationTestBase
{
  class TestZKHelixManager extends ZKHelixManager
  {
    public TestZKHelixManager(String clusterName,
                              String instanceName,
                              InstanceType instanceType,
                              String zkConnectString) throws Exception
    {
      super(clusterName, instanceName, instanceType, zkConnectString);
      // TODO Auto-generated constructor stub
    }

    public ZkClient getZkClient()
    {
      return _zkClient;
    }
  }

  @Test
  public void testHandleNewSession() throws Exception
  {
    // Logger.getRootLogger().setLevel(Level.INFO);
    System.out.println("START TestHandleNewSession at "
        + new Date(System.currentTimeMillis()));

    final String clusterName = "TestHandleNewSession";
    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            10, // partitions per resource
                            5, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance

    TestZKHelixManager manager =
        new TestZKHelixManager(clusterName,
                               "localhost_12918",
                               InstanceType.PARTICIPANT,
                               ZK_ADDR);
    manager.connect();
    final ZkClient zkclient = manager.getZkClient();
    String lastSessionId = manager.getSessionId();
    for (int i = 0; i < 3; i++)
    {
      ZkConnection zkConnection = ((ZkConnection) zkclient.getConnection());
      ZooKeeper zk = zkConnection.getZookeeper();
      System.out.println(zk);

      simulateSessionExpiry(zkclient);

      while (manager.getSessionId().equals(lastSessionId))
      {
        Thread.sleep(1000); // ZkHelixManager.handleNewSession() hasn't been called yet
      }
      String sessionId = manager.getSessionId();
      Assert.assertNotSame(sessionId, lastSessionId, "session id should be changed after session expiry");
      lastSessionId = sessionId;
      System.out.println("sessionId: " + sessionId);
      Assert.assertFalse(sessionId.equals("0"),
                         "race condition in zhclient.handleNewSession(). sessionId is not returned yet.");
      
      // TODO: need to test session expiry during handleNewSession()
      Thread.sleep(1000);   // wait ZKHelixManager.handleNewSession() to complete
    }

    // Logger.getRootLogger().setLevel(Level.INFO);
    System.out.println("Disconnecting ...");
    
    manager.disconnect(); 
    
    System.out.println("END TestHandleNewSession at "
        + new Date(System.currentTimeMillis()));

  }
}
