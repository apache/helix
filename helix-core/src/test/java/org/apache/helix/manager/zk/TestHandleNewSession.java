package org.apache.helix.manager.zk;

import java.util.Date;

import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.integration.ZkIntegrationTestBase;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestHandleNewSession extends ZkIntegrationTestBase
{
  @Test
  public void testHandleNewSession() throws Exception
  {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    
    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    
    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            10, // partitions per resource
                            5, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance

    ZKHelixManager manager =
        new ZKHelixManager(clusterName,
                               "localhost_12918",
                               InstanceType.PARTICIPANT,
                               ZK_ADDR);
    manager.connect();
   
    // Logger.getRootLogger().setLevel(Level.INFO);
    String lastSessionId = manager.getSessionId();
    for (int i = 0; i < 3; i++)
    {
      // System.err.println("curSessionId: " + lastSessionId);
      ZkTestHelper.expireSession(manager._zkClient);

      String sessionId = manager.getSessionId();
      Assert.assertTrue(sessionId.compareTo(lastSessionId) > 0, "Session id should be increased after expiry");
      lastSessionId = sessionId;

      // make sure session id is not 0
      Assert.assertFalse(sessionId.equals("0"),
                         "Hit race condition in zhclient.handleNewSession(). sessionId is not returned yet.");
      
      // TODO: need to test session expiry during handleNewSession()
    }

    // Logger.getRootLogger().setLevel(Level.INFO);
    System.out.println("Disconnecting ...");
    manager.disconnect();
    
    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

  }
}
