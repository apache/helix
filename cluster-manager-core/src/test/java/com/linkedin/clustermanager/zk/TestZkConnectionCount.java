package com.linkedin.clustermanager.zk;

import org.I0Itec.zkclient.ZkConnection;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.util.ZKClientPool;

public class TestZkConnectionCount extends ZkTestBase
{
  private static Logger LOG = Logger.getLogger(TestZkConnectionCount.class);

  @Test
  public void testZkConnectionCount()
  {
    ZkClient zkClient;
    int nrOfConn = ZkClient.getNumberOfConnections();
    System.out.println("Number of zk connections made " + nrOfConn);
    
    ZkConnection zkConn = new ZkConnection(ZK_ADDR);

    zkClient = new ZkClient(zkConn);
    AssertJUnit.assertEquals(nrOfConn + 1, ZkClient.getNumberOfConnections());
    
    zkClient = new ZkClient(ZK_ADDR);
    AssertJUnit.assertEquals(nrOfConn + 2, ZkClient.getNumberOfConnections());
    
    zkClient = ZKClientPool.getZkClient(ZK_ADDR);
    AssertJUnit.assertEquals(nrOfConn + 2, ZkClient.getNumberOfConnections());
  }
  
}
