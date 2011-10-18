package com.linkedin.clustermanager;

import org.apache.log4j.Logger;

// TODO fix this test
// @Test (groups = {"unitTest"})
public class TestZkConnectionCount extends ZkUnitTestBase
{
  private static Logger LOG = Logger.getLogger(TestZkConnectionCount.class);

  // @Test
  public void testZkConnectionCount()
  {
    /*
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
    */
  }
  
}
