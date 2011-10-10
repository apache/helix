package com.linkedin.clustermanager;

import java.util.Date;

import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

public class BasicTestUsingTestDriver
{
  private static Logger LOG = Logger.getLogger(BasicTestUsingTestDriver.class);
  
  @Test
  public void testDriverExample() throws Exception
  {
    LOG.info("RUN at " + new Date(System.currentTimeMillis()));
    
    int numDbs[] = new int[] {1};   // , 2};    // , 3, 6};
    int numPartitionsPerDbs[] = new int[] {10, 20, 50, 100};    // , 1000};
    int numNodes[] = new int[] {5}; // , 10, 50, 100, 1000};
    int replicas[] = new int[] {2}; // , 3};  //, 4, 5};
    
    ZkServer zkServer = TestDriver.startZk();
    
    for (int numDb : numDbs)
    {
      for (int numPartitionsPerDb : numPartitionsPerDbs)
      {
        for (int numNode : numNodes)
        {
          for (int replica : replicas)
          {
            String uniqTestName = "test_" + "db" + numDb + "_p" + numPartitionsPerDb 
                + "_n" + numNode + "_r" + replica;
            // System.err.println("START " + uniqTestName + "@" + new Date().getTime());
            LOG.info("START " + uniqTestName + "@" + new Date().getTime());
            
            TestDriver.setupCluster(uniqTestName, numDb, numPartitionsPerDb, numNode, replica);
            TestDriver.startDummyParticipants(uniqTestName, numNode);
            TestDriver.startController(uniqTestName);
            TestDriver.verifyCluster(uniqTestName);
            TestDriver.stopCluster(uniqTestName);
            
            // System.err.println("END " + uniqTestName + "@" + new Date().getTime());
            LOG.info("END " + uniqTestName + "@" + new Date().getTime());
          }
        }
      }
    }

    LOG.info("END at " + new Date(System.currentTimeMillis()));
    
    TestDriver.stopZk(zkServer);
  }
}
