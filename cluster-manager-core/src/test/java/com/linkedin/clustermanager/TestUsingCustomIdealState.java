package com.linkedin.clustermanager;

import java.util.Date;

import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Logger;

public class TestUsingCustomIdealState
{
  private static Logger logger = Logger.getLogger(TestUsingCustomIdealState.class);

  // @Test
  public void testFrameworkExample() throws Exception
  {
    String uniqTestName = "test_customIS";
    logger.info("START " + uniqTestName + " @ "  + new Date(System.currentTimeMillis()));

    ZkServer zkServer = TestDriver.startZk();

    TestDriver.setupClusterWithoutRebalance(uniqTestName, 1, 128, 10, 3);
    TestDriver.startDummyParticipants(uniqTestName, 10);
    TestDriver.startController(uniqTestName);
    
    TestDriver.randomFailWithCustomIs(uniqTestName, 1, 0, 30, 10000);
    
    TestDriver.verifyCluster(uniqTestName);
    TestDriver.stopCluster(uniqTestName);
 
    logger.info("END " + uniqTestName + " @ " + new Date(System.currentTimeMillis()));
    TestDriver.stopZk(zkServer);
  }

}
