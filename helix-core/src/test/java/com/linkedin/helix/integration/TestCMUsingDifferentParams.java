package com.linkedin.helix.integration;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.agent.zk.ZNRecordSerializer;
import com.linkedin.helix.agent.zk.ZkClient;


public class TestCMUsingDifferentParams extends ZkIntegrationTestBase
{
  private static Logger LOG = Logger.getLogger(TestCMUsingDifferentParams.class);

  ZkClient _zkClient;
  @BeforeClass ()
  public void beforeClass() throws Exception
  {
  	_zkClient = new ZkClient(ZK_ADDR);
  	_zkClient.setZkSerializer(new ZNRecordSerializer());
  }


	@AfterClass
  public void afterClass()
  {
  	_zkClient.close();
  }

  @Test ()
  public void testCMUsingDifferentParams() throws Exception
  {
    System.out.println("START " + getShortClassName() + " at "
        + new Date(System.currentTimeMillis()));

    int numDbs[] = new int[] {1};   // , 2};    // , 3, 6};
    int numPartitionsPerDbs[] = new int[] {10}; // , 20, 50, 100};    // , 1000};
    int numNodes[] = new int[] {5}; // , 10}; // , 50, 100, 1000};
    int replicas[] = new int[] {2}; // , 3};  //, 4, 5};

    for (int numDb : numDbs)
    {
      for (int numPartitionsPerDb : numPartitionsPerDbs)
      {
        for (int numNode : numNodes)
        {
          for (int replica : replicas)
          {
            String uniqTestName = "TestDiffParam_" + "db" + numDb + "_p" + numPartitionsPerDb
                + "_n" + numNode + "_r" + replica;
            System.out.println("START " + uniqTestName + " at "
                + new Date(System.currentTimeMillis()));

            TestDriver.setupCluster(uniqTestName, _zkClient, numDb, numPartitionsPerDb, numNode, replica);

            for (int i = 0; i < numNode; i++)
            {
              TestDriver.startDummyParticipant(uniqTestName, i);
            }

            TestDriver.startController(uniqTestName);
            TestDriver.verifyCluster(uniqTestName, 1000);
            TestDriver.stopCluster(uniqTestName);

            System.out.println("END " + uniqTestName + " at "
                + new Date(System.currentTimeMillis()));
          }
        }
      }
    }

    System.out.println("END " + getShortClassName() + " at "
        + new Date(System.currentTimeMillis()));
  }
}
