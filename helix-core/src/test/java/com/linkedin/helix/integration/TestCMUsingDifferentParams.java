package com.linkedin.helix.integration;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;

public class TestCMUsingDifferentParams extends ZkIntegrationTestBase
{
  private static Logger LOG = Logger.getLogger(TestCMUsingDifferentParams.class);

  // ZkClient _zkClient;
  // @BeforeClass ()
  // public void beforeClass() throws Exception
  // {
  // _zkClient = new ZkClient(ZK_ADDR);
  // _zkClient.setZkSerializer(new ZNRecordSerializer());
  // }
  //
  //
  // @AfterClass
  // public void afterClass()
  // {
  // _zkClient.close();
  // }

  @Test()
  public void testCMUsingDifferentParams() throws Exception
  {
    System.out.println("START " + getShortClassName() + " at "
        + new Date(System.currentTimeMillis()));

    int numResGroups[] = new int[] { 1 }; // , 2}; // , 3, 6};
    int numPartitionsPerResGroups[] = new int[] { 10 }; // , 20, 50, 100}; // ,
                                                        // 1000};
    int numInstances[] = new int[] { 5 }; // , 10}; // , 50, 100, 1000};
    int replicas[] = new int[] { 2 }; // , 3}; //, 4, 5};

    for (int numResGroup : numResGroups)
    {
      for (int numPartitionsPerResGroup : numPartitionsPerResGroups)
      {
        for (int numInstance : numInstances)
        {
          for (int replica : replicas)
          {
            String uniqClusterName = "TestDiffParam_" + "rg" + numResGroup + "_p"
                + numPartitionsPerResGroup + "_n" + numInstance + "_r" + replica;
            System.out.println("START " + uniqClusterName + " at "
                + new Date(System.currentTimeMillis()));

            TestDriver.setupCluster(uniqClusterName, ZK_ADDR, numResGroup,
                numPartitionsPerResGroup, numInstance, replica);

            for (int i = 0; i < numInstance; i++)
            {
              TestDriver.startDummyParticipant(uniqClusterName, i);
            }

            TestDriver.startController(uniqClusterName);
            TestDriver.verifyCluster(uniqClusterName, 1000, 50 * 1000);
            TestDriver.stopCluster(uniqClusterName);

            System.out.println("END " + uniqClusterName + " at "
                + new Date(System.currentTimeMillis()));
          }
        }
      }
    }

    System.out
        .println("END " + getShortClassName() + " at " + new Date(System.currentTimeMillis()));
  }
}
