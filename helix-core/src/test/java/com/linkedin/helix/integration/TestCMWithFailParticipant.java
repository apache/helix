package com.linkedin.helix.integration;

import java.util.Date;

import org.testng.annotations.Test;

public class TestCMWithFailParticipant extends ZkIntegrationTestBase
{
  // ZkClient _zkClient;
  //
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
  public void testCMWithFailParticipant() throws Exception
  {
    int numResources = 1;
    int numPartitionsPerResource = 10;
    int numInstance = 5;
    int replica = 3;

    String uniqClusterName = "TestFail_" + "rg" + numResources + "_p" + numPartitionsPerResource
        + "_n" + numInstance + "_r" + replica;
    System.out.println("START " + uniqClusterName + " at " + new Date(System.currentTimeMillis()));

    TestDriver.setupCluster(uniqClusterName, ZK_ADDR, numResources, numPartitionsPerResource,
        numInstance, replica);

    for (int i = 0; i < numInstance; i++)
    {
      TestDriver.startDummyParticipant(uniqClusterName, i);
    }
    TestDriver.startController(uniqClusterName);

    TestDriver.stopDummyParticipant(uniqClusterName, 2000, 0);
    TestDriver.verifyCluster(uniqClusterName, 3000, 50 * 1000);
    TestDriver.stopCluster(uniqClusterName);

    System.out.println("END " + uniqClusterName + " at " + new Date(System.currentTimeMillis()));

  }
}
