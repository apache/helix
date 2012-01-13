package com.linkedin.clustermanager.integration;

import java.util.Date;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.agent.zk.ZkClient;


public class TestCMWithFailParticipant extends ZkIntegrationTestBase
{
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
  public void testCMWithFailParticipant() throws Exception
  {
    int numDb = 1;
    int numPartitionsPerDb = 10;
    int numNode = 5;
    int replica = 3;

    String uniqTestName = "TestFail_" + "db" + numDb + "_p" + numPartitionsPerDb + "_n"
        + numNode + "_r" + replica;
    System.out.println("START " + uniqTestName + " at " + new Date(System.currentTimeMillis()));

    TestDriver.setupCluster(uniqTestName, _zkClient, numDb, numPartitionsPerDb, numNode, replica);

    for (int i = 0; i < numNode; i++)
    {
      TestDriver.startDummyParticipant(uniqTestName, i);
    }
    TestDriver.startController(uniqTestName);

    TestDriver.stopDummyParticipant(uniqTestName, 2000, 0);
    TestDriver.verifyCluster(uniqTestName, 3000);
    TestDriver.stopCluster(uniqTestName);

    System.out.println("END " + uniqTestName + " at " + new Date(System.currentTimeMillis()));

  }
}
