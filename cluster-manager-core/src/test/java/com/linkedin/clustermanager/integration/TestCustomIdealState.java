package com.linkedin.clustermanager.integration;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.agent.zk.ZkClient;


public class TestCustomIdealState extends ZkIntegrationTestBase
{
  private static Logger LOG = Logger.getLogger(TestCustomIdealState.class);
  ZkClient _zkClient;

  @BeforeClass
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

  @Test
  public void testBasic() throws Exception
  {

    int numDb = 2;
    int numPartitionsPerDb = 100;
    int numNode = 5;
    int replica = 3;

    String uniqTestName = "TestCustomIS_" + "db" + numDb + "_p" + numPartitionsPerDb + "_n"
        + numNode + "_r" + replica + "_basic";
    System.out.println("START " + uniqTestName + " at " + new Date(System.currentTimeMillis()));

    TestDriver.setupClusterWithoutRebalance(uniqTestName, _zkClient, numDb, numPartitionsPerDb, numNode, replica);

    for (int i = 0; i < numNode; i++)
    {
      TestDriver.startDummyParticipant(uniqTestName, i);
    }
    TestDriver.startController(uniqTestName);

    TestDriver.setIdealState(uniqTestName, 2000, 50);
    TestDriver.verifyCluster(uniqTestName, 3000);

    TestDriver.stopCluster(uniqTestName);

    System.out.println("STOP " + uniqTestName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testNonAliveInstances() throws Exception
  {
    int numDb = 2;
    int numPartitionsPerDb = 50;
    int numNode = 5;
    int replica = 3;

    String uniqTestName = "TestCustomIS_" + "db" + numDb + "_p" + numPartitionsPerDb + "_n"
        + numNode + "_r" + replica + "_nonalive";
    System.out.println("START " + uniqTestName + " at " + new Date(System.currentTimeMillis()));

    TestDriver.setupClusterWithoutRebalance(uniqTestName, _zkClient, numDb, numPartitionsPerDb, numNode, replica);

    for (int i = 0; i < numNode/2; i++)
    {
      TestDriver.startDummyParticipant(uniqTestName, i);
    }

    TestDriver.startController(uniqTestName);
    TestDriver.setIdealState(uniqTestName, 0, 100);

    // wait some time for customized ideal state being populated
    Thread.sleep(1000);

    // start the rest of participants after ideal state is set
    for (int i = numNode/2; i < numNode; i++)
    {
      TestDriver.startDummyParticipant(uniqTestName, i);
    }

    TestDriver.verifyCluster(uniqTestName, 4000);

    TestDriver.stopCluster(uniqTestName);

    System.out.println("STOP " + uniqTestName + " at " + new Date(System.currentTimeMillis()));

  }

  // TODO add a test case that verify (in case of node failure) best possible state is a subset of ideal state
}
