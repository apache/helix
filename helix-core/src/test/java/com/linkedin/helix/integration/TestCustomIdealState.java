package com.linkedin.helix.integration;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.agent.zk.ZNRecordSerializer;
import com.linkedin.helix.agent.zk.ZkClient;
import com.linkedin.helix.tools.ClusterSetup;

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

    int numResGroup = 2;
    int numPartitionsPerResGroup = 100;
    int numInstance = 5;
    int replica = 3;

    String uniqClusterName = "TestCustomIS_" + "rg" + numResGroup + "_p" + numPartitionsPerResGroup
        + "_n" + numInstance + "_r" + replica + "_basic";
    System.out.println("START " + uniqClusterName + " at " + new Date(System.currentTimeMillis()));

    TestDriver.setupClusterWithoutRebalance(uniqClusterName, ZK_ADDR, numResGroup,
        numPartitionsPerResGroup, numInstance, replica);

    for (int i = 0; i < numInstance; i++)
    {
      TestDriver.startDummyParticipant(uniqClusterName, i);
    }
    TestDriver.startController(uniqClusterName);

    TestDriver.setIdealState(uniqClusterName, 2000, 50);
    TestDriver.verifyCluster(uniqClusterName, 3000, 50 * 1000);

    TestDriver.stopCluster(uniqClusterName);

    System.out.println("STOP " + uniqClusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testNonAliveInstances() throws Exception
  {
    int numResGroup = 2;
    int numPartitionsPerResGroup = 50;
    int numInstance = 5;
    int replica = 3;

    String uniqClusterName = "TestCustomIS_" + "rg" + numResGroup + "_p" + numPartitionsPerResGroup
        + "_n" + numInstance + "_r" + replica + "_nonalive";
    System.out.println("START " + uniqClusterName + " at " + new Date(System.currentTimeMillis()));

    TestDriver.setupClusterWithoutRebalance(uniqClusterName, ZK_ADDR, numResGroup,
        numPartitionsPerResGroup, numInstance, replica);

    for (int i = 0; i < numInstance / 2; i++)
    {
      TestDriver.startDummyParticipant(uniqClusterName, i);
    }

    TestDriver.startController(uniqClusterName);
    TestDriver.setIdealState(uniqClusterName, 0, 100);

    // wait some time for customized ideal state being populated
    Thread.sleep(1000);

    // start the rest of participants after ideal state is set
    for (int i = numInstance / 2; i < numInstance; i++)
    {
      TestDriver.startDummyParticipant(uniqClusterName, i);
    }

    TestDriver.verifyCluster(uniqClusterName, 4000, 50 * 1000);

    TestDriver.stopCluster(uniqClusterName);

    System.out.println("STOP " + uniqClusterName + " at " + new Date(System.currentTimeMillis()));

  }

  @Test()
  public void testDrop() throws Exception
  {
    int numResGroup = 2;
    int numPartitionsPerGroup = 50;
    int numInstance = 5;
    int replica = 3;

    String uniqClusterName = "TestCustomIS_" + "rg" + numResGroup + "_p" + numPartitionsPerGroup
        + "_n" + numInstance + "_r" + replica + "_drop";

    System.out.println("START " + uniqClusterName + " at " + new Date(System.currentTimeMillis()));
    TestDriver.setupClusterWithoutRebalance(uniqClusterName, ZK_ADDR, numResGroup,
        numPartitionsPerGroup, numInstance, replica);

    for (int i = 0; i < numInstance; i++)
    {
      TestDriver.startDummyParticipant(uniqClusterName, i);
    }
    TestDriver.startController(uniqClusterName);
    TestDriver.setIdealState(uniqClusterName, 2000, 50);
    TestDriver.verifyCluster(uniqClusterName, 3000, 50 * 1000);

    // drop resource group
    ClusterSetup setup = new ClusterSetup(ZK_ADDR);
    setup.dropResourceGroupToCluster(uniqClusterName, "TestDB0");

    TestHelper.verifyWithTimeout("verifyEmptyCurStateAndExtView", uniqClusterName, "TestDB0",
        TestHelper.<String> setOf("localhost_12918", "localhost_12919", "localhost_12920",
            "localhost_12921", "localhost_12922"), ZK_ADDR);

    TestDriver.stopCluster(uniqClusterName);
    System.out.println("STOP " + uniqClusterName + " at " + new Date(System.currentTimeMillis()));
  }

  // TODO add a test case that verify (in case of node failure) best possible
  // state is a subset of ideal state
}
