package com.linkedin.helix.integration;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.PropertyType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.manager.zk.ZKDataAccessor;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.tools.ClusterStateVerifier;
import com.linkedin.helix.tools.IdealStateCalculatorForStorageNode;

public class TestRenamePartition extends ZkIntegrationTestBase
{
  @Test()
  public void testRenamePartitionAutoIS() throws Exception
  {
    String clusterName = "CLUSTER_" + getShortClassName() + "_auto";
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant start port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            10, // partitions per resource
                            5, // number of nodes
                            3, // replicas
                            "MasterSlave", true); // do rebalance


    startAndVerify(clusterName);
    
    // rename partition name TestDB0_0 tp TestDB0_100
    ZKDataAccessor accessor = new ZKDataAccessor(clusterName, _gZkClient);
    IdealState idealState = accessor.getProperty(IdealState.class, PropertyType.IDEALSTATES, "TestDB0");
    
    List<String> prioList = idealState.getRecord().getListFields().remove("TestDB0_0");
    idealState.getRecord().getListFields().put("TestDB0_100", prioList);
    accessor.setProperty(PropertyType.IDEALSTATES, idealState, "TestDB0");

    boolean result = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }
  
  @Test()
  public void testRenamePartitionCustomIS() throws Exception
  {
    
    String clusterName = "CLUSTER_" + getShortClassName() + "_custom";
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant start port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            10, // partitions per resource
                            5, // number of nodes
                            3, // replicas
                            "MasterSlave", false); // do rebalance

    // calculate idealState
    List<String> instanceNames = Arrays.asList("localhost_12918", "localhost_12919", "localhost_12920",
        "localhost_12921", "localhost_12922");
    ZNRecord destIS = IdealStateCalculatorForStorageNode.calculateIdealState(instanceNames,
        10, 3-1, "TestDB0", "MASTER", "SLAVE");
    IdealState idealState = new IdealState(destIS);
    idealState.setIdealStateMode("CUSTOMIZED");
    idealState.setReplicas("3");
    idealState.setStateModelDefRef("MasterSlave");
    ZKDataAccessor accessor = new ZKDataAccessor(clusterName, _gZkClient);
    accessor.setProperty(PropertyType.IDEALSTATES, idealState, "TestDB0");

    startAndVerify(clusterName);
    
    Map<String, String> stateMap = idealState.getRecord().getMapFields().remove("TestDB0_0");
    idealState.getRecord().getMapFields().put("TestDB0_100", stateMap);
    accessor.setProperty(PropertyType.IDEALSTATES, idealState, "TestDB0");

    boolean result = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }
  
  private void startAndVerify(String clusterName) throws Exception
  {
    MockParticipant[] participants = new MockParticipant[5];

    TestHelper.startController(clusterName, "controller_0", ZK_ADDR, HelixControllerMain.STANDALONE);
    
    // start participants
    for (int i = 0; i < 5; i++)
    {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, null);
      new Thread(participants[i]).start();
    }

    boolean result = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(result);

  }
}
