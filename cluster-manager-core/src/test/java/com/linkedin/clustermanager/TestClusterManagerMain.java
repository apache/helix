package com.linkedin.clustermanager;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.tools.ClusterSetup;
import com.linkedin.clustermanager.tools.ClusterStateVerifier;
import com.linkedin.clustermanager.tools.StateModelConfigGenerator;

public class TestClusterManagerMain
{
  private static Logger logger = Logger.getLogger(TestClusterManagerMain.class);
  private static final String _zkAddr = "localhost:2181";
  private static ZkServer _zkServer = null;
  private ClusterSetup _setupTool;
  
  @BeforeClass
  public void beforeClass()
  {
    List<String> namespaces = new ArrayList<String>();
    namespaces.add("/CONTROLLER_CLUSTER");
    for (int i = 0; i < 11; i++)
    {
      String storageClusterNamespace = "/ESPRESSO_STORAGE" + "_" + i;
      namespaces.add(storageClusterNamespace);
    }
    
    _zkServer = TestHelper.startZkSever(_zkAddr, namespaces);
    _setupTool = new ClusterSetup(_zkAddr);

  }
  
  @AfterClass
  public void afterClass()
  {
    TestHelper.stopZkServer(_zkServer);
  }
  
  @Test
  public void testStandaloneMode() throws Exception
  {
    logger.info("Run testStandaloneMode() at " + new Date(System.currentTimeMillis()));
    
    _setupTool.addCluster("ESPRESSO_STORAGE_10", true);
    _setupTool.addResourceGroupToCluster("ESPRESSO_STORAGE_10", "TestDB", 20, "MasterSlave");
    for (int i = 0; i < 5; i++)
    {
      String storageNodeName = "localhost:" + (12918 + i);
      _setupTool.addInstanceToCluster("ESPRESSO_STORAGE_10", storageNodeName);
    }
    _setupTool.rebalanceStorageCluster("ESPRESSO_STORAGE_10", "TestDB", 3);
    
    for (int i = 0; i < 5; i++)
    {
      TestHelper.startDummyProcess(_zkAddr, "ESPRESSO_STORAGE_10", "localhost_" + (12918 + i));
    }
    TestHelper.startClusterController("-zkSvr localhost:2181 -cluster ESPRESSO_STORAGE_10" +
          " -mode STANDALONE -controllerName controller_0");
    TestHelper.startClusterController("-zkSvr localhost:2181 -cluster ESPRESSO_STORAGE_10" +
          " -mode STANDALONE -controllerName controller_1");
    
    Thread.sleep(5000);
    boolean result = ClusterStateVerifier.VerifyClusterStates(_zkAddr, "ESPRESSO_STORAGE_10");
    Assert.assertTrue(result);
    
    logger.info("End testStandaloneMode() at " + new Date(System.currentTimeMillis())); 
  }
  
  @Test
  public void testDistMode() throws Exception
  {
    logger.info("Run testDistMode() at " + new Date(System.currentTimeMillis()));

    // setup storage clusters, ESPRESSO_STORAGE_0 ...
    for (int i = 0; i < 10; i++)
    {
      String clusterName = "ESPRESSO_STORAGE" + "_" + i;
      _setupTool.addCluster(clusterName, true);
    }
    
    _setupTool.addResourceGroupToCluster("ESPRESSO_STORAGE_0", "TestDB", 20, "MasterSlave");
    for (int i = 0; i < 5; i++)
    {
      String storageNodeName = "localhost:" + (12918 + i);
      _setupTool.addInstanceToCluster("ESPRESSO_STORAGE_0", storageNodeName);
    }
    _setupTool.rebalanceStorageCluster("ESPRESSO_STORAGE_0", "TestDB", 3);

    // setup CONTROLLER_CLUSTER
    StateModelConfigGenerator generator = new StateModelConfigGenerator();
    _setupTool.addCluster("CONTROLLER_CLUSTER", false, "LeaderStandby", generator.generateConfigForLeaderStandby());
    _setupTool.addResourceGroupToCluster("CONTROLLER_CLUSTER", "ESPRESSO_STORAGE", 10, "LeaderStandby");
    for (int i = 0; i < 5; i++)
    {
      _setupTool.addInstanceToCluster("CONTROLLER_CLUSTER", "localhost", 8900 + i);
    }
    _setupTool.rebalanceStorageCluster("CONTROLLER_CLUSTER", "ESPRESSO_STORAGE", 3);
    
    
    // start dummy storage node for ESPRESSO_STORAGE_0
    for (int i = 0; i < 5; i++)
    {
      TestHelper.startDummyProcess(_zkAddr, "ESPRESSO_STORAGE_0", "localhost_" + (12918 + i));
    }
    
    // start distributed cluster controllers
    for (int i = 0; i < 5; i++)
    {
      String instanceName = "localhost_" + (8900 + i);
      TestHelper.startClusterController("-zkSvr localhost:2181 -cluster CONTROLLER_CLUSTER -mode DISTRIBUTED"
                                        + " -controllerName " + instanceName);
    }

    Thread.sleep(10000);
    boolean result = ClusterStateVerifier.VerifyClusterStates(_zkAddr, "CONTROLLER_CLUSTER");
    logger.info("CONTROLLER_CLUSTER verification result:" + result);
    Assert.assertTrue(result);
    result = ClusterStateVerifier.VerifyClusterStates(_zkAddr, "ESPRESSO_STORAGE_0");
    logger.info("ESPRESSO_STORAGE_0 verification result:" + result);
    Assert.assertTrue(result);
    
    logger.info("End testDistMode() at " + new Date(System.currentTimeMillis()));
  }
}
