package com.linkedin.clustermanager.integration;

import java.util.Date;

import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.ClusterManagerFactory;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.tools.ClusterSetup;
import com.linkedin.clustermanager.util.CMUtil;

public class TestClusterManagerStartsup extends ZkStandAloneCMHandler
{
  void setupCluster() throws ClusterManagerException
  {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
    
    String namespace = "/" + CLUSTER_NAME;
    if (_zkClient.exists(namespace))
    {
      _zkClient.deleteRecursive(namespace);
    }
    _setupTool = new ClusterSetup(ZK_ADDR);

    // setup storage cluster
    _setupTool.addCluster(CLUSTER_NAME, true);
    _setupTool.addResourceGroupToCluster(CLUSTER_NAME, TEST_DB, 20, STATE_MODEL);
    for (int i = 0; i < NODE_NR; i++)
    {
      String storageNodeName = "localhost:" + (START_PORT + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, 3);
  }
  
  
  @Override
  @BeforeClass (groups = {"integrationTest"})
  public void beforeClass() throws Exception
  {
  	_zkClient = new ZkClient(ZK_ADDR);
  }
  
  @Override
	@AfterClass
  public void afterClass()
  {
  	_zkClient.close();
  }
  
  @Test (groups = {"integrationTest"})
  public void testParticipantStartUp() throws Exception
  {
    setupCluster();
    String controllerMsgPath = CMUtil.getControllerPropertyPath(CLUSTER_NAME, PropertyType.MESSAGES_CONTROLLER);
    _zkClient.deleteRecursive(controllerMsgPath);
    boolean exceptionThrown = false;
    ClusterManager manager = null;;
    
    try
    {
      manager = ClusterManagerFactory.getZKBasedManagerForParticipant(CLUSTER_NAME, "localhost_" + (START_PORT + 1), ZK_ADDR);
      manager.connect();
    }
    catch(ClusterManagerException e)
    {
      exceptionThrown = true;
      AssertJUnit.assertTrue(e.getMessage().indexOf("Initial cluster structure is not set up for cluster") != -1);
    }
    AssertJUnit.assertTrue(exceptionThrown);
    if(manager != null)
    {
      AssertJUnit.assertFalse(manager.isConnected());
    }
    exceptionThrown = false;
    
    try
    {
      manager = ClusterManagerFactory.getZKBasedManagerForController(CLUSTER_NAME, "localhost_" + (START_PORT + 3), ZK_ADDR);
      manager.connect();
    }
    catch(ClusterManagerException e)
    {
      exceptionThrown = true;
      AssertJUnit.assertTrue(e.getMessage().indexOf("Initial cluster structure is not set up for cluster") != -1);
    }
    AssertJUnit.assertTrue(exceptionThrown);
    exceptionThrown = false;
    if(manager != null)
    {
      AssertJUnit.assertFalse(manager.isConnected());
    }
    
    setupCluster();
    String stateModelPath = CMUtil.getStateModelDefinitionPath(CLUSTER_NAME);
    _zkClient.deleteRecursive(stateModelPath);
    
    try
    {
      manager = ClusterManagerFactory.getZKBasedManagerForParticipant(CLUSTER_NAME, "localhost_" + (START_PORT + 1), ZK_ADDR);
      manager.connect();
    }
    catch(ClusterManagerException e)
    {
      exceptionThrown = true;
      AssertJUnit.assertTrue(e.getMessage().indexOf("Initial cluster structure is not set up for cluster") != -1);
    }
    AssertJUnit.assertTrue(exceptionThrown);
    exceptionThrown = false;
    if(manager != null)
    {
      AssertJUnit.assertFalse(manager.isConnected());
    }
    
    setupCluster();
    String instanceStatusUpdatePath = CMUtil.getInstancePropertyPath(CLUSTER_NAME, "localhost_" + (START_PORT + 1), PropertyType.STATUSUPDATES);
    _zkClient.deleteRecursive(instanceStatusUpdatePath);
    
    try
    {
      manager = ClusterManagerFactory.getZKBasedManagerForParticipant(CLUSTER_NAME, "localhost_" + (START_PORT + 1), ZK_ADDR);
      manager.connect();
    }
    catch(ClusterManagerException e)
    {
      exceptionThrown = true;
      AssertJUnit.assertTrue(e.getMessage().indexOf("Initial cluster structure is not set up for instance") != -1);
    }
    AssertJUnit.assertTrue(exceptionThrown);
    exceptionThrown = false;
    if(manager != null)
    {
      AssertJUnit.assertFalse(manager.isConnected());
    }
  
  }
}
