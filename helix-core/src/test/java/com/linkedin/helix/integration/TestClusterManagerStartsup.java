package com.linkedin.helix.integration;

import java.util.Date;

import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixAgent;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.HelixAgentFactory;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.agent.zk.ZkClient;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.util.CMUtil;

public class TestClusterManagerStartsup extends ZkStandAloneCMTestBase
{
  void setupCluster() throws HelixException
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
  @BeforeClass()
  public void beforeClass() throws Exception
  {
  	_zkClient = new ZkClient(ZK_ADDR);
  }

  @Override
	@AfterClass()
  public void afterClass()
  {
  	_zkClient.close();
  }

  @Test()
  public void testParticipantStartUp() throws Exception
  {
    setupCluster();
    String controllerMsgPath = CMUtil.getControllerPropertyPath(CLUSTER_NAME, PropertyType.MESSAGES_CONTROLLER);
    _zkClient.deleteRecursive(controllerMsgPath);
    HelixAgent manager = null;;

    try
    {
      manager = HelixAgentFactory.getZKHelixAgent(CLUSTER_NAME,
                                                          "localhost_" + (START_PORT + 1),
                                                          InstanceType.PARTICIPANT,
                                                          ZK_ADDR);
      manager.connect();
      Assert.fail("Should fail on connect() since cluster structure is not set up");
    }
    catch(HelixException e)
    {
      // OK
    }

    if(manager != null)
    {
      AssertJUnit.assertFalse(manager.isConnected());
    }

    try
    {
      manager = HelixAgentFactory.getZKHelixAgent(CLUSTER_NAME,
                                                          "localhost_" + (START_PORT + 3),
                                                          InstanceType.PARTICIPANT,
                                                          ZK_ADDR);
      manager.connect();
      Assert.fail("Should fail on connect() since cluster structure is not set up");
    }
    catch(HelixException e)
    {
      // OK
    }

    if(manager != null)
    {
      AssertJUnit.assertFalse(manager.isConnected());
    }

    setupCluster();
    String stateModelPath = CMUtil.getStateModelDefinitionPath(CLUSTER_NAME);
    _zkClient.deleteRecursive(stateModelPath);

    try
    {
      manager = HelixAgentFactory.getZKHelixAgent(CLUSTER_NAME,
                                                          "localhost_" + (START_PORT + 1),
                                                          InstanceType.PARTICIPANT,
                                                          ZK_ADDR);
      manager.connect();
      Assert.fail("Should fail on connect() since cluster structure is not set up");
    }
    catch(HelixException e)
    {
      // OK
    }
    if(manager != null)
    {
      AssertJUnit.assertFalse(manager.isConnected());
    }

    setupCluster();
    String instanceStatusUpdatePath = CMUtil.getInstancePropertyPath(CLUSTER_NAME, "localhost_" + (START_PORT + 1), PropertyType.STATUSUPDATES);
    _zkClient.deleteRecursive(instanceStatusUpdatePath);

    try
    {
      manager = HelixAgentFactory.getZKHelixAgent(CLUSTER_NAME,
                                                          "localhost_" + (START_PORT + 1),
                                                          InstanceType.PARTICIPANT,
                                                          ZK_ADDR);
      manager.connect();
      Assert.fail("Should fail on connect() since cluster structure is not set up");
    }
    catch(HelixException e)
    {
      // OK
    }
    if(manager != null)
    {
      AssertJUnit.assertFalse(manager.isConnected());
    }

  }
}
