package com.linkedin.helix.integration;

import java.util.Date;

import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.HelixManagerFactory;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.util.HelixUtil;

public class TestClusterStartsup extends ZkStandAloneCMTestBase
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
    _setupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB, 20, STATE_MODEL);
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
    String controllerMsgPath = HelixUtil.getControllerPropertyPath(CLUSTER_NAME, PropertyType.MESSAGES_CONTROLLER);
    _zkClient.deleteRecursive(controllerMsgPath);
    HelixManager manager = null;;

    try
    {
      manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME,
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
      manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME,
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
    String stateModelPath = HelixUtil.getStateModelDefinitionPath(CLUSTER_NAME);
    _zkClient.deleteRecursive(stateModelPath);

    try
    {
      manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME,
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
    String instanceStatusUpdatePath = HelixUtil.getInstancePropertyPath(CLUSTER_NAME, "localhost_" + (START_PORT + 1), PropertyType.STATUSUPDATES);
    _zkClient.deleteRecursive(instanceStatusUpdatePath);

    try
    {
      manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME,
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
