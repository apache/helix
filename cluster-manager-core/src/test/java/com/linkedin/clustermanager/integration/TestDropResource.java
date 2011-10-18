package com.linkedin.clustermanager.integration;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

@Test (groups = {"integrationTest"})
public class TestDropResource extends ZkStandAloneCMHandler
{
  @Test
  public void testDropResource() throws Exception
  {
    // add a resource group to be dropped
    _setupTool.addResourceGroupToCluster(CLUSTER_NAME, "MyDB", 6, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "MyDB", 3);
    // Thread.sleep(10000);
    /*
    boolean result = ClusterStateVerifier.verifyClusterStates(ZK_ADDR, CLUSTER_NAME);
    AssertJUnit.assertTrue(result);
    */
    verifyIdealAndCurrentStateTimeout(CLUSTER_NAME);
    
    _setupTool.dropResourceGroupToCluster(CLUSTER_NAME, "MyDB");
    
    List<String> instanceNames = new ArrayList<String>();
    for (int i = 0; i < NODE_NR; i++)
    {
      instanceNames.add("localhost_" + (START_PORT + i));
    }
    verifyEmtpyCurrentStateTimeout(CLUSTER_NAME, "MyDB", instanceNames);
    
    /*
    Thread.sleep(10000);
    
    for (int i = 0; i < NODE_NR; i++)
    {
      verifyEmptyCurrentState(_controllerZkClient, CLUSTER_NAME, 
                              "localhost_" + (START_PORT + i), "MyDB");
    }
    */
  }

  /*
  protected void verifyEmptyCurrentState(ZkClient zkClient, String clusterName, 
                      String instanceName, String dbName)
  {
    ClusterDataAccessor accessor = new ZKDataAccessor(CLUSTER_NAME, zkClient);
    String path = CMUtil
        .getInstancePropertyPath(clusterName, instanceName, InstancePropertyType.CURRENTSTATES);
    
    List<String> subPaths = accessor
        .getInstancePropertySubPaths(instanceName, InstancePropertyType.CURRENTSTATES);
    
    for (String previousSessionId : subPaths)
    {
      if (zkClient.exists(path+ "/" + previousSessionId + "/" + dbName))
      {
        ZNRecord previousCurrentState = accessor.getInstanceProperty(instanceName, 
                              InstancePropertyType.CURRENTSTATES, previousSessionId, dbName);

        AssertJUnit.assertEquals(previousCurrentState.getMapFields().size(), 0);
      }
    }
  }
  */
}
