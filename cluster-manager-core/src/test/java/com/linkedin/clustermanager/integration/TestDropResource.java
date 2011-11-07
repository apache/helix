package com.linkedin.clustermanager.integration;

import java.util.ArrayList;
import java.util.List;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.agent.zk.ZKDataAccessor;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.util.CMUtil;

public class TestDropResource extends ZkStandAloneCMTestBase
{
  @Test (groups = {"integrationTest"})
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
    verifyEmtpyCurrentStateTimeout(_zkClient, CLUSTER_NAME, "MyDB", instanceNames);
    
    
    Thread.sleep(10000);
    //Thread.currentThread().join();
    for (int i = 0; i < NODE_NR; i++)
    {
      verifyEmptyCurrentState(_controllerZkClient, CLUSTER_NAME, 
                              "localhost_" + (START_PORT + i), "MyDB");
    }
    
  }

  
  protected void verifyEmptyCurrentState(ZkClient zkClient, String clusterName, 
                      String instanceName, String dbName)
  {
    ClusterDataAccessor accessor = new ZKDataAccessor(CLUSTER_NAME, zkClient);
    String path = CMUtil
        .getInstancePropertyPath(clusterName, instanceName, PropertyType.CURRENTSTATES);
    
    List<String> subPaths = accessor
        .getChildNames( PropertyType.CURRENTSTATES, instanceName);
    
    for (String previousSessionId : subPaths)
    {
      if (zkClient.exists(path+ "/" + previousSessionId + "/" + dbName))
      {
        ZNRecord previousCurrentState = accessor.getProperty(PropertyType.CURRENTSTATES, instanceName, 
             previousSessionId, dbName);

        AssertJUnit.assertEquals(previousCurrentState.getMapFields().size(), 0);
      }
    }
  }
  
}
