package com.linkedin.clustermanager;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.agent.zk.ZKDataAccessor;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.tools.ClusterStateVerifier;
import com.linkedin.clustermanager.util.CMUtil;

public class TestDropResource extends ZkStandAloneCMHandler
{
  @Test
  public void testDropResource() throws Exception
  {
    // add a resource group to be dropped
    _setupTool.addResourceGroupToCluster(CLUSTER_NAME, "MyDB", 6, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "MyDB", 3);
    Thread.sleep(10000);
    
    boolean result = ClusterStateVerifier.VerifyClusterStates(ZK_ADDR, CLUSTER_NAME);
    Assert.assertTrue(result);
    
    _setupTool.dropResourceGroupToCluster(CLUSTER_NAME, "MyDB");
    Thread.sleep(10000);
    
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
        .getInstancePropertyPath(clusterName, instanceName, InstancePropertyType.CURRENTSTATES);
    
    List<String> subPaths = accessor
        .getInstancePropertySubPaths(instanceName, InstancePropertyType.CURRENTSTATES);
    
    for (String previousSessionId : subPaths)
    {
      if (zkClient.exists(path+ "/" + previousSessionId + "/" + dbName))
      {
        ZNRecord previousCurrentState = accessor.getInstanceProperty(instanceName, 
                              InstancePropertyType.CURRENTSTATES, previousSessionId, dbName);

        Assert.assertEquals(previousCurrentState.getMapFields().size(), 0);
      }
    }
  }
  
}
