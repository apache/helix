package com.linkedin.clustermanager.agent.zk;


import java.util.List;

import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.PropertyPathConfig;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ZkUnitTestBase;

public class TestZkClusterManagementTool extends ZkUnitTestBase
{
	ZkClient _zkClient;
	
	@BeforeClass
	public void beforeClass()
	{
		_zkClient = new ZkClient(ZK_ADDR);
		_zkClient.setZkSerializer(new ZNRecordSerializer());
	}
	
	@AfterClass
	public void afterClass()
	{
		_zkClient.close();
	}
	
  @Test (groups = { "unitTest" })
  public void testZkClusterManagementTool()
  {
    final String clusterName = getShortClassName();
    if (_zkClient.exists("/" + clusterName))
    {
      _zkClient.deleteRecursive("/" + clusterName);
    }
    
    ZKClusterManagementTool tool = new ZKClusterManagementTool(_zkClient);
    tool.addCluster(clusterName, true);
    AssertJUnit.assertTrue(_zkClient.exists("/" + clusterName));
    tool.addCluster(clusterName, true);
    
    List<String> list = tool.getClusters();
    AssertJUnit.assertTrue(list.size() > 0);
    
    boolean exceptionCaught = false;
    try
    {
      tool.addCluster(clusterName, false);
    } catch (ClusterManagerException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);
    
    ZNRecord nodeRecord = new ZNRecord("id0");
    tool.addInstance(clusterName, nodeRecord);
    tool.enableInstance(clusterName, "id0", true);
    String path = PropertyPathConfig.getPath(PropertyType.INSTANCES, clusterName, "id0");
    AssertJUnit.assertTrue(_zkClient.exists(path));
    
    exceptionCaught = false;
    try
    {
      tool.addInstance(clusterName, nodeRecord);
    } catch (ClusterManagerException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);
    
    nodeRecord = tool.getInstanceConfig(clusterName, "id0");
    AssertJUnit.assertEquals(nodeRecord.getId(), "id0");
    
    tool.dropInstance(clusterName, nodeRecord);
    exceptionCaught = false;
    try
    {
      tool.getInstanceConfig(clusterName, "id0");
    } catch (ClusterManagerException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);
    
    exceptionCaught = false;
    try
    {
      tool.dropInstance(clusterName, nodeRecord);
    } catch (ClusterManagerException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);
    
    exceptionCaught = false;
    try
    {
      tool.enableInstance(clusterName, "id0", false);
    } catch (ClusterManagerException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);
    
    ZNRecord stateModelRecord = new ZNRecord("id1");
    tool.addStateModelDef(clusterName, "id1", stateModelRecord);
    path = PropertyPathConfig.getPath(PropertyType.STATEMODELDEFS, clusterName, "id1");
    AssertJUnit.assertTrue(_zkClient.exists(path));
    
    exceptionCaught = false;
    try
    {
      tool.addStateModelDef(clusterName, "id1", stateModelRecord);
    } catch (ClusterManagerException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);

    list = tool.getStateModelDefs(clusterName);
    AssertJUnit.assertEquals(list.size(), 1);
    
    exceptionCaught = false;
    try
    {
      tool.addResourceGroup(clusterName, "resourceGroup", 10, "nonexistStateModelDef");
    } catch (ClusterManagerException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);
    
    tool.addResourceGroup(clusterName, "resourceGroup", 10, "id1");
    list = tool.getResourceGroupsInCluster(clusterName);
    AssertJUnit.assertEquals(list.size(), 1);
    
    tool.addResourceGroup(clusterName, "resourceGroup", 10, "id1");
    list = tool.getResourceGroupsInCluster(clusterName);
    AssertJUnit.assertEquals(list.size(), 1);

    ZNRecord resourceGroupExternalViewRecord = tool.getResourceGroupExternalView(clusterName, "resourceGroup");
    AssertJUnit.assertNull(resourceGroupExternalViewRecord);
  }

}
