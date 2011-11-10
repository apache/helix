package com.linkedin.clustermanager.agent.zk;

import java.util.Date;
import java.util.List;

import org.testng.Assert;
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
    System.out.println("START TestZkClusterManagementTool at "
        + new Date(System.currentTimeMillis()));
    _zkClient = new ZkClient(ZK_ADDR);
    _zkClient.setZkSerializer(new ZNRecordSerializer());
  }

  @AfterClass
  public void afterClass()
  {
    _zkClient.close();
    System.out.println("END TestZkClusterManagementTool at "
        + new Date(System.currentTimeMillis()));
  }

  @Test()
  public void testZkClusterManagementTool()
  {
    final String clusterName = getShortClassName();
    if (_zkClient.exists("/" + clusterName))
    {
      _zkClient.deleteRecursive("/" + clusterName);
    }

    ZKClusterManagementTool tool = new ZKClusterManagementTool(_zkClient);
    tool.addCluster(clusterName, true);
    Assert.assertTrue(ZKUtil.isClusterSetup(clusterName, _zkClient));
    tool.addCluster(clusterName, true);
    Assert.assertTrue(ZKUtil.isClusterSetup(clusterName, _zkClient));

    List<String> list = tool.getClusters();
    AssertJUnit.assertTrue(list.size() > 0);

    try
    {
      tool.addCluster(clusterName, false);
      Assert.fail("should fail if add an already existing cluster");
    }
    catch (ClusterManagerException e)
    {
      // OK
    }

    ZNRecord nodeRecord = new ZNRecord("id0");
    tool.addInstance(clusterName, nodeRecord);
    tool.enableInstance(clusterName, "id0", true);
    String path = PropertyPathConfig.getPath(PropertyType.INSTANCES, clusterName, "id0");
    AssertJUnit.assertTrue(_zkClient.exists(path));

    try
    {
      tool.addInstance(clusterName, nodeRecord);
      Assert.fail("should fail if add an alredy-existing instance");
    }
    catch (ClusterManagerException e)
    {
      // OK
    }

    nodeRecord = tool.getInstanceConfig(clusterName, "id0");
    AssertJUnit.assertEquals(nodeRecord.getId(), "id0");

    tool.dropInstance(clusterName, nodeRecord);
    try
    {
      tool.getInstanceConfig(clusterName, "id0");
      Assert.fail("should fail if get a non-existent instance");
    }
    catch (ClusterManagerException e)
    {
      // OK
    }

    try
    {
      tool.dropInstance(clusterName, nodeRecord);
      Assert.fail("should fail if drop on a non-existent instance");
    }
    catch (ClusterManagerException e)
    {
      // OK
    }

    try
    {
      tool.enableInstance(clusterName, "id0", false);
      Assert.fail("should fail if enable a non-existent instance");
    }
    catch (ClusterManagerException e)
    {
      // OK
    }

    ZNRecord stateModelRecord = new ZNRecord("id1");
    tool.addStateModelDef(clusterName, "id1", stateModelRecord);
    path = PropertyPathConfig.getPath(PropertyType.STATEMODELDEFS, clusterName, "id1");
    AssertJUnit.assertTrue(_zkClient.exists(path));

    try
    {
      tool.addStateModelDef(clusterName, "id1", stateModelRecord);
      Assert.fail("should fail if add an already-existing state model");
    }
    catch (ClusterManagerException e)
    {
      // OK
    }

    list = tool.getStateModelDefs(clusterName);
    AssertJUnit.assertEquals(list.size(), 1);

    try
    {
      tool.addResourceGroup(clusterName, "resourceGroup", 10, "nonexistStateModelDef");
      Assert.fail("should fail if add a resource group without an existing state model");
    }
    catch (ClusterManagerException e)
    {
      // OK
    }

    tool.addResourceGroup(clusterName, "resourceGroup", 10, "id1");
    list = tool.getResourceGroupsInCluster(clusterName);
    AssertJUnit.assertEquals(list.size(), 1);

    tool.addResourceGroup(clusterName, "resourceGroup", 10, "id1");
    list = tool.getResourceGroupsInCluster(clusterName);
    AssertJUnit.assertEquals(list.size(), 1);

    ZNRecord resourceGroupExternalViewRecord =
        tool.getResourceGroupExternalView(clusterName, "resourceGroup");
    AssertJUnit.assertNull(resourceGroupExternalViewRecord);
  }

}
