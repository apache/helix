package com.linkedin.helix.agent.zk;

import java.util.Date;
import java.util.List;

import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.ClusterManagerException;
import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.agent.zk.ZKClusterManagementTool;
import com.linkedin.helix.agent.zk.ZKUtil;
import com.linkedin.helix.agent.zk.ZNRecordSerializer;
import com.linkedin.helix.agent.zk.ZkClient;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.model.StateModelDefinition;

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
    boolean exceptionThrown = false;

    try
    {
      tool.addCluster(clusterName, false);
      Assert.fail("should fail if add an already existing cluster");
    } catch (ClusterManagerException e)
    {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
    exceptionThrown = false;

    InstanceConfig config = new InstanceConfig("host1_9999");
    config.setHostName("host1");
    config.setPort("9999");
    tool.addInstance(clusterName, config);
    tool.enableInstance(clusterName, "host1_9999", true);
    String path = PropertyPathConfig.getPath(PropertyType.INSTANCES,
        clusterName, "host1_9999");
    AssertJUnit.assertTrue(_zkClient.exists(path));

    try
    {
      tool.addInstance(clusterName, config);
      Assert.fail("should fail if add an alredy-existing instance");
    } catch (ClusterManagerException e)
    {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
    exceptionThrown = false;
    config = tool.getInstanceConfig(clusterName, "host1_9999");
    AssertJUnit.assertEquals(config.getId(), "host1_9999");

    tool.dropInstance(clusterName, config);
    try
    {
      tool.getInstanceConfig(clusterName, "host1_9999");
      Assert.fail("should fail if get a non-existent instance");
    } catch (ClusterManagerException e)
    {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
    exceptionThrown = false;
    try
    {
      tool.dropInstance(clusterName, config);
      Assert.fail("should fail if drop on a non-existent instance");
    } catch (ClusterManagerException e)
    {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
    exceptionThrown = false;
    try
    {
      tool.enableInstance(clusterName, "host1_9999", false);
      Assert.fail("should fail if enable a non-existent instance");
    } catch (ClusterManagerException e)
    {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
    exceptionThrown = false;
    ZNRecord stateModelRecord = new ZNRecord("id1");
    try
    {
      tool.addStateModelDef(clusterName, "id1", new StateModelDefinition(
          stateModelRecord));
      path = PropertyPathConfig.getPath(PropertyType.STATEMODELDEFS,
          clusterName, "id1");
      AssertJUnit.assertTrue(_zkClient.exists(path));
    } catch (ClusterManagerException e)
    {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
    exceptionThrown = false;
    try
    {
      tool.addStateModelDef(clusterName, "id1", new StateModelDefinition(
          stateModelRecord));
      Assert.fail("should fail if add an already-existing state model");
    } catch (ClusterManagerException e)
    {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
    exceptionThrown = false;
    list = tool.getStateModelDefs(clusterName);
    AssertJUnit.assertEquals(list.size(), 0);

    try
    {
      tool.addResourceGroup(clusterName, "resourceGroup", 10,
          "nonexistStateModelDef");
      Assert
          .fail("should fail if add a resource group without an existing state model");
    } catch (ClusterManagerException e)
    {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
    exceptionThrown = false;
    try
    {
      tool.addResourceGroup(clusterName, "resourceGroup", 10, "id1");
    } catch (ClusterManagerException e)
    {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
    exceptionThrown = false;
    list = tool.getResourceGroupsInCluster(clusterName);
    AssertJUnit.assertEquals(list.size(), 0);
    try
    {
      tool.addResourceGroup(clusterName, "resourceGroup", 10, "id1");
    } catch (ClusterManagerException e)
    {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
    exceptionThrown = false;
    list = tool.getResourceGroupsInCluster(clusterName);
    AssertJUnit.assertEquals(list.size(), 0);

    ExternalView resourceGroupExternalView = tool.getResourceGroupExternalView(
        clusterName, "resourceGroup");
    AssertJUnit.assertNull(resourceGroupExternalView);
  }

}
