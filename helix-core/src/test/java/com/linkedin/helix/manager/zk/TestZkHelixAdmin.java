package com.linkedin.helix.manager.zk;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.helix.ConfigScope;
import com.linkedin.helix.ConfigScopeBuilder;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.model.StateModelDefinition;

public class TestZkHelixAdmin extends ZkUnitTestBase
{
  @Test()
  public void testZkHelixAdmin()
  {
    System.out.println("START testZkHelixAdmin at " + new Date(System.currentTimeMillis()));

    final String clusterName = getShortClassName();
    if (_gZkClient.exists("/" + clusterName))
    {
      _gZkClient.deleteRecursive("/" + clusterName);
    }

    ZKHelixAdmin tool = new ZKHelixAdmin(_gZkClient);
    tool.addCluster(clusterName, true);
    Assert.assertTrue(ZKUtil.isClusterSetup(clusterName, _gZkClient));
    tool.addCluster(clusterName, true);
    Assert.assertTrue(ZKUtil.isClusterSetup(clusterName, _gZkClient));

    List<String> list = tool.getClusters();
    AssertJUnit.assertTrue(list.size() > 0);

    try
    {
      tool.addCluster(clusterName, false);
      Assert.fail("should fail if add an already existing cluster");
    } catch (HelixException e)
    {
      // OK
    }

    InstanceConfig config = new InstanceConfig("host1_9999");
    config.setHostName("host1");
    config.setPort("9999");
    tool.addInstance(clusterName, config);
    tool.enableInstance(clusterName, "host1_9999", true);
    String path = PropertyPathConfig.getPath(PropertyType.INSTANCES,
        clusterName, "host1_9999");
    AssertJUnit.assertTrue(_gZkClient.exists(path));

    try
    {
      tool.addInstance(clusterName, config);
      Assert.fail("should fail if add an alredy-existing instance");
    } catch (HelixException e)
    {
      // OK
    }
    config = tool.getInstanceConfig(clusterName, "host1_9999");
    AssertJUnit.assertEquals(config.getId(), "host1_9999");

    tool.dropInstance(clusterName, config);
    try
    {
      tool.getInstanceConfig(clusterName, "host1_9999");
      Assert.fail("should fail if get a non-existent instance");
    } catch (HelixException e)
    {
      // OK
    }
    try
    {
      tool.dropInstance(clusterName, config);
      Assert.fail("should fail if drop on a non-existent instance");
    } catch (HelixException e)
    {
      // OK
    }
    try
    {
      tool.enableInstance(clusterName, "host1_9999", false);
      Assert.fail("should fail if enable a non-existent instance");
    } catch (HelixException e)
    {
      // OK
    }
    ZNRecord stateModelRecord = new ZNRecord("id1");
    try
    {
      tool.addStateModelDef(clusterName, "id1", new StateModelDefinition(
          stateModelRecord));
      path = PropertyPathConfig.getPath(PropertyType.STATEMODELDEFS,
          clusterName, "id1");
      AssertJUnit.assertTrue(_gZkClient.exists(path));
      Assert.fail("should fail");
    } catch (HelixException e)
    {
      // OK
    }
    try
    {
      tool.addStateModelDef(clusterName, "id1", new StateModelDefinition(
          stateModelRecord));
      Assert.fail("should fail if add an already-existing state model");
    } catch (HelixException e)
    {
      // OK
    }
    list = tool.getStateModelDefs(clusterName);
    AssertJUnit.assertEquals(list.size(), 0);

    try
    {
      tool.addResource(clusterName, "resource", 10,
          "nonexistStateModelDef");
      Assert
          .fail("should fail if add a resource without an existing state model");
    } catch (HelixException e)
    {
      // OK
    }
    try
    {
      tool.addResource(clusterName, "resource", 10, "id1");
      Assert.fail("should fail");
    } catch (HelixException e)
    {
      // OK
    }
    list = tool.getResourcesInCluster(clusterName);
    AssertJUnit.assertEquals(list.size(), 0);
    try
    {
      tool.addResource(clusterName, "resource", 10, "id1");
      Assert.fail("should fail");
    } catch (HelixException e)
    {
      // OK
    }
    list = tool.getResourcesInCluster(clusterName);
    AssertJUnit.assertEquals(list.size(), 0);

    ExternalView resourceExternalView = tool.getResourceExternalView(
        clusterName, "resource");
    AssertJUnit.assertNull(resourceExternalView);

    // test config support
    ConfigScope scope = new ConfigScopeBuilder().forCluster(clusterName)
        .forResource("testResource").forPartition("testPartition").build();
    Map<String, String> properties = new HashMap<String, String>();
    properties.put("pKey1", "pValue1");
    properties.put("pKey2", "pValue2");
    
    // make sure calling set/getConfig() many times will not drain zkClient resources
    int nbOfZkClients = ZkClient.getNumberOfConnections();
    for (int i = 0; i < 100; i++)
    {
      tool.setConfig(scope, properties);
      Map<String, String> newProperties = tool.getConfig(scope, properties.keySet());
      Assert.assertEquals(newProperties.size(), 2);
      Assert.assertEquals(newProperties.get("pKey1"), "pValue1");
      Assert.assertEquals(newProperties.get("pKey2"), "pValue2");
    }
    Assert.assertTrue(ZkClient.getNumberOfConnections() - nbOfZkClients < 5);

    System.out.println("END testZkHelixAdmin at " + new Date(System.currentTimeMillis()));
  }

}
