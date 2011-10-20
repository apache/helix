package com.linkedin.clustermanager;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class TestPropertyPathConfig
{
  public void testGetPath()
  {
    String actual;
    actual = PropertyPathConfig.getPath(PropertyType.IDEALSTATES, "test_cluster");
    Assert.assertEquals(actual, "/test_cluster/IDEALSTATES");
    actual = PropertyPathConfig.getPath(PropertyType.IDEALSTATES, "test_cluster","resourceGroup");
    Assert.assertEquals(actual, "/test_cluster/IDEALSTATES/resourceGroup");

    
    actual = PropertyPathConfig.getPath(PropertyType.INSTANCES, "test_cluster","instanceName1");
    Assert.assertEquals(actual, "/test_cluster/INSTANCES/instanceName1");

    actual = PropertyPathConfig.getPath(PropertyType.CURRENTSTATES, "test_cluster","instanceName1");
    Assert.assertEquals(actual, "/test_cluster/INSTANCES/instanceName1/CURRENTSTATES");
    actual = PropertyPathConfig.getPath(PropertyType.CURRENTSTATES, "test_cluster","instanceName1","sessionId");
    Assert.assertEquals(actual, "/test_cluster/INSTANCES/instanceName1/CURRENTSTATES/sessionId");
    
    actual = PropertyPathConfig.getPath(PropertyType.CONTROLLER, "test_cluster");
    Assert.assertEquals(actual, "/test_cluster/CONTROLLER");
    actual = PropertyPathConfig.getPath(PropertyType.MESSAGES_CONTROLLER, "test_cluster");
    Assert.assertEquals(actual, "/test_cluster/CONTROLLER/MESSAGES");

    
  }
}
