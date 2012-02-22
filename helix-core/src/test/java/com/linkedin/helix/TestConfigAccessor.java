package com.linkedin.helix;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestConfigAccessor extends ZkUnitTestBase
{
  final String _className = getShortClassName();
  final String _clusterName = "CLUSTER_" + _className;

  @Test
  public void testZkConfigAccessor() throws Exception
  {
    TestHelper.setupCluster(_clusterName, ZK_ADDR, 12918, "localhost", "TestDB", 1, 10, 5, 3,
        "MasterSlave", true);

    ConfigAccessor appConfig = new ConfigAccessor(ZK_ADDR);
    ConfigScope clusterScope = new ConfigScopeBuilder().forCluster(_clusterName).build();

    // cluster scope config
    String clusterConfigValue = appConfig.get(clusterScope, "clusterConfigKey");
    Assert.assertNull(clusterConfigValue);

    appConfig.set(clusterScope, "clusterConfigKey", "clusterConfigValue");
    clusterConfigValue = appConfig.get(clusterScope, "clusterConfigKey");
    Assert.assertEquals(clusterConfigValue, "clusterConfigValue");

    // resource scope config
    ConfigScope resourceScope = new ConfigScopeBuilder().forCluster(_clusterName)
        .forResource("testResource").build();
    appConfig.set(resourceScope, "resourceConfigKey", "resourceConfigValue");
    String resourceConfigValue = appConfig.get(resourceScope, "resourceConfigKey");
    Assert.assertEquals(resourceConfigValue, "resourceConfigValue");

    // partition scope config
    ConfigScope partitionScope = new ConfigScopeBuilder().forCluster(_clusterName)
        .forResource("testResource").forPartition("testPartition").build();
    appConfig.set(partitionScope, "partitionConfigKey", "partitionConfigValue");
    String partitionConfigValue = appConfig.get(partitionScope, "partitionConfigKey");
    Assert.assertEquals(partitionConfigValue, "partitionConfigValue");

    // participant scope config
    ConfigScope participantScope = new ConfigScopeBuilder().forCluster(_clusterName)
        .forParticipant("localhost_12918").build();
    appConfig.set(participantScope, "participantConfigKey", "participantConfigValue");
    String participantConfigValue = appConfig.get(participantScope, "participantConfigKey");
    Assert.assertEquals(participantConfigValue, "participantConfigValue");

    // resource config under participant scope
    resourceScope = new ConfigScopeBuilder().forCluster(_clusterName).forParticipant("localhost_12918")
        .forResource("testResource").build();
    appConfig.set(resourceScope, "resourceConfigKey2", "resourceConfigValue2");
    resourceConfigValue = appConfig.get(resourceScope, "resourceConfigKey2");
    Assert.assertEquals(resourceConfigValue, "resourceConfigValue2");

    // negative tests
    try
    {
      new ConfigScopeBuilder().forPartition("testPartition").build();
      Assert.fail("Should fail since cluster name is not set");
    } catch (HelixException e)
    {
      // OK
    }

    try
    {
      new ConfigScopeBuilder().forCluster("testCluster").forPartition("testPartition").build();
      Assert.fail("Should fail since resource name is not set");
    } catch (HelixException e)
    {
      // OK
    }

    try
    {
      new ConfigScopeBuilder().forParticipant("testParticipant").build();
      Assert.fail("Should fail since participant name is not set");
    } catch (HelixException e)
    {
      // OK
    }

  }
}
