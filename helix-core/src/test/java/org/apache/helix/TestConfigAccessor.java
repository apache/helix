package org.apache.helix;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.List;

import org.apache.helix.model.ConfigScope;
import org.apache.helix.model.ConfigScope.ConfigScopeProperty;
import org.apache.helix.model.builder.ConfigScopeBuilder;
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

    ConfigAccessor appConfig = new ConfigAccessor(_gZkClient);
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

    List<String> keys = appConfig.getKeys(ConfigScopeProperty.RESOURCE, _clusterName);
    Assert.assertEquals(keys.size(), 1, "should be [testResource]");
    Assert.assertEquals(keys.get(0), "testResource");

    keys = appConfig.getKeys(ConfigScopeProperty.CLUSTER, _clusterName);
    Assert.assertEquals(keys.size(), 1, "should be [" + _clusterName + "]");
    Assert.assertEquals(keys.get(0), _clusterName);

    keys = appConfig.getKeys(ConfigScopeProperty.PARTICIPANT, _clusterName);
    Assert.assertEquals(keys.size(), 5, "should be [localhost_12918~22] sorted");
    Assert.assertEquals(keys.get(0), "localhost_12918");
    Assert.assertEquals(keys.get(4), "localhost_12922");

    keys = appConfig.getKeys(ConfigScopeProperty.PARTITION, _clusterName, "testResource");
    Assert.assertEquals(keys.size(), 1, "should be [testPartition]");
    Assert.assertEquals(keys.get(0), "testPartition");

    keys = appConfig.getKeys(ConfigScopeProperty.RESOURCE, _clusterName, "testResource");
    Assert.assertEquals(keys.size(), 1, "should be [resourceConfigKey]");
    Assert.assertEquals(keys.get(0), "resourceConfigKey");

    keys = appConfig.getKeys(ConfigScopeProperty.CLUSTER, _clusterName, _clusterName);
    Assert.assertEquals(keys.size(), 1, "should be [clusterConfigKey]");
    Assert.assertEquals(keys.get(0), "clusterConfigKey");

    keys = appConfig.getKeys(ConfigScopeProperty.PARTICIPANT, _clusterName, "localhost_12918");
    Assert.assertEquals(keys.size(), 4, "should be [HELIX_ENABLED, HELIX_HOST, HELIX_PORT, participantConfigKey]");
    Assert.assertEquals(keys.get(3), "participantConfigKey");

    keys = appConfig.getKeys(ConfigScopeProperty.PARTITION, _clusterName, "testResource", "testPartition");
    Assert.assertEquals(keys.size(), 1, "should be [partitionConfigKey]");
    Assert.assertEquals(keys.get(0), "partitionConfigKey");

    // test configAccessor.remove()
    appConfig.remove(clusterScope, "clusterConfigKey");
    clusterConfigValue = appConfig.get(clusterScope, "clusterConfigKey");
    Assert.assertNull(clusterConfigValue, "Should be null since it's removed");

    appConfig.remove(resourceScope, "resourceConfigKey");
    resourceConfigValue = appConfig.get(resourceScope, "resourceConfigKey");
    Assert.assertNull(resourceConfigValue, "Should be null since it's removed");

    appConfig.remove(partitionScope, "partitionConfigKey");
    partitionConfigValue = appConfig.get(partitionScope, "partitionConfigKey");
    Assert.assertNull(partitionConfigValue, "Should be null since it's removed");
    
    appConfig.remove(participantScope, "participantConfigKey");
    participantConfigValue = appConfig.get(partitionScope, "participantConfigKey");
    Assert.assertNull(participantConfigValue, "Should be null since it's removed");
    
    // negative tests
    try
    {
      new ConfigScopeBuilder().forPartition("testPartition").build();
      Assert.fail("Should fail since cluster name is not set");
    } catch (Exception e)
    {
      // OK
    }

    try
    {
      new ConfigScopeBuilder().forCluster("testCluster").forPartition("testPartition").build();
      Assert.fail("Should fail since resource name is not set");
    } catch (Exception e)
    {
      // OK
    }

    try
    {
      new ConfigScopeBuilder().forParticipant("testParticipant").build();
      Assert.fail("Should fail since cluster name is not set");
    } catch (Exception e)
    {
      // OK
    }

  }
}
