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

import java.util.Date;
import java.util.List;

import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.ConfigScopeBuilder;
import org.apache.helix.testutil.ZkTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestConfigAccessor extends ZkTestBase {
  @Test
  public void testBasic() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, "localhost", "TestDB", 1, 10, 5, 3,
        "MasterSlave", true);

    ConfigAccessor configAccessor = new ConfigAccessor(_zkclient);
    ConfigScope clusterScope = new ConfigScopeBuilder().forCluster(clusterName).build();

    // cluster scope config
    String clusterConfigValue = configAccessor.get(clusterScope, "clusterConfigKey");
    Assert.assertNull(clusterConfigValue);

    configAccessor.set(clusterScope, "clusterConfigKey", "clusterConfigValue");
    clusterConfigValue = configAccessor.get(clusterScope, "clusterConfigKey");
    Assert.assertEquals(clusterConfigValue, "clusterConfigValue");

    // resource scope config
    ConfigScope resourceScope =
        new ConfigScopeBuilder().forCluster(clusterName).forResource("testResource").build();
    configAccessor.set(resourceScope, "resourceConfigKey", "resourceConfigValue");
    String resourceConfigValue = configAccessor.get(resourceScope, "resourceConfigKey");
    Assert.assertEquals(resourceConfigValue, "resourceConfigValue");

    // partition scope config
    ConfigScope partitionScope =
        new ConfigScopeBuilder().forCluster(clusterName).forResource("testResource")
            .forPartition("testPartition").build();
    configAccessor.set(partitionScope, "partitionConfigKey", "partitionConfigValue");
    String partitionConfigValue = configAccessor.get(partitionScope, "partitionConfigKey");
    Assert.assertEquals(partitionConfigValue, "partitionConfigValue");

    // participant scope config
    ConfigScope participantScope =
        new ConfigScopeBuilder().forCluster(clusterName).forParticipant("localhost_12918").build();
    configAccessor.set(participantScope, "participantConfigKey", "participantConfigValue");
    String participantConfigValue = configAccessor.get(participantScope, "participantConfigKey");
    Assert.assertEquals(participantConfigValue, "participantConfigValue");

    List<String> keys = configAccessor.getKeys(ConfigScopeProperty.RESOURCE, clusterName);
    Assert.assertEquals(keys.size(), 1, "should be [testResource]");
    Assert.assertEquals(keys.get(0), "testResource");

    keys = configAccessor.getKeys(ConfigScopeProperty.CLUSTER, clusterName);
    Assert.assertEquals(keys.size(), 1, "should be [" + clusterName + "]");
    Assert.assertEquals(keys.get(0), clusterName);

    keys = configAccessor.getKeys(ConfigScopeProperty.PARTICIPANT, clusterName);
    Assert.assertEquals(keys.size(), 5, "should be [localhost_12918~22] sorted");
    Assert.assertEquals(keys.get(0), "localhost_12918");
    Assert.assertEquals(keys.get(4), "localhost_12922");

    keys = configAccessor.getKeys(ConfigScopeProperty.PARTITION, clusterName, "testResource");
    Assert.assertEquals(keys.size(), 1, "should be [testPartition]");
    Assert.assertEquals(keys.get(0), "testPartition");

    keys = configAccessor.getKeys(ConfigScopeProperty.RESOURCE, clusterName, "testResource");
    Assert.assertEquals(keys.size(), 1, "should be [resourceConfigKey]");
    Assert.assertEquals(keys.get(0), "resourceConfigKey");

    keys = configAccessor.getKeys(ConfigScopeProperty.CLUSTER, clusterName, clusterName);
    Assert.assertEquals(keys.size(), 1, "should be [clusterConfigKey]");
    Assert.assertEquals(keys.get(0), "clusterConfigKey");

    keys = configAccessor.getKeys(ConfigScopeProperty.PARTICIPANT, clusterName, "localhost_12918");
    System.out.println((keys));
    Assert.assertEquals(keys.size(), 4,
        "should be [HELIX_ENABLED, HELIX_HOST, HELIX_PORT, participantConfigKey]");
    Assert.assertEquals(keys.get(3), "participantConfigKey");

    keys =
        configAccessor.getKeys(ConfigScopeProperty.PARTITION, clusterName, "testResource",
            "testPartition");
    Assert.assertEquals(keys.size(), 1, "should be [partitionConfigKey]");
    Assert.assertEquals(keys.get(0), "partitionConfigKey");

    // test configAccessor.remove()
    configAccessor.remove(clusterScope, "clusterConfigKey");
    clusterConfigValue = configAccessor.get(clusterScope, "clusterConfigKey");
    Assert.assertNull(clusterConfigValue, "Should be null since it's removed");

    configAccessor.remove(resourceScope, "resourceConfigKey");
    resourceConfigValue = configAccessor.get(resourceScope, "resourceConfigKey");
    Assert.assertNull(resourceConfigValue, "Should be null since it's removed");

    configAccessor.remove(partitionScope, "partitionConfigKey");
    partitionConfigValue = configAccessor.get(partitionScope, "partitionConfigKey");
    Assert.assertNull(partitionConfigValue, "Should be null since it's removed");

    configAccessor.remove(participantScope, "participantConfigKey");
    participantConfigValue = configAccessor.get(partitionScope, "participantConfigKey");
    Assert.assertNull(participantConfigValue, "Should be null since it's removed");

    // negative tests
    try {
      new ConfigScopeBuilder().forPartition("testPartition").build();
      Assert.fail("Should fail since cluster name is not set");
    } catch (Exception e) {
      // OK
    }

    try {
      new ConfigScopeBuilder().forCluster("testCluster").forPartition("testPartition").build();
      Assert.fail("Should fail since resource name is not set");
    } catch (Exception e) {
      // OK
    }

    try {
      new ConfigScopeBuilder().forParticipant("testParticipant").build();
      Assert.fail("Should fail since cluster name is not set");
    } catch (Exception e) {
      // OK
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  // HELIX-25: set participant Config should check existence of instance
  @Test
  public void testSetNonexistentParticipantConfig() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    ZKHelixAdmin admin = new ZKHelixAdmin(_zkclient);
    admin.addCluster(clusterName, true);
    ConfigAccessor configAccessor = new ConfigAccessor(_zkclient);
    ConfigScope participantScope =
        new ConfigScopeBuilder().forCluster(clusterName).forParticipant("localhost_12918").build();

    try {
      configAccessor.set(participantScope, "participantConfigKey", "participantConfigValue");
      Assert
          .fail("Except fail to set participant-config because participant: localhost_12918 is not added to cluster yet");
    } catch (HelixException e) {
      // OK
    }
    admin.addInstance(clusterName, new InstanceConfig("localhost_12918"));

    try {
      configAccessor.set(participantScope, "participantConfigKey", "participantConfigValue");
    } catch (Exception e) {
      Assert
          .fail("Except succeed to set participant-config because participant: localhost_12918 has been added to cluster");
    }

    String participantConfigValue = configAccessor.get(participantScope, "participantConfigKey");
    Assert.assertEquals(participantConfigValue, "participantConfigValue");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
