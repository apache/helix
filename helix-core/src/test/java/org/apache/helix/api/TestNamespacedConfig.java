package org.apache.helix.api;

import java.util.List;
import java.util.Map;

import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.ClusterConfiguration;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.InstanceConfig.InstanceConfigProperty;
import org.apache.helix.model.ResourceConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

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

/**
 * A user config is a namespaced subset in the physical model and a separate entity in the logical
 * model. These tests ensure that that separation is honored.
 */
public class TestNamespacedConfig {
  /**
   * Ensure that user configs are separated from helix configs in properties that hold both
   */
  @Test
  public void testUserConfigUpdates() {
    final String testKey = "testKey";
    final String prefixedKey = UserConfig.class.getSimpleName() + "!testKey";
    final String testSimpleValue = "testValue";
    final List<String> testListValue = ImmutableList.of("testValue");
    final Map<String, String> testMapValue = ImmutableMap.of("testInnerKey", "testValue");

    // first, add Helix configuration to an InstanceConfig
    ParticipantId participantId = ParticipantId.from("testParticipant");
    InstanceConfig instanceConfig = new InstanceConfig(participantId);
    instanceConfig.setHostName("localhost");

    // now, add user configuration
    UserConfig userConfig = new UserConfig(Scope.participant(participantId));
    userConfig.setSimpleField(testKey, testSimpleValue);
    userConfig.setListField(testKey, testListValue);
    userConfig.setMapField(testKey, testMapValue);

    // add the user configuration to the Helix configuration
    instanceConfig.addNamespacedConfig(userConfig);

    // get the user configuration back from the property
    UserConfig retrievedConfig = UserConfig.from(instanceConfig);

    // check that the property still has the host name
    Assert.assertTrue(instanceConfig.getHostName().equals("localhost"));

    // check that the retrieved config does not contain the host name
    Assert.assertEquals(retrievedConfig.getStringField(
        InstanceConfigProperty.HELIX_HOST.toString(), "not localhost"), "not localhost");

    // check that both the retrieved config and the original config have the added properties
    Assert.assertEquals(userConfig.getSimpleField(testKey), testSimpleValue);
    Assert.assertEquals(userConfig.getListField(testKey), testListValue);
    Assert.assertEquals(userConfig.getMapField(testKey), testMapValue);
    Assert.assertEquals(retrievedConfig.getSimpleField(testKey), testSimpleValue);
    Assert.assertEquals(retrievedConfig.getListField(testKey), testListValue);
    Assert.assertEquals(retrievedConfig.getMapField(testKey), testMapValue);

    // test that the property has the user config, but prefixed
    Assert.assertEquals(instanceConfig.getRecord().getSimpleField(prefixedKey), testSimpleValue);
    Assert.assertEquals(instanceConfig.getRecord().getListField(prefixedKey), testListValue);
    Assert.assertEquals(instanceConfig.getRecord().getMapField(prefixedKey), testMapValue);
  }

  @Test
  public void testResourceUserConfigCompatibility() {
    final String KEY1 = "key1";
    final String VALUE1 = "value1";
    final String KEY2 = "key2";
    final String VALUE2 = "value2";
    final String KEY3 = "key3";
    final String VALUE3 = "value3";

    // add key1 through user config, key2 through resource config, key3 through ideal state,
    // resource type through resource config, rebalance mode through ideal state
    ResourceId resourceId = ResourceId.from("resourceId");
    UserConfig userConfig = new UserConfig(Scope.resource(resourceId));
    userConfig.setSimpleField(KEY1, VALUE1);
    ResourceConfiguration resourceConfig = new ResourceConfiguration(resourceId);
    resourceConfig.addNamespacedConfig(userConfig);
    resourceConfig.getRecord().setSimpleField(KEY2, VALUE2);
    IdealState idealState = new IdealState(resourceId);
    idealState.setRebalanceMode(RebalanceMode.USER_DEFINED);
    idealState.getRecord().setSimpleField(KEY3, VALUE3);

    // should have key1, key2, and key3, not rebalance mode
    UserConfig result = resourceConfig.getUserConfig();
    idealState.updateUserConfig(result);
    Assert.assertEquals(result.getSimpleField(KEY1), VALUE1);
    Assert.assertEquals(result.getSimpleField(KEY2), VALUE2);
    Assert.assertEquals(result.getSimpleField(KEY3), VALUE3);
    Assert
        .assertNull(result.getSimpleField(IdealState.IdealStateProperty.REBALANCE_MODE.toString()));
  }

  @Test
  public void testParticipantUserConfigCompatibility() {
    final String KEY1 = "key1";
    final String VALUE1 = "value1";
    final String KEY2 = "key2";
    final String VALUE2 = "value2";

    // add key1 through user config, key2 through instance config, hostname through user config
    ParticipantId participantId = ParticipantId.from("participantId");
    UserConfig userConfig = new UserConfig(Scope.participant(participantId));
    userConfig.setSimpleField(KEY1, VALUE1);
    InstanceConfig instanceConfig = new InstanceConfig(participantId);
    instanceConfig.setHostName("localhost");
    instanceConfig.addNamespacedConfig(userConfig);
    instanceConfig.getRecord().setSimpleField(KEY2, VALUE2);

    // should have key1 and key2, not hostname
    UserConfig result = instanceConfig.getUserConfig();
    Assert.assertEquals(result.getSimpleField(KEY1), VALUE1);
    Assert.assertEquals(result.getSimpleField(KEY2), VALUE2);
    Assert.assertNull(result.getSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_HOST
        .toString()));
  }

  @Test
  public void testClusterUserConfigCompatibility() {
    final String KEY1 = "key1";
    final String VALUE1 = "value1";
    final String KEY2 = "key2";
    final String VALUE2 = "value2";

    // add the following: key1 straight to user config, key2 to cluster configuration,
    // allow auto join to cluster configuration
    ClusterId clusterId = ClusterId.from("clusterId");
    UserConfig userConfig = new UserConfig(Scope.cluster(clusterId));
    userConfig.setSimpleField(KEY1, VALUE1);
    ClusterConfiguration clusterConfiguration = new ClusterConfiguration(clusterId);
    clusterConfiguration.addNamespacedConfig(userConfig);
    clusterConfiguration.getRecord().setSimpleField(KEY2, VALUE2);
    clusterConfiguration.setAutoJoinAllowed(true);

    // there should be key1 and key2, but not auto join
    UserConfig result = clusterConfiguration.getUserConfig();
    Assert.assertEquals(result.getSimpleField(KEY1), VALUE1);
    Assert.assertEquals(result.getSimpleField(KEY2), VALUE2);
    Assert.assertNull(result.getSimpleField(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN));
  }
}
