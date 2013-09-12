package org.apache.helix.api;

import java.util.List;
import java.util.Map;

import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.InstanceConfig.InstanceConfigProperty;
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
public class TestUserConfig {
  /**
   * Ensure that user configs are separated from helix configs in properties that hold both
   */
  @Test
  public void testUserConfigUpdates() {
    final String testKey = "testKey";
    final String prefixedKey = UserConfig.class.getSimpleName() + "_testKey";
    final String testSimpleValue = "testValue";
    final List<String> testListValue = ImmutableList.of("testValue");
    final Map<String, String> testMapValue = ImmutableMap.of("testInnerKey", "testValue");

    // first, add Helix configuration to an InstanceConfig
    ParticipantId participantId = Id.participant("testParticipant");
    InstanceConfig instanceConfig = new InstanceConfig(participantId);
    instanceConfig.setHostName("localhost");

    // now, add user configuration
    UserConfig userConfig = new UserConfig(participantId);
    userConfig.setSimpleField(testKey, testSimpleValue);
    userConfig.setListField(testKey, testListValue);
    userConfig.setMapField(testKey, testMapValue);

    // add the user configuration to the Helix configuration
    instanceConfig.addUserConfig(userConfig);

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
}
