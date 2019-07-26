package org.apache.helix.model;

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

import com.google.common.collect.ImmutableList;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.helix.model.ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS;
import static org.apache.helix.model.ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT;

public class TestClusterConfig {

  @Test
  public void testGetCapacityKeys() {
    List<String> keys = ImmutableList.of("CPU", "MEMORY", "Random");

    ClusterConfig testConfig = new ClusterConfig("testId");
    testConfig.getRecord()
        .setListField(ClusterConfig.ClusterConfigProperty.INSTANCE_CAPACITY_KEYS.name(), keys);

    Assert.assertEquals(testConfig.getInstanceCapacityKeys(), keys);
  }

  @Test
  public void testGetCapacityKeysEmpty() {
    ClusterConfig testConfig = new ClusterConfig("testId");
    Assert.assertEquals(testConfig.getInstanceCapacityKeys(), Collections.emptyList());
  }

  @Test
  public void testSetCapacityKeys() {
    List<String> keys = ImmutableList.of("CPU", "MEMORY", "Random");

    ClusterConfig testConfig = new ClusterConfig("testId");
    testConfig.setInstanceCapacityKeys(keys);

    Assert.assertEquals(keys, testConfig.getRecord()
        .getListField(ClusterConfig.ClusterConfigProperty.INSTANCE_CAPACITY_KEYS.name()));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSetCapacityKeysEmptyList() {
    ClusterConfig testConfig = new ClusterConfig("testId");
    testConfig.setInstanceCapacityKeys(Collections.emptyList());
  }

  @Test
  public void testGetRebalancePreference() {
    Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preference = new HashMap<>();
    preference.put(EVENNESS, 5);
    preference.put(LESS_MOVEMENT, 3);

    Map<String, String> mapFieldData = new HashMap<>();
    for (ClusterConfig.GlobalRebalancePreferenceKey key : preference.keySet()) {
      mapFieldData.put(key.name(), String.valueOf(preference.get(key)));
    }

    ClusterConfig testConfig = new ClusterConfig("testId");
    testConfig.getRecord()
        .setMapField(ClusterConfig.ClusterConfigProperty.REBALANCE_PREFERENCE.name(), mapFieldData);

    Assert.assertEquals(testConfig.getGlobalRebalancePreference(), preference);
  }

  @Test
  public void testGetRebalancePreferenceDefault() {
    ClusterConfig testConfig = new ClusterConfig("testId");
    Assert.assertEquals(testConfig.getGlobalRebalancePreference(),
        ClusterConfig.DEFAULT_GLOBAL_REBALANCE_PREFERENCE);

    Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preference = new HashMap<>();
    preference.put(EVENNESS, 5);
    testConfig.setGlobalRebalancePreference(preference);

    Assert.assertEquals(testConfig.getGlobalRebalancePreference(),
        ClusterConfig.DEFAULT_GLOBAL_REBALANCE_PREFERENCE);
  }

  @Test
  public void testSetRebalancePreference() {
    Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preference = new HashMap<>();
    preference.put(EVENNESS, 5);
    preference.put(LESS_MOVEMENT, 3);

    Map<String, String> mapFieldData = new HashMap<>();
    for (ClusterConfig.GlobalRebalancePreferenceKey key : preference.keySet()) {
      mapFieldData.put(key.name(), String.valueOf(preference.get(key)));
    }

    ClusterConfig testConfig = new ClusterConfig("testId");
    testConfig.setGlobalRebalancePreference(preference);

    Assert.assertEquals(testConfig.getRecord()
            .getMapField(ClusterConfig.ClusterConfigProperty.REBALANCE_PREFERENCE.name()),
        mapFieldData);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSetRebalancePreferenceInvalidNumber() {
    Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preference = new HashMap<>();
    preference.put(EVENNESS, -1);
    preference.put(LESS_MOVEMENT, 3);

    ClusterConfig testConfig = new ClusterConfig("testId");
    testConfig.setGlobalRebalancePreference(preference);
  }
}
