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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.controller.rebalancer.constraint.MockAbnormalStateResolver;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

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

    testConfig.setInstanceCapacityKeys(Collections.emptyList());

    Assert.assertEquals(testConfig.getRecord()
            .getListField(ClusterConfig.ClusterConfigProperty.INSTANCE_CAPACITY_KEYS.name()),
        Collections.emptyList());

    testConfig.setInstanceCapacityKeys(null);

    Assert.assertTrue(testConfig.getRecord()
        .getListField(ClusterConfig.ClusterConfigProperty.INSTANCE_CAPACITY_KEYS.name()) == null);
  }

  @Test
  public void testGetGlobalTargetTaskThreadPoolSize() {
    ClusterConfig testConfig = new ClusterConfig("testId");
    testConfig.getRecord().setIntField(
        ClusterConfig.ClusterConfigProperty.GLOBAL_TARGET_TASK_THREAD_POOL_SIZE.name(), 100);

    Assert.assertEquals(testConfig.getGlobalTargetTaskThreadPoolSize(), 100);
  }

  @Test
  public void testSetGlobalTargetTaskThreadPoolSize() {
    ClusterConfig testConfig = new ClusterConfig("testId");
    testConfig.setGlobalTargetTaskThreadPoolSize(100);

    Assert.assertEquals(testConfig.getRecord().getIntField(
        ClusterConfig.ClusterConfigProperty.GLOBAL_TARGET_TASK_THREAD_POOL_SIZE.name(), -1), 100);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSetGlobalTargetTaskThreadPoolSizeIllegalArgument() {
    ClusterConfig testConfig = new ClusterConfig("testId");
    testConfig.setGlobalTargetTaskThreadPoolSize(-1);
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
  }

  @Test
  public void testGetRebalancePreferenceMissingKey() {
    ClusterConfig testConfig = new ClusterConfig("testId");
    Map<String, String> preference = new HashMap<>();
    preference.put(EVENNESS.name(), String.valueOf(5));
    testConfig.getRecord()
        .setMapField(ClusterConfig.ClusterConfigProperty.REBALANCE_PREFERENCE.name(), preference);

    Assert.assertEquals(testConfig.getGlobalRebalancePreference(), Collections.emptyMap());
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

    testConfig.setGlobalRebalancePreference(Collections.emptyMap());

    Assert.assertEquals(testConfig.getRecord()
            .getMapField(ClusterConfig.ClusterConfigProperty.REBALANCE_PREFERENCE.name()),
        Collections.emptyMap());

    testConfig.setGlobalRebalancePreference(null);

    Assert.assertTrue(testConfig.getRecord()
        .getMapField(ClusterConfig.ClusterConfigProperty.REBALANCE_PREFERENCE.name()) == null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSetRebalancePreferenceInvalidNumber() {
    Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preference = new HashMap<>();
    preference.put(EVENNESS, -1);
    preference.put(LESS_MOVEMENT, 3);

    ClusterConfig testConfig = new ClusterConfig("testId");
    testConfig.setGlobalRebalancePreference(preference);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSetRebalancePreferenceMissingKey() {
    Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preference = new HashMap<>();
    preference.put(EVENNESS, 1);

    ClusterConfig testConfig = new ClusterConfig("testId");
    testConfig.setGlobalRebalancePreference(preference);
  }

  @Test
  public void testGetInstanceCapacityMap() {
    Map<String, Integer> capacityDataMap = ImmutableMap.of("item1", 1, "item2", 2, "item3", 3);

    Map<String, String> capacityDataMapString =
        ImmutableMap.of("item1", "1", "item2", "2", "item3", "3");

    ZNRecord rec = new ZNRecord("testId");
    rec.setMapField(ClusterConfig.ClusterConfigProperty.DEFAULT_INSTANCE_CAPACITY_MAP.name(),
        capacityDataMapString);
    ClusterConfig testConfig = new ClusterConfig(rec);

    Assert.assertTrue(testConfig.getDefaultInstanceCapacityMap().equals(capacityDataMap));
  }

  @Test
  public void testGetInstanceCapacityMapEmpty() {
    ClusterConfig testConfig = new ClusterConfig("testId");

    Assert.assertTrue(testConfig.getDefaultInstanceCapacityMap().equals(Collections.emptyMap()));
  }

  @Test
  public void testSetInstanceCapacityMap() {
    Map<String, Integer> capacityDataMap = ImmutableMap.of("item1", 1, "item2", 2, "item3", 3);

    Map<String, String> capacityDataMapString =
        ImmutableMap.of("item1", "1", "item2", "2", "item3", "3");

    ClusterConfig testConfig = new ClusterConfig("testConfig");
    testConfig.setDefaultInstanceCapacityMap(capacityDataMap);

    Assert.assertEquals(testConfig.getRecord().getMapField(ClusterConfig.ClusterConfigProperty.
        DEFAULT_INSTANCE_CAPACITY_MAP.name()), capacityDataMapString);

    // The following operation can be done, this will clear the default values
    testConfig.setDefaultInstanceCapacityMap(Collections.emptyMap());

    Assert.assertEquals(testConfig.getRecord().getMapField(ClusterConfig.ClusterConfigProperty.
        DEFAULT_INSTANCE_CAPACITY_MAP.name()), Collections.emptyMap());

    testConfig.setDefaultInstanceCapacityMap(null);

    Assert.assertTrue(testConfig.getRecord().getMapField(ClusterConfig.ClusterConfigProperty.
        DEFAULT_INSTANCE_CAPACITY_MAP.name()) == null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Default capacity data contains a negative value: item3 = -3")
  public void testSetInstanceCapacityMapInvalid() {
    Map<String, Integer> capacityDataMap = ImmutableMap.of("item1", 1, "item2", 2, "item3", -3);

    ClusterConfig testConfig = new ClusterConfig("testConfig");
    testConfig.setDefaultInstanceCapacityMap(capacityDataMap);
  }

  @Test
  public void testGetPartitionWeightMap() {
    Map<String, Integer> weightDataMap = ImmutableMap.of("item1", 1, "item2", 2, "item3", 3);

    Map<String, String> weightDataMapString =
        ImmutableMap.of("item1", "1", "item2", "2", "item3", "3");

    ZNRecord rec = new ZNRecord("testId");
    rec.setMapField(ClusterConfig.ClusterConfigProperty.DEFAULT_PARTITION_WEIGHT_MAP.name(),
        weightDataMapString);
    ClusterConfig testConfig = new ClusterConfig(rec);

    Assert.assertTrue(testConfig.getDefaultPartitionWeightMap().equals(weightDataMap));
  }

  @Test
  public void testGetPartitionWeightMapEmpty() {
    ClusterConfig testConfig = new ClusterConfig("testId");

    Assert.assertTrue(testConfig.getDefaultPartitionWeightMap().equals(Collections.emptyMap()));
  }

  @Test
  public void testSetPartitionWeightMap() {
    Map<String, Integer> weightDataMap = ImmutableMap.of("item1", 1, "item2", 2, "item3", 3);

    Map<String, String> weightDataMapString =
        ImmutableMap.of("item1", "1", "item2", "2", "item3", "3");

    ClusterConfig testConfig = new ClusterConfig("testConfig");
    testConfig.setDefaultPartitionWeightMap(weightDataMap);

    Assert.assertEquals(testConfig.getRecord().getMapField(ClusterConfig.ClusterConfigProperty.
        DEFAULT_PARTITION_WEIGHT_MAP.name()), weightDataMapString);

    // The following operation can be done, this will clear the default values
    testConfig.setDefaultPartitionWeightMap(Collections.emptyMap());

    Assert.assertEquals(testConfig.getRecord().getMapField(ClusterConfig.ClusterConfigProperty.
        DEFAULT_PARTITION_WEIGHT_MAP.name()), Collections.emptyMap());

    testConfig.setDefaultPartitionWeightMap(null);

    Assert.assertTrue(testConfig.getRecord().getMapField(ClusterConfig.ClusterConfigProperty.
        DEFAULT_PARTITION_WEIGHT_MAP.name()) == null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Default capacity data contains a negative value: item3 = -3")
  public void testSetPartitionWeightMapInvalid() {
    Map<String, Integer> weightDataMap = ImmutableMap.of("item1", 1, "item2", 2, "item3", -3);

    ClusterConfig testConfig = new ClusterConfig("testConfig");
    testConfig.setDefaultPartitionWeightMap(weightDataMap);
  }

  @Test
  public void testAsyncGlobalRebalanceOption() {
    ClusterConfig testConfig = new ClusterConfig("testConfig");
    // Default value is true.
    Assert.assertEquals(testConfig.isGlobalRebalanceAsyncModeEnabled(), true);
    // Test get the option
    testConfig.getRecord()
        .setBooleanField(ClusterConfig.ClusterConfigProperty.GLOBAL_REBALANCE_ASYNC_MODE.name(),
            false);
    Assert.assertEquals(testConfig.isGlobalRebalanceAsyncModeEnabled(), false);
    // Test set the option
    testConfig.setGlobalRebalanceAsyncMode(true);
    Assert.assertEquals(testConfig.getRecord()
        .getBooleanField(ClusterConfig.ClusterConfigProperty.GLOBAL_REBALANCE_ASYNC_MODE.name(),
            false), true);
  }

  @Test
  public void testGetOfflineNodeTimeOutForMaintenanceMode() {
    ClusterConfig testConfig = new ClusterConfig("testId");
    Assert.assertEquals(testConfig.getOfflineNodeTimeOutForMaintenanceMode(), -1);

    testConfig.getRecord()
        .setLongField(ClusterConfig.ClusterConfigProperty.OFFLINE_NODE_TIME_OUT_FOR_MAINTENANCE_MODE
                .name(),
            10000L);
    Assert.assertEquals(testConfig.getOfflineNodeTimeOutForMaintenanceMode(), 10000L);
  }

  @Test
  public void testSetOfflineNodeTimeOutForMaintenanceMode() {
    ClusterConfig testConfig = new ClusterConfig("testId");
    testConfig.setOfflineNodeTimeOutForMaintenanceMode(10000L);
    Assert.assertEquals(testConfig.getRecord()
        .getLongField(ClusterConfig.ClusterConfigProperty.OFFLINE_NODE_TIME_OUT_FOR_MAINTENANCE_MODE
                .name(),
            -1), 10000L);
  }


  @Test
  public void testGetOfflineNodeTimeOutForPurge() {
    ClusterConfig testConfig = new ClusterConfig("testId");
    Assert.assertEquals(testConfig.getOfflineDurationForPurge(), -1);

    testConfig.getRecord()
        .setLongField(ClusterConfig.ClusterConfigProperty.OFFLINE_DURATION_FOR_PURGE_MS
                .name(),
            10000L);
    Assert.assertEquals(testConfig.getOfflineDurationForPurge(), 10000L);
  }

  @Test
  public void testSetOfflineNodeTimeOutForPurge() {
    ClusterConfig testConfig = new ClusterConfig("testId");
    testConfig.setOfflineDurationForPurge(10000L);
    Assert.assertEquals(testConfig.getRecord()
        .getLongField(ClusterConfig.ClusterConfigProperty.OFFLINE_DURATION_FOR_PURGE_MS
                .name(),
            -1), 10000L);
  }

  @Test
  public void testAbnormalStatesResolverConfig() {
    ClusterConfig testConfig = new ClusterConfig("testConfig");
    // Default value is empty
    Assert.assertEquals(testConfig.getAbnormalStateResolverMap(), Collections.EMPTY_MAP);
    // Test set
    Map<String, String> resolverMap =
        ImmutableMap.of(MasterSlaveSMD.name, MockAbnormalStateResolver.class.getName());
    testConfig.setAbnormalStateResolverMap(resolverMap);
    Assert.assertEquals(testConfig.getAbnormalStateResolverMap(), resolverMap);
    // Test empty the map
    testConfig.setAbnormalStateResolverMap(Collections.emptyMap());
    Assert.assertEquals(testConfig.getAbnormalStateResolverMap(), Collections.EMPTY_MAP);

    testConfig.setAbnormalStateResolverMap(null);
    Assert.assertTrue(testConfig.getRecord()
        .getMapField(ClusterConfig.ClusterConfigProperty.ABNORMAL_STATES_RESOLVER_MAP.name())
        == null);
  }

  @Test
  public void testSetInvalidAbnormalStatesResolverConfig() {
    ClusterConfig testConfig = new ClusterConfig("testConfig");

    Map<String, String> resolverMap = new HashMap<>();
    resolverMap.put(null, MockAbnormalStateResolver.class.getName());
    trySetInvalidAbnormalStatesResolverMap(testConfig, resolverMap);

    resolverMap.clear();
    resolverMap.put("", MockAbnormalStateResolver.class.getName());
    trySetInvalidAbnormalStatesResolverMap(testConfig, resolverMap);

    resolverMap.clear();
    resolverMap.put(MasterSlaveSMD.name, null);
    trySetInvalidAbnormalStatesResolverMap(testConfig, resolverMap);

    resolverMap.clear();
    resolverMap.put(MasterSlaveSMD.name, "");
    trySetInvalidAbnormalStatesResolverMap(testConfig, resolverMap);
  }

  @Test
  public void testGetLastOnDemandRebalanceTimestamp() {
    ClusterConfig testConfig = new ClusterConfig("testConfig");
    Assert.assertEquals(testConfig.getLastOnDemandRebalanceTimestamp(), -1L);

    testConfig.getRecord()
        .setLongField(ClusterConfig.ClusterConfigProperty.LAST_ON_DEMAND_REBALANCE_TIMESTAMP.name(),
            10000L);
    Assert.assertEquals(testConfig.getLastOnDemandRebalanceTimestamp(), 10000L);
  }

  @Test
  public void testSetLastOnDemandRebalanceTimestamp() {
    ClusterConfig testConfig = new ClusterConfig("testConfig");
    testConfig.setLastOnDemandRebalanceTimestamp(10000L);

    Assert.assertEquals(testConfig.getRecord()
        .getLongField(ClusterConfig.ClusterConfigProperty.LAST_ON_DEMAND_REBALANCE_TIMESTAMP.name(),
            -1), 10000L);
  }


  private void trySetInvalidAbnormalStatesResolverMap(ClusterConfig testConfig,
      Map<String, String> resolverMap) {
    try {
      testConfig.setAbnormalStateResolverMap(resolverMap);
      Assert.fail("Invalid resolver setup shall fail.");
    } catch (IllegalArgumentException ex) {
      // expected
    }
  }
}
