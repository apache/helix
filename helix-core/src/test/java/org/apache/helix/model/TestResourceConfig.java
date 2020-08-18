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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestResourceConfig {
  private static final ObjectMapper _objectMapper = new ObjectMapper();

  @Test
  public void testGetPartitionCapacityMap() throws IOException {
    Map<String, Integer> capacityDataMap = ImmutableMap.of("item1", 1,
        "item2", 2,
        "item3", 3);

    ZNRecord rec = new ZNRecord("testId");
    rec.setMapField(ResourceConfig.ResourceConfigProperty.PARTITION_CAPACITY_MAP.name(), Collections
        .singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY,
            _objectMapper.writeValueAsString(capacityDataMap)));
    ResourceConfig testConfig = new ResourceConfig(rec);

    Assert.assertTrue(testConfig.getPartitionCapacityMap().get(ResourceConfig.DEFAULT_PARTITION_KEY)
        .equals(capacityDataMap));
  }

  @Test
  public void testGetPartitionCapacityMapEmpty() throws IOException {
    ResourceConfig testConfig = new ResourceConfig("testId");

    Assert.assertTrue(testConfig.getPartitionCapacityMap().equals(Collections.emptyMap()));
  }

  @Test(expectedExceptions = IOException.class)
  public void testGetPartitionCapacityMapInvalidJson() throws IOException {
    ZNRecord rec = new ZNRecord("testId");
    rec.setMapField(ResourceConfig.ResourceConfigProperty.PARTITION_CAPACITY_MAP.name(),
        Collections.singletonMap("test", "gibberish"));
    ResourceConfig testConfig = new ResourceConfig(rec);

    testConfig.getPartitionCapacityMap();
  }

  @Test(dependsOnMethods = "testGetPartitionCapacityMap", expectedExceptions = IOException.class)
  public void testGetPartitionCapacityMapInvalidJsonType() throws IOException {
    Map<String, String> capacityDataMap = ImmutableMap.of("item1", "1",
        "item2", "2",
        "item3", "three");

    ZNRecord rec = new ZNRecord("testId");
    rec.setMapField(ResourceConfig.ResourceConfigProperty.PARTITION_CAPACITY_MAP.name(), Collections
        .singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY,
            _objectMapper.writeValueAsString(capacityDataMap)));
    ResourceConfig testConfig = new ResourceConfig(rec);

    testConfig.getPartitionCapacityMap();
  }

  @Test
  public void testSetPartitionCapacityMap() throws IOException {
    Map<String, Integer> capacityDataMap = ImmutableMap.of("item1", 1,
        "item2", 2,
        "item3", 3);

    ResourceConfig testConfig = new ResourceConfig("testConfig");
    testConfig.setPartitionCapacityMap(
        Collections.singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY, capacityDataMap));

    Assert.assertEquals(testConfig.getRecord().getMapField(ResourceConfig.ResourceConfigProperty.
            PARTITION_CAPACITY_MAP.name()).get(ResourceConfig.DEFAULT_PARTITION_KEY),
        _objectMapper.writeValueAsString(capacityDataMap));
  }

  @Test
  public void testSetMultiplePartitionCapacityMap() throws IOException {
    Map<String, Integer> capacityDataMap = ImmutableMap.of("item1", 1,
        "item2", 2,
        "item3", 3);

    Map<String, Map<String, Integer>> totalCapacityMap =
        ImmutableMap.of(ResourceConfig.DEFAULT_PARTITION_KEY, capacityDataMap,
        "partition2", capacityDataMap,
        "partition3", capacityDataMap);

    ResourceConfig testConfig = new ResourceConfig("testConfig");
    testConfig.setPartitionCapacityMap(totalCapacityMap);

    Assert.assertNull(testConfig.getRecord().getMapField(ResourceConfig.ResourceConfigProperty.
        PARTITION_CAPACITY_MAP.name()).get("partition1"));
    Assert.assertEquals(testConfig.getRecord().getMapField(ResourceConfig.ResourceConfigProperty.
        PARTITION_CAPACITY_MAP.name()).get(ResourceConfig.DEFAULT_PARTITION_KEY),
        _objectMapper.writeValueAsString(capacityDataMap));
    Assert.assertEquals(testConfig.getRecord().getMapField(ResourceConfig.ResourceConfigProperty.
            PARTITION_CAPACITY_MAP.name()).get("partition2"),
        _objectMapper.writeValueAsString(capacityDataMap));
    Assert.assertEquals(testConfig.getRecord().getMapField(ResourceConfig.ResourceConfigProperty.
            PARTITION_CAPACITY_MAP.name()).get("partition3"),
        _objectMapper.writeValueAsString(capacityDataMap));
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Capacity Data is empty")
  public void testSetPartitionCapacityMapEmpty() throws IOException {
    Map<String, Integer> capacityDataMap = new HashMap<>();

    ResourceConfig testConfig = new ResourceConfig("testConfig");
    testConfig.setPartitionCapacityMap(
        Collections.singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY, capacityDataMap));
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "The default partition capacity with the default key DEFAULT is required.")
  public void testSetPartitionCapacityMapWithoutDefault() throws IOException {
    Map<String, Integer> capacityDataMap = new HashMap<>();

    ResourceConfig testConfig = new ResourceConfig("testConfig");
    testConfig.setPartitionCapacityMap(
        Collections.singletonMap("Random", capacityDataMap));
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Capacity Data contains a negative value:.+")
  public void testSetPartitionCapacityMapInvalid() throws IOException {
    Map<String, Integer> capacityDataMap = ImmutableMap.of("item1", 1,
        "item2", 2,
        "item3", -3);

    ResourceConfig testConfig = new ResourceConfig("testConfig");
    testConfig.setPartitionCapacityMap(
        Collections.singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY, capacityDataMap));
  }

  @Test
  public void testWithResourceBuilder() throws IOException {
    Map<String, Integer> capacityDataMap = ImmutableMap.of("item1", 1,
        "item2", 2,
        "item3", 3);

    ResourceConfig.Builder builder = new ResourceConfig.Builder("testConfig");
    builder.setPartitionCapacity(capacityDataMap);
    builder.setPartitionCapacity("partition1", capacityDataMap);

    Assert.assertEquals(
        builder.build().getPartitionCapacityMap().get(ResourceConfig.DEFAULT_PARTITION_KEY),
        capacityDataMap);
    Assert.assertEquals(
        builder.build().getPartitionCapacityMap().get("partition1"),
        capacityDataMap);
    Assert.assertNull(
        builder.build().getPartitionCapacityMap().get("Random"));
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "The default partition capacity with the default key DEFAULT is required.")
  public void testWithResourceBuilderInvalidInput() {
    Map<String, Integer> capacityDataMap = ImmutableMap.of("item1", 1,
        "item2", 2,
        "item3", 3);

    ResourceConfig.Builder builder = new ResourceConfig.Builder("testConfig");
    builder.setPartitionCapacity("Random", capacityDataMap);

    builder.build();
  }

  @Test
  public void testMergeWithIdealState() {
    // Test failure case
    ResourceConfig testConfig = new ResourceConfig("testResource");
    IdealState testIdealState = new IdealState("DifferentState");
    try {
      ResourceConfig.mergeIdealStateWithResourceConfig(testConfig, testIdealState);
      Assert.fail("Should not be able merge with a IdealState of different resource.");
    } catch (IllegalArgumentException ex) {
      // expected
    }
    testIdealState = new IdealState("testResource");
    testIdealState.setInstanceGroupTag("testISGroup");
    testIdealState.setMaxPartitionsPerInstance(1);
    testIdealState.setNumPartitions(1);
    testIdealState.setStateModelDefRef("testISDef");
    testIdealState.setStateModelFactoryName("testISFactory");
    testIdealState.setReplicas("3");
    testIdealState.setMinActiveReplicas(1);
    testIdealState.enable(true);
    testIdealState.setResourceGroupName("testISGroup");
    testIdealState.setResourceType("ISType");
    testIdealState.setDisableExternalView(false);
    testIdealState.setDelayRebalanceEnabled(true);
    // Test IdealState info overriding the empty config fields.
    ResourceConfig mergedResourceConfig =
        ResourceConfig.mergeIdealStateWithResourceConfig(null, testIdealState);
    Assert.assertEquals(mergedResourceConfig.getInstanceGroupTag(),
        testIdealState.getInstanceGroupTag());
    Assert.assertEquals(mergedResourceConfig.getMaxPartitionsPerInstance(),
        testIdealState.getMaxPartitionsPerInstance());
    Assert.assertEquals(mergedResourceConfig.getNumPartitions(), testIdealState.getNumPartitions());
    Assert.assertEquals(mergedResourceConfig.getStateModelDefRef(),
        testIdealState.getStateModelDefRef());
    Assert.assertEquals(mergedResourceConfig.getStateModelFactoryName(),
        testIdealState.getStateModelFactoryName());
    Assert.assertEquals(mergedResourceConfig.getNumReplica(), testIdealState.getReplicas());
    Assert.assertEquals(mergedResourceConfig.getMinActiveReplica(),
        testIdealState.getMinActiveReplicas());
    Assert
        .assertEquals(mergedResourceConfig.isEnabled().booleanValue(), testIdealState.isEnabled());
    Assert.assertEquals(mergedResourceConfig.getResourceGroupName(),
        testIdealState.getResourceGroupName());
    Assert.assertEquals(mergedResourceConfig.getResourceType(), testIdealState.getResourceType());
    Assert.assertEquals(mergedResourceConfig.isExternalViewDisabled().booleanValue(),
        testIdealState.isExternalViewDisabled());
    Assert.assertEquals(Boolean.valueOf(mergedResourceConfig
        .getSimpleConfig(ResourceConfig.ResourceConfigProperty.DELAY_REBALANCE_ENABLED.name()))
        .booleanValue(), testIdealState.isDelayRebalanceEnabled());
    // Test priority, Resource Config field has higher priority.
    ResourceConfig.Builder configBuilder = new ResourceConfig.Builder("testResource");
    configBuilder.setInstanceGroupTag("testRCGroup");
    configBuilder.setMaxPartitionsPerInstance(2);
    configBuilder.setNumPartitions(2);
    configBuilder.setStateModelDefRef("testRCDef");
    configBuilder.setStateModelFactoryName("testRCFactory");
    configBuilder.setNumReplica("4");
    configBuilder.setMinActiveReplica(2);
    configBuilder.setHelixEnabled(false);
    configBuilder.setResourceGroupName("testRCGroup");
    configBuilder.setResourceType("RCType");
    configBuilder.setExternalViewDisabled(true);
    testConfig = configBuilder.build();
    mergedResourceConfig =
        ResourceConfig.mergeIdealStateWithResourceConfig(testConfig, testIdealState);
    Assert
        .assertEquals(mergedResourceConfig.getInstanceGroupTag(), testConfig.getInstanceGroupTag());
    Assert.assertEquals(mergedResourceConfig.getMaxPartitionsPerInstance(),
        testConfig.getMaxPartitionsPerInstance());
    Assert.assertEquals(mergedResourceConfig.getNumPartitions(), testConfig.getNumPartitions());
    Assert
        .assertEquals(mergedResourceConfig.getStateModelDefRef(), testConfig.getStateModelDefRef());
    Assert.assertEquals(mergedResourceConfig.getStateModelFactoryName(),
        testConfig.getStateModelFactoryName());
    Assert.assertEquals(mergedResourceConfig.getNumReplica(), testConfig.getNumReplica());
    Assert
        .assertEquals(mergedResourceConfig.getMinActiveReplica(), testConfig.getMinActiveReplica());
    Assert.assertEquals(mergedResourceConfig.isEnabled(), testConfig.isEnabled());
    Assert.assertEquals(mergedResourceConfig.getResourceGroupName(),
        testConfig.getResourceGroupName());
    Assert.assertEquals(mergedResourceConfig.getResourceType(), testConfig.getResourceType());
    Assert.assertEquals(mergedResourceConfig.isExternalViewDisabled(),
        testConfig.isExternalViewDisabled());
  }
}
