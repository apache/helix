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
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestInstanceConfig {
  @Test
  public void testNotCheckingHostPortExistence() {
    InstanceConfig config = new InstanceConfig("node_0");
    Assert.assertTrue(config.isValid(),
        "HELIX-65: should not check host/port existence for instance-config");
  }

  @Test
  public void testGetParsedDomain() {
    InstanceConfig instanceConfig = new InstanceConfig(new ZNRecord("id"));
    instanceConfig
        .setDomain("cluster=myCluster,zone=myZone1,rack=myRack,host=hostname,instance=instance001");

    Map<String, String> parsedDomain = instanceConfig.getDomainAsMap();
    Assert.assertEquals(parsedDomain.size(), 5);
    Assert.assertEquals(parsedDomain.get("zone"), "myZone1");
  }

  @Test
  public void testSetInstanceEnableWithReason() {
    InstanceConfig instanceConfig = new InstanceConfig(new ZNRecord("id"));
    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
    instanceConfig.setInstanceDisabledReason("NoShowReason");
    instanceConfig.setInstanceDisabledType(InstanceConstants.InstanceDisabledType.USER_OPERATION);

    Assert.assertEquals(instanceConfig.getRecord().getSimpleFields()
        .get(InstanceConfig.InstanceConfigProperty.HELIX_ENABLED.toString()), "true");
    Assert.assertEquals(instanceConfig.getRecord().getSimpleFields()
        .get(InstanceConfig.InstanceConfigProperty.HELIX_DISABLED_REASON.toString()), null);
    Assert.assertEquals(instanceConfig.getRecord().getSimpleFields()
        .get(InstanceConfig.InstanceConfigProperty.HELIX_DISABLED_TYPE.toString()), null);


    instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.DISABLE);
    String reasonCode = "ReasonCode";
    instanceConfig.setInstanceDisabledReason(reasonCode);
    instanceConfig.setInstanceDisabledType(InstanceConstants.InstanceDisabledType.USER_OPERATION);
    Assert.assertEquals(instanceConfig.getRecord().getSimpleFields()
        .get(InstanceConfig.InstanceConfigProperty.HELIX_ENABLED.toString()), "false");
    Assert.assertEquals(instanceConfig.getRecord().getSimpleFields()
        .get(InstanceConfig.InstanceConfigProperty.HELIX_DISABLED_REASON.toString()), reasonCode);
    Assert.assertEquals(instanceConfig.getInstanceDisabledReason(), reasonCode);
    Assert.assertEquals(instanceConfig.getInstanceDisabledType(),
        InstanceConstants.InstanceDisabledType.USER_OPERATION.toString());
  }

  @Test
  public void testGetParsedDomainEmptyDomain() {
    InstanceConfig instanceConfig = new InstanceConfig(new ZNRecord("id"));

    Map<String, String> parsedDomain = instanceConfig.getDomainAsMap();
    Assert.assertTrue(parsedDomain.isEmpty());
  }

  @Test
  public void testGetInstanceCapacityMap() {
    Map<String, Integer> capacityDataMap = ImmutableMap.of("item1", 1,
        "item2", 2,
        "item3", 3);

    Map<String, String> capacityDataMapString = ImmutableMap.of("item1", "1",
        "item2", "2",
        "item3", "3");

    ZNRecord rec = new ZNRecord("testId");
    rec.setMapField(InstanceConfig.InstanceConfigProperty.INSTANCE_CAPACITY_MAP.name(), capacityDataMapString);
    InstanceConfig testConfig = new InstanceConfig(rec);

    Assert.assertTrue(testConfig.getInstanceCapacityMap().equals(capacityDataMap));
  }

  @Test
  public void testGetInstanceCapacityMapEmpty() {
    InstanceConfig testConfig = new InstanceConfig("testId");

    Assert.assertTrue(testConfig.getInstanceCapacityMap().equals(Collections.emptyMap()));
  }

  @Test
  public void testSetInstanceCapacityMap() {
    Map<String, Integer> capacityDataMap = ImmutableMap.of("item1", 1,
        "item2", 2,
        "item3", 3);

    Map<String, String> capacityDataMapString =
        ImmutableMap.of("item1", "1", "item2", "2", "item3", "3");

    InstanceConfig testConfig = new InstanceConfig("testConfig");
    testConfig.setInstanceCapacityMap(capacityDataMap);

    Assert.assertEquals(testConfig.getRecord().getMapField(InstanceConfig.InstanceConfigProperty.
        INSTANCE_CAPACITY_MAP.name()), capacityDataMapString);

    // This operation shall be done. This will clear the instance capacity map in the InstanceConfig
    testConfig.setInstanceCapacityMap(Collections.emptyMap());

    Assert.assertEquals(testConfig.getRecord().getMapField(InstanceConfig.InstanceConfigProperty.
        INSTANCE_CAPACITY_MAP.name()), Collections.emptyMap());

    // This operation shall be done. This will remove the instance capacity map in the InstanceConfig
    testConfig.setInstanceCapacityMap(null);

    Assert.assertTrue(testConfig.getRecord().getMapField(InstanceConfig.InstanceConfigProperty.
        INSTANCE_CAPACITY_MAP.name()) == null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = "Capacity Data contains a negative value: item3 = -3")
  public void testSetInstanceCapacityMapInvalid() {
    Map<String, Integer> capacityDataMap = ImmutableMap.of("item1", 1,
        "item2", 2,
        "item3", -3);

    InstanceConfig testConfig = new InstanceConfig("testConfig");
    testConfig.setInstanceCapacityMap(capacityDataMap);
  }

  @Test
  public void testGetTargetTaskThreadPoolSize() {
    InstanceConfig testConfig = new InstanceConfig("testConfig");
    testConfig.getRecord().setIntField(
        InstanceConfig.InstanceConfigProperty.TARGET_TASK_THREAD_POOL_SIZE.name(), 100);

    Assert.assertEquals(testConfig.getTargetTaskThreadPoolSize(), 100);
  }

  @Test
  public void testSetTargetTaskThreadPoolSize() {
    InstanceConfig testConfig = new InstanceConfig("testConfig");
    testConfig.setTargetTaskThreadPoolSize(100);

    Assert.assertEquals(testConfig.getRecord()
            .getIntField(InstanceConfig.InstanceConfigProperty.TARGET_TASK_THREAD_POOL_SIZE.name(), -1),
        100);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSetTargetTaskThreadPoolSizeIllegalArgument() {
    InstanceConfig testConfig = new InstanceConfig("testConfig");
    testConfig.setTargetTaskThreadPoolSize(-1);
  }

  @Test
  public void testInstanceConfigBuilder() {

    Map<String, String> instanceInfoMap = new HashMap<>();
    instanceInfoMap.put("CAGE", "H");
    Map<String, Integer> capacityDataMap = ImmutableMap.of("weight1", 1);
    InstanceConfig instanceConfig =
        new InstanceConfig.Builder().setHostName("testHost").setPort("1234").setDomain("foo=bar")
            .setWeight(100).setInstanceEnabled(true).addTag("tag1").addTag("tag2")
            .setInstanceEnabled(false).setInstanceInfoMap(instanceInfoMap)
            .addInstanceInfo("CAGE", "G").addInstanceInfo("CABINET", "30")
            .setInstanceCapacityMap(capacityDataMap).build("instance1");

    Assert.assertEquals(instanceConfig.getId(), "instance1");
    Assert.assertEquals(instanceConfig.getHostName(), "testHost");
    Assert.assertEquals(instanceConfig.getPort(), "1234");
    Assert.assertEquals(instanceConfig.getDomainAsString(), "foo=bar");
    Assert.assertEquals(instanceConfig.getWeight(), 100);
    Assert.assertTrue(instanceConfig.getTags().contains("tag1"));
    Assert.assertTrue(instanceConfig.getTags().contains("tag2"));
    Assert.assertFalse(instanceConfig.getInstanceEnabled());
    Assert.assertEquals(instanceConfig.getInstanceInfoMap().get("CAGE"), "H");
    Assert.assertEquals(instanceConfig.getInstanceInfoMap().get("CABINET"), "30");
    Assert.assertEquals(instanceConfig.getInstanceCapacityMap().get("weight1"), Integer.valueOf(1));
  }

  @Test
  public void testOverwriteInstanceConfig() {
    InstanceConfig instanceConfig = new InstanceConfig("instance2");
    instanceConfig.setHostName("host1");
    instanceConfig.setPort("1234");
    instanceConfig.setDomain("foo=bar");
    instanceConfig.setWeight(100);
    instanceConfig.setInstanceEnabled(false);
    instanceConfig.addTag("tag1");
    instanceConfig.addTag("tag2");
    instanceConfig.setInstanceCapacityMap(ImmutableMap.of("weight1", 1));

    InstanceConfig overrideConfig = new InstanceConfig("instance1");
    overrideConfig.setHostName("host2");
    overrideConfig.setPort("5678");
    overrideConfig.setDomain("foo=bar2");
    overrideConfig.setWeight(200);
    overrideConfig.addTag("tag3");
    overrideConfig.addTag("tag4");
    overrideConfig.setInstanceOperation(InstanceConstants.InstanceOperation.EVACUATE);
    overrideConfig.setInstanceCapacityMap(ImmutableMap.of("weight2", 2));

    instanceConfig.overwriteInstanceConfig(overrideConfig);

    Assert.assertEquals(instanceConfig.getId(), "instance2");
    Assert.assertEquals(instanceConfig.getHostName(), "host1");
    Assert.assertEquals(instanceConfig.getPort(), "1234");
    Assert.assertEquals(instanceConfig.getDomainAsString(), "foo=bar");
    Assert.assertEquals(instanceConfig.getWeight(), 200);
    Assert.assertFalse(instanceConfig.getTags().contains("tag1"));
    Assert.assertFalse(instanceConfig.getTags().contains("tag2"));
    Assert.assertTrue(instanceConfig.getTags().contains("tag3"));
    Assert.assertTrue(instanceConfig.getTags().contains("tag4"));
    Assert.assertFalse(instanceConfig.getRecord().getSimpleFields()
        .containsKey(InstanceConfig.InstanceConfigProperty.HELIX_ENABLED.toString()));
    Assert.assertEquals(instanceConfig.getInstanceOperation(),
        InstanceConstants.InstanceOperation.EVACUATE);
    Assert.assertFalse(instanceConfig.getInstanceCapacityMap().containsKey("weight1"));
    Assert.assertEquals(instanceConfig.getInstanceCapacityMap().get("weight2"), Integer.valueOf(2));
  }
}
