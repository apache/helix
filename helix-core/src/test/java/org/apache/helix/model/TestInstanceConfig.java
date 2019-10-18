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

import com.google.common.collect.ImmutableMap;
import org.apache.helix.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: zzhang
 * Date: 3/19/13
 * Time: 5:28 PM
 * To change this template use File | Settings | File Templates.
 */
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
    instanceConfig.setDomain("cluster=myCluster,zone=myZone1,rack=myRack,host=hostname,instance=instance001");

    Map<String, String> parsedDomain = instanceConfig.getDomainAsMap();
    Assert.assertEquals(parsedDomain.size(), 5);
    Assert.assertEquals(parsedDomain.get("zone"), "myZone1");
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

    Map<String, String> capacityDataMapString = ImmutableMap.of("item1", "1",
        "item2", "2",
        "item3", "3");

    InstanceConfig testConfig = new InstanceConfig("testConfig");
    testConfig.setInstanceCapacityMap(capacityDataMap);

    Assert.assertEquals(testConfig.getRecord().getMapField(InstanceConfig.InstanceConfigProperty.
        INSTANCE_CAPACITY_MAP.name()), capacityDataMapString);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Capacity Data is null")
  public void testSetInstanceCapacityMapEmpty() {
    Map<String, Integer> capacityDataMap = new HashMap<>();

    InstanceConfig testConfig = new InstanceConfig("testConfig");
    // This operation shall be done. This will clear the instance capacity map in the InstanceConfig
    testConfig.setInstanceCapacityMap(capacityDataMap);
    // This operation will fall.
    testConfig.setInstanceCapacityMap(null);
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
}
