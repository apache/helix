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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestTrieClusterTopology {
  private TrieClusterTopology _trie;

  final List<String> _instanceNames = new ArrayList<>();
  final Map<String, InstanceConfig> _instanceConfigMap = new HashMap<>();
  private ClusterConfig _clusterConfig;
  final int _numOfNodes = 40;

  @BeforeClass
  public void beforeClass() {
    for (int i = 0; i < _numOfNodes; i++) {
      _instanceNames.add(String.valueOf(i));
    }
    createClusterConfig();
    createInstanceConfigMap();
  }

  @Test
  public void testConstructionMissingInstanceConfigMap() {
    Map<String, InstanceConfig> emptyMap = new HashMap<>();
    try {
      new TrieClusterTopology(_instanceNames, emptyMap, _clusterConfig);
      Assert.fail("Expecting Instance Config is not found exception");
    } catch (HelixException e) {
      Assert.assertTrue(e.getMessage().contains("is not found!"));
    }
  }

  @Test
  public void testConstructionNormal() {
    try {
      _trie = new TrieClusterTopology(_instanceNames, _instanceConfigMap, _clusterConfig);
    } catch (HelixException e) {
      Assert.fail("Not expecting HelixException");
    }
  }

  @Test(dependsOnMethods = "testConstructionNormal")
  public void testGetInstancesUnderInvalidzone() {
    Map<String, String> domains = new HashMap<>();
    domains.put("group", "1");
    domains.put("InvalidDomain", "2");
    try {
      _trie.getTopologyUnderDomain(domains);
      Assert.fail("Expecting IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("The input domain is not valid"));
    }
  }

  @Test(dependsOnMethods = "testGetInstancesUnderInvalidzone")
  public void testGetInstancesUnderzoneUnderInvalidDomain() {
    Map<String, String> domains = new HashMap<>();
    domains.put("group", "1");
    // missing the domain "zone"
    domains.put("rack", "10");
    try {
      _trie.getTopologyUnderDomain(domains);
      Map<String, List<String>> results = _trie.getTopologyUnderDomain(domains);
      Assert.fail("IllegalArgumentException is expected");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("The input domain is not valid"));
    }
  }

  @Test(dependsOnMethods = "testGetInstancesUnderzoneUnderInvalidDomain")
  public void testGetInstancesUnderNonExistingDomain() {
    Map<String, String> domains = new HashMap<>();
    domains.put("group", "1");
    // zone_10 does not exist
    domains.put("zone", "10");
    try {
      _trie.getTopologyUnderDomain(domains);
      Assert.fail("Helix Exception is expected");
    } catch (IllegalArgumentException e) {
      Assert
          .assertTrue(e.getMessage().contains("The input domain zone does not have the value 10"));
    }
  }

  @Test(dependsOnMethods = "testGetInstancesUnderNonExistingDomain")
  public void testGetInstancesUnderSpecifiedDomain() {
    Map<String, String> domains = new HashMap<>();
    domains.put("zone", "0");
    domains.put("group", "0");
    Map<String, List<String>> results = _trie.getTopologyUnderDomain(domains);
    Assert.assertEquals(results.size(), 1);
    Assert.assertTrue(results.containsKey("/group:0/zone:0/rack:0"));
    Assert.assertEquals(results.get("/group:0/zone:0/rack:0").size(), 5);
    Assert.assertTrue(results.get("/group:0/zone:0/rack:0").contains("/host:0"));
  }

  @Test(dependsOnMethods = "testGetInstancesUnderSpecifiedDomain")
  public void testGetInstancesUnderSpecifiedPath() {
    String path = "/group:0/zone:0";
    Map<String, List<String>> results = _trie.getTopologyUnderPath(path);
    Assert.assertEquals(results.size(), 1);
    Assert.assertTrue(results.containsKey("/group:0/zone:0/rack:0"));
    Assert.assertEquals(results.get("/group:0/zone:0/rack:0").size(), 5);
    Assert.assertTrue(results.get("/group:0/zone:0/rack:0").contains("/host:0"));
  }

  @Test(dependsOnMethods = "testGetInstancesUnderSpecifiedPath")
  public void testGetInstancesUnderDomainType() {
    String domainType = "zone";
    Map<String, List<String>> results = _trie.getTopologyUnderDomainType(domainType);
    Assert.assertEquals(results.size(), 8);
    Assert.assertEquals(results.get("/group:0/zone:0").size(), 5);
    Assert.assertTrue(results.get("/group:0/zone:0").contains("/rack:0/host:0"));
  }

  @Test(dependsOnMethods = "testGetInstancesUnderDomainType")
  public void testGetInstancesUnderFaultZone() {
    Map<String, List<String>> results = _trie.getInstancesUnderFaultZone();
    Assert.assertEquals(results.size(), 8);
    Assert.assertEquals(results.get("/group:0/zone:0/rack:0").size(), 5);
    Assert.assertTrue(results.get("/group:0/zone:0/rack:0").contains("/host:0"));
  }

  @Test(dependsOnMethods = "testGetInstancesUnderSpecifiedDomain")
  public void testGetClusterTopology() {
    Map<String, List<String>> results = _trie.getClusterTopology();
    Assert.assertEquals(results.size(), 4);
    Assert.assertEquals(results.keySet(), new HashSet<>(Arrays.asList("/group:0", "/group:1",
        "/group:2", "/group:3")));
  }

  private void createInstanceConfigMap() {
    for (int i = 0; i < _instanceNames.size(); i++) {
      String instance = _instanceNames.get(i);
      InstanceConfig config = new InstanceConfig(instance);
      // create 2 groups, 4 zones, and 4 racks.
      config.setDomain(String.format("group=%s, zone=%s, rack=%s, host=%s", i % (_numOfNodes / 10),
          i % (_numOfNodes / 5), i % (_numOfNodes / 5), instance));
      _instanceConfigMap.put(instance, config);
    }
  }

  private void createClusterConfig() {
    _clusterConfig = new ClusterConfig("test");
    _clusterConfig.setTopologyAwareEnabled(true);
    _clusterConfig.setTopology("/group/zone/rack/host");
    _clusterConfig.setFaultZoneType("rack");
  }
}