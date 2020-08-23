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
import java.util.Set;

import org.apache.helix.HelixException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestTrieClusterTopology {
  private TrieClusterTopology _trie;

  final List<String> _instanceNames = new ArrayList<>();
  final Map<String, InstanceConfig> _instanceConfigMap = new HashMap<>();
  private ClusterConfig _clusterConfig;
  final int _numOfNodes = 20;

  @BeforeClass
  public void beforeClass() {
    for (int i = 0; i < _numOfNodes; i++) {
      _instanceNames.add("node" + i);
    }
    createInstanceConfigMap();
    createClusterConfig();
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
  public void testGetInstancesUnderInvalidZone() {
    Map<String, String> domains = new HashMap<>();
    domains.put("Group", "1");
    domains.put("InvalidDomain", "2");
    try {
      _trie.getTopologyUnderDomain(domains);
      Assert.fail("Expecting IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("The input domain is not valid"));
    }
  }

  @Test(dependsOnMethods = "testGetInstancesUnderInvalidZone")
  public void testGetInstancesUnderZoneUnderInvalidDomain() {
    Map<String, String> domains = new HashMap<>();
    domains.put("Group", "1");
    // missing the domain "Zone"
    domains.put("Rack", "10");
    try {
      _trie.getTopologyUnderDomain(domains);
      Map<String, Set<String>> results = _trie.getTopologyUnderDomain(domains);
      Assert.fail("IllegalArgumentException is expected");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("The input domain is not valid"));
    }
  }

  @Test(dependsOnMethods = "testGetInstancesUnderZoneUnderInvalidDomain")
  public void testGetInstancesUnderNonExistingDomain() {
    Map<String, String> domains = new HashMap<>();
    domains.put("Group", "1");
    // Zone_10 does not exist
    domains.put("Zone", "10");
    try {
      _trie.getTopologyUnderDomain(domains);
      Map<String, Set<String>> results = _trie.getTopologyUnderDomain(domains);
      Assert.fail("Helix Exception is expected");
    } catch (IllegalArgumentException e) {
      Assert
          .assertTrue(e.getMessage().contains("The input domain Zone does not have the value 10"));
    }
  }

  @Test(dependsOnMethods = "testGetInstancesUnderNonExistingDomain")
  public void testGetInstancesUnderSpecifiedDomain() {
    Map<String, String> domains = new HashMap<>();
    domains.put("Zone", "0");
    domains.put("Group", "0");
    _trie.getTopologyUnderDomain(domains);
    Map<String, Set<String>> results = _trie.getTopologyUnderDomain(domains);
    Assert.assertEquals(results.size(), 5);
    Assert.assertTrue(results.containsKey("Rack_0"));
    Assert.assertEquals(results.get("Rack_0").size(), 1);
    Assert.assertTrue(results.get("Rack_0").contains("Host_node0"));
  }

  @Test(dependsOnMethods = "testGetInstancesUnderSpecifiedDomain")
  public void testGetInstancesUnderAllZones() {
    Map<String, Set<String>> results = _trie.getClusterTopology();
    Assert.assertEquals(results.size(), 2);
    Assert.assertEquals(results.keySet(), new HashSet<>(Arrays.asList("Group_0", "Group_1")));
  }

  private void createInstanceConfigMap() {
    for (int i = 0; i < _instanceNames.size(); i++) {
      String instance = _instanceNames.get(i);
      InstanceConfig config = new InstanceConfig(instance);
      // create 2 groups, 4 zones, and 10 racks.
      config.setDomain(String.format("Group=%s, Zone=%s, Rack=%s, Host=%s", i % (_numOfNodes / 10),
          i % (_numOfNodes / 5), i % (_numOfNodes / 2), instance));
      _instanceConfigMap.put(instance, config);
    }
  }

  private void createClusterConfig() {
    _clusterConfig = new ClusterConfig("test");
    _clusterConfig.setTopologyAwareEnabled(true);
    _clusterConfig.setTopology("/Group/Zone/Rack/Host");
  }
}