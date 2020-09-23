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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestClusterTrie {
  private ClusterTrie _trie;

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
      new ClusterTrie(_instanceNames, emptyMap, _clusterConfig);
      Assert.fail("Expecting instance config not found exception");
    } catch (HelixException e) {
      Assert.assertTrue(e.getMessage().contains("is not found!"));
    }
  }

  @Test
  public void testConstructionMissingTopology() {
    _clusterConfig.setTopology(null);
    try {
      new ClusterTrie(_instanceNames, _instanceConfigMap, _clusterConfig);
      Assert.fail("Expecting topology not set exception");
    } catch (HelixException e) {
      Assert.assertTrue(e.getMessage().contains("is invalid!"));
    }
    _clusterConfig.setTopology("/group/zone/rack/host");
  }

  @Test
  public void testConstructionInvalidTopology() {
    _clusterConfig.setTopology("invalidTopology");
    try {
      new ClusterTrie(_instanceNames, _instanceConfigMap, _clusterConfig);
      Assert.fail("Expecting topology invalid exception");
    } catch (HelixException e) {
      Assert.assertTrue(e.getMessage().contains("is invalid!"));
    }
    _clusterConfig.setTopology("/group/zone/rack/host");
  }

  @Test
  public void testConstructionNormal() {
    try {
      _trie = new ClusterTrie(_instanceNames, _instanceConfigMap, _clusterConfig);
    } catch (HelixException e) {
      Assert.fail("Not expecting HelixException");
    }
  }

  @Test
  public void testConstructionNormalWithSpace() {
    _clusterConfig.setTopology("/ group/ zone/rack/host");
    try {
      _trie = new ClusterTrie(_instanceNames, _instanceConfigMap, _clusterConfig);
    } catch (HelixException e) {
      Assert.fail("Not expecting HelixException");
    }
    String[] topologyDef = _trie.getTopologyKeys();
    Assert.assertEquals(topologyDef[0], "group");
    Assert.assertEquals(topologyDef[1], "zone");
    _clusterConfig.setTopology("/group/zone/rack/host");
  }

  @Test
  public void testConstructionNormalWithInvalidConfig() {
    String instance = "invalidInstance";
    InstanceConfig config = new InstanceConfig(instance);
    config.setDomain(String.format("invaliddomain=%s, zone=%s, rack=%s, host=%s", 1, 2, 3, 4));
    _instanceConfigMap.put(instance, config);
    try {
      _trie = new ClusterTrie(_instanceNames, _instanceConfigMap, _clusterConfig);
    } catch (HelixException e) {
      Assert.fail("Not expecting HelixException");
    }
    Assert.assertEquals(_trie.getInvalidInstances().size(), 1);
    Assert.assertEquals(_trie.getInvalidInstances().get(0), instance );
    _instanceConfigMap.remove(instance);
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