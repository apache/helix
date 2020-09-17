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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.helix.HelixException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a class that uses a trie data structure to represent cluster topology. Each node
 * except the terminal node represents a certain domain in the topology, and an terminal node
 * represents an instance in the cluster.
 */
public class ClusterTrie {
  public static final String DELIMITER = "/";
  public static final String CONNECTOR = ":";

  private static Logger logger = LoggerFactory.getLogger(ClusterTrie.class);
  private TrieNode _rootNode;
  private String[] _topologyKeys;
  private String _faultZoneType;
  private List<String> _invalidInstances = new ArrayList<>();

  public ClusterTrie(final List<String> liveNodes,
      final Map<String, InstanceConfig> instanceConfigMap, ClusterConfig clusterConfig) {
    validateInstanceConfig(liveNodes, instanceConfigMap);
    validateClusterConfig(clusterConfig);
    _faultZoneType = clusterConfig.getFaultZoneType();
    _rootNode = constructTrie(instanceConfigMap);
  }


  public TrieNode getRootNode() {
    return _rootNode;
  }

  public String[] getTopologyKeys() {
    return _topologyKeys;
  }

  public  String getFaultZoneType() {
    return _faultZoneType;
  }

  public List<String> getInvalidInstances() {
    return _invalidInstances;
  }

  private void removeInvalidInstanceConfig(Map<String, InstanceConfig> instanceConfigMap) {
    for (String instanceName : instanceConfigMap.keySet()) {
      try {
        Map<String, String> domainAsMap = instanceConfigMap.get(instanceName).getDomainAsMap();
        for (String key : _topologyKeys) {
          String value = domainAsMap.get(key);
          if (value == null || value.length() == 0) {
            logger.info(String.format("Domain %s for instance %s is not set", domainAsMap.get(key),
                instanceName));
            _invalidInstances.add(instanceName);
            break;
          }
        }
      } catch (IllegalArgumentException e) {
        _invalidInstances.add(instanceName);
      }
    }
    _invalidInstances.forEach(entry -> instanceConfigMap.remove(entry));
  }

  private void validateInstanceConfig(final List<String> liveNodes,
      final Map<String, InstanceConfig> instanceConfigMap) {
    if (instanceConfigMap == null || !instanceConfigMap.keySet().containsAll(liveNodes)) {
      List<String> liveNodesCopy = new ArrayList<>();
      liveNodesCopy.addAll(liveNodes);
      throw new HelixException(String.format("Config for instances %s is not found!",
          instanceConfigMap == null ? liveNodes
              : liveNodesCopy.removeAll(instanceConfigMap.keySet())));
    }
  }

  // Note that we do not validate whether topology-aware is enabled or fault zone type is
  // defined, as they do not block the construction of the trie
  private void validateClusterConfig(ClusterConfig clusterConfig) {
    String topologyDef = clusterConfig.getTopology();
    if (topologyDef == null) {
      throw new HelixException(String.format("The topology of cluster %s is empty!",
          clusterConfig.getClusterName()));
    }
    // A list of all keys in cluster topology, e.g., a cluster topology defined as
    // /group/zone/rack/host will return ["group", "zone", "rack", "host"].
    _topologyKeys = Arrays.asList(topologyDef.trim().split(DELIMITER)).stream()
        .filter(str -> !str.isEmpty()).collect(Collectors.toList()).toArray(new String[0]);
    if (_topologyKeys.length == 0) {
      throw new HelixException(String.format("The topology of cluster %s is not correctly defined",
          clusterConfig.getClusterName()));
    }
  }

  /**
   * Constructs a trie based on the provided instance config map. It loops through all instance
   * configs and constructs the trie in a top down manner.
   */
  private TrieNode constructTrie(Map<String, InstanceConfig> instanceConfigMap) {
    TrieNode rootNode = new TrieNode(new HashMap<>(), "", "ROOT");
    removeInvalidInstanceConfig(instanceConfigMap);
    Map<String, Map<String, String>> instanceDomainsMap = new HashMap<>();
    instanceConfigMap.entrySet().forEach(
        entry -> instanceDomainsMap.put(entry.getKey(), entry.getValue().getDomainAsMap()));

    for (Map.Entry<String, Map<String, String>> entry : instanceDomainsMap.entrySet()) {
      TrieNode curNode = rootNode;
      String path = "";
      for (int i = 0; i < _topologyKeys.length; i++) {
        String key = _topologyKeys[i] + CONNECTOR + entry.getValue().get(_topologyKeys[i]);
        path = path + DELIMITER + key;
        TrieNode nextNode = curNode.getChildren().get(key);
        if (nextNode == null) {
          nextNode = new TrieNode(new HashMap<>(), path, _topologyKeys[i]);
        }
        curNode.addChild(key, nextNode);
        curNode = nextNode;
      }
    }
    return rootNode;
  }
}