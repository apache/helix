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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    _topologyKeys = getTopologyDef(clusterConfig);
    _faultZoneType = clusterConfig.getFaultZoneType();
    _invalidInstances = getInvalidInstancesFromConfig(instanceConfigMap, _topologyKeys);
    instanceConfigMap.keySet().removeAll(_invalidInstances);
    _rootNode = constructTrie(instanceConfigMap, _topologyKeys);
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

  /**
   * Return all the paths from a TrieNode as a set.
   * @param node the node from where to collect all the nodes' paths.
   * @return All the paths under the node.
   */
  public Set<String> getPathUnderNode(TrieNode node) {
    Set<String> resultMap = new HashSet<>();
    Deque<TrieNode> nodeStack = new ArrayDeque<>();
    nodeStack.push(node);
    while (!nodeStack.isEmpty()) {
      node = nodeStack.pop();
      if (node.getChildren().isEmpty()) {
        resultMap.add(node.getPath());
      } else {
        for (TrieNode child : node.getChildren().values()) {
          nodeStack.push(child);
        }
      }
    }
    return resultMap;
  }

  /**
   * Get a specific node in the trie given a map of domain type and its value.
   * @param domainMap a map of domain type and the corresponding value
   * @return a trie node
   */
  public TrieNode getNode(LinkedHashMap<String, String> domainMap) {
    TrieNode curNode = _rootNode;
    TrieNode nextNode;
    for (Map.Entry<String, String> entry : domainMap.entrySet()) {
      nextNode = curNode.getChildren().get(entry.getKey() + CONNECTOR + entry.getValue());
      if (nextNode == null) {
        throw new IllegalArgumentException(String
            .format("The input domain %s does not have the value %s", entry.getKey(),
                entry.getValue()));
      }
      curNode = nextNode;
    }
    return curNode;
  }

  /**
   * Get all the starting nodes for a certain domain type. E.g., if the domainType is "zone", it
   * will return the list of trie nodes that represent zone:0, zone:1, zone:2, etc.
   * @param domainType a specific domain type
   * @return a list of trie nodes
   */
  public List<TrieNode> getStartNodes(String domainType) {
    List<TrieNode> results = new ArrayList<>();
    TrieNode curNode = _rootNode;
    Deque<TrieNode> nodeStack = new ArrayDeque<>();
    nodeStack.push(curNode);
    while (!nodeStack.isEmpty()) {
      curNode = nodeStack.pop();
      if (curNode.getNodeKey().equals(domainType)) {
        results.add(curNode);
      } else {
        for (TrieNode child : curNode.getChildren().values()) {
          nodeStack.push(child);
        }
      }
    }
    return results;
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

  private List<String> getInvalidInstancesFromConfig(Map<String, InstanceConfig> instanceConfigMap,
      final String[] topologyKeys) {
    List<String> invalidInstances = new ArrayList<>();
    for (String instanceName : instanceConfigMap.keySet()) {
      try {
        Map<String, String> domainAsMap = instanceConfigMap.get(instanceName).getDomainAsMap();
        for (String key : topologyKeys) {
          String value = domainAsMap.get(key);
          if (value == null || value.length() == 0) {
            logger.info(String.format("Domain %s for instance %s is not set", domainAsMap.get(key),
                instanceName));
            invalidInstances.add(instanceName);
            break;
          }
        }
      } catch (IllegalArgumentException e) {
        invalidInstances.add(instanceName);
      }
    }
    return invalidInstances;
  }

  // Note that we do not validate whether topology-aware is enabled or fault zone type is
  // defined, as they do not block the construction of the trie
  private String[] getTopologyDef(ClusterConfig clusterConfig) {
    String[] topologyDef;
    String topologyDefInConfig = clusterConfig.getTopology();
    if (topologyDefInConfig == null || !topologyDefInConfig.trim().startsWith(DELIMITER)) {
      throw new HelixException(String.format("The topology of cluster %s is invalid!",
          clusterConfig.getClusterName()));
    }
    // A list of all keys in cluster topology, e.g., a cluster topology defined as
    // /group/zone/rack/host will return ["group", "zone", "rack", "host"].
    topologyDef =
        Arrays.asList(topologyDefInConfig.split(DELIMITER)).stream().map(str -> str.trim())
            .filter(str -> !str.isEmpty()).collect(Collectors.toList()).toArray(new String[0]);
    if (topologyDef.length == 0) {
      throw new HelixException(String.format("The topology of cluster %s is not correctly defined",
          clusterConfig.getClusterName()));
    }
    return topologyDef;
  }

  /**
   * Constructs a trie based on the provided instance config map. It loops through all instance
   * configs and constructs the trie in a top down manner.
   */
  private TrieNode constructTrie(Map<String, InstanceConfig> instanceConfigMap,
      final String[] topologyKeys) {
    TrieNode rootNode = new TrieNode("", "ROOT");
    Map<String, Map<String, String>> instanceDomainsMap = new HashMap<>();
    instanceConfigMap.entrySet().forEach(
        entry -> instanceDomainsMap.put(entry.getKey(), entry.getValue().getDomainAsMap()));

    for (Map.Entry<String, Map<String, String>> entry : instanceDomainsMap.entrySet()) {
      TrieNode curNode = rootNode;
      StringBuilder path = new StringBuilder();
      for (int i = 0; i < topologyKeys.length; i++) {
        String key = topologyKeys[i] + CONNECTOR + entry.getValue().get(topologyKeys[i]);
        path.append(DELIMITER).append(key);
        TrieNode nextNode = curNode.getChildren().get(key);
        if (nextNode == null) {
          nextNode = new TrieNode(path.toString(), topologyKeys[i]);
        }
        curNode.addChild(key, nextNode);
        curNode = nextNode;
      }
    }
    return rootNode;
  }
}