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
public class TrieClusterTopology {
  private static Logger logger = LoggerFactory.getLogger(TrieClusterTopology.class);
  private static final String DELIMITER = "/";
  private static final String CONNECTOR = ":";

  private final TrieNode _rootNode;
  private final String[] _topologyKeys;
  private final String _faultZoneType;

  public TrieClusterTopology(final List<String> liveNodes,
      final Map<String, InstanceConfig> instanceConfigMap, ClusterConfig clusterConfig) {
    if (instanceConfigMap == null || !instanceConfigMap.keySet().containsAll(liveNodes)) {
      throw new HelixException(String.format("Config for instances %s is not found!",
          instanceConfigMap == null ? liveNodes : liveNodes.removeAll(instanceConfigMap.keySet())));
    }
    // A list of all keys in cluster topology, e.g., a cluster topology defined as
    // /group/zone/rack/host will return ["group", "zone", "rack", "host"].
    _topologyKeys = Arrays.asList(clusterConfig.getTopology().trim().split(DELIMITER)).stream()
        .filter(str -> !str.isEmpty()).collect(Collectors.toList()).toArray(new String[0]);
    _faultZoneType = clusterConfig.getFaultZoneType();
    _rootNode = new TrieNode(new HashMap<>(), "", "ROOT");
    constructTrie(instanceConfigMap);
  }

  /**
   * Return the topology of a cluster as a map. The key of the map is the first level of
   * domain, and the value is a list of string that represents the path to each end node in that
   * domain. E.g., assume the topology is defined as /group/zone/rack/host, the result may be {
   * ["/group:0": {"/zone:0/rack:0/host:0", "/zone:1/rack:1/host:1"}], ["/group:1": {"/zone:1
   * /rack:1/host:1", "/zone:1/rack:1/host:2"}]}
   */
  public Map<String, List<String>> getClusterTopology() {
    return getTopologyUnderDomain(new HashMap<>());
  }

  /**
   * Return the topology under a certain domain as a map. The key of the returned map is the next
   * level domain, and the value is a list of string that represents the path to each end node in
   * that domain.
   * @param domain A map defining the domain name and its value, e.g. {["group": "1"], ["zone",
   *               "2"]}
   * @return the topology under the given domain, e.g. {["/group:1/zone:2/rack:0": {"/host:0",
   * "/host:1"}, ["/group:1/zone:2/rack:1": {"/host:2", "/host:3"}]}
   */
  public Map<String, List<String>> getTopologyUnderDomain(Map<String, String> domain) {
    LinkedHashMap<String, String> orderedDomain = validateAndOrderDomain(domain);
    TrieNode startNode = getStartNode(orderedDomain);
    Map<String, TrieNode> children = startNode.getChildren();
    Map<String, List<String>> results = new HashMap<>();
    children.entrySet().forEach(child -> {
      results.put(startNode.getPath() + DELIMITER + child.getKey(),
          truncatePath(getPathUnderNode(child.getValue()), child.getValue().getPath()));
    });
    return results;
  }

  /**
   * Return the topology under a certain path as a map. The key of the returned map is the next
   * level domain, and the value is a list of string that represents the path to each end node in
   * that domain.
   * @param path a path to a certain Trie node, e.g. /group:1/zone:2
   * @return the topology under the given domain, e.g. {["/group:1/zone:2/rack:0": {"/host:0",
   * "/host:1"}, ["/group:1/zone:2/rack:1": {"/host:2", "/host:3"}]}
   */
  public Map<String, List<String>> getTopologyUnderPath(String path) {
    Map<String, String> domain = convertPathToDomain(path);
    return getTopologyUnderDomain(domain);
  }

  /**
   * Return the full topology of a certain domain type.
   * @param domainType a specific type of domain, e.g. zone
   * @return the topology of the given domain type, e.g. {["/group:0/zone:0": {"rack:0/host:0",
   * "rack:1/host:1"}, ["/group:0/zone:1": {"/rack:0:host:2", "/rack:1/host:3"}]}
   */
  public Map<String, List<String>> getTopologyUnderDomainType(String domainType) {
    if (domainType.equals(_topologyKeys[0])) {
      return getClusterTopology();
    }
    Map<String, List<String>> results = new HashMap<>();
    String parentDomainType = null;
    for (int i = 1; i < _topologyKeys.length; i++) {
      if (_topologyKeys[i].equals(domainType)) {
        parentDomainType = _topologyKeys[i - 1];
        break;
      }
    }
    // get all the starting nodes for the domain type
    List<TrieNode> startNodes = getStartNodes(parentDomainType);
    for (TrieNode startNode : startNodes) {
      results.putAll(getTopologyUnderPath(startNode.getPath()));
    }
    return results;
  }

  /**
   * Return all the end nodes under fault zone type. The key of the returned map is the next
   * level domain, and the value is a list of string that represents the path to each end node in
   * that domain.
   * @return , e.g. if the fault zone is "zone", it may return {["/group:0/zone:0": {"rack:0/host
   * :0", "rack:1/host:1"}, ["/group:0/zone:1": {"/rack:0:host:2", "/rack:1/host:3"}]}
   */
  public Map<String, List<String>> getInstancesUnderFaultZone() {
    return getTopologyUnderDomainType(_faultZoneType);
  }

  private Map<String, String> convertPathToDomain(String path) {
    Map<String, String> results = new HashMap<>();
    for (String part : path.substring(1).split(DELIMITER)) {
      results.put(part.substring(0, part.indexOf(CONNECTOR)),
          part.substring(part.indexOf(CONNECTOR) + 1));
    }
    return results;
  }

  /**
   * Validate the domain provided has continuous fields in cluster topology definition. If it
   * has, order the domain based on cluster topology definition. E.g. if the cluster topology is
   * /group/zone/rack/instance, and domain is provided as {["zone": "1"], ["group", "2"]} will be
   * reordered in a LinkedinHashMap as {["group", "2"], ["zone": "1"]}
   */
  private LinkedHashMap<String, String> validateAndOrderDomain(Map<String, String> domain) {
    LinkedHashMap<String, String> orderedDomain = new LinkedHashMap<>();
    if (domain == null) {
      throw new IllegalArgumentException("The domain should not be null");
    }
    for (int i = 0; i < domain.size(); i++) {
      if (!domain.containsKey(_topologyKeys[i])) {
        throw new IllegalArgumentException(String
            .format("The input domain is not valid, the key %s is required", _topologyKeys[i]));
      } else {
        orderedDomain.put(_topologyKeys[i], domain.get(_topologyKeys[i]));
      }
    }
    return orderedDomain;
  }

  /**
   * Truncate each path in the given set and only retain path starting from current node's
   * children to each end node.
   * @param toRemovePath The path from root to current node. It should be removed so that users
   *                     can get a better view.
   */
  private List<String> truncatePath(Set<String> paths, String toRemovePath) {
    List<String> results = new ArrayList<>();
    paths.forEach(path -> {
      String truncatedPath = path.replace(toRemovePath, "");
      results.add(truncatedPath);
    });
    return results;
  }

  /**
   * Return all the paths from a TrieNode as a set.
   * @param node the node from where to collect all the nodes' paths.
   * @return All the paths under the node.
   */
  private Set<String> getPathUnderNode(TrieNode node) {
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

  private TrieNode getStartNode(LinkedHashMap<String, String> domain) {
    TrieNode curNode = _rootNode;
    TrieNode nextNode;
    for (Map.Entry<String, String> entry : domain.entrySet()) {
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

  private List<TrieNode> getStartNodes(String domain) {
    List<TrieNode> results = new ArrayList<>();
    TrieNode curNode = _rootNode;
    Deque<TrieNode> nodeStack = new ArrayDeque<>();
    nodeStack.push(curNode);
    while (!nodeStack.isEmpty()) {
      curNode = nodeStack.pop();
      if (curNode.getDomainType().equals(domain)) {
        results.add(curNode);
      } else {
        for (TrieNode child : curNode.getChildren().values()) {
          nodeStack.push(child);
        }
      }
    }
    return results;
  }

  private void removeInvalidInstanceConfig(Map<String, InstanceConfig> instanceConfigMap) {
    Set<String> toRemoveConfig = new HashSet<>();
    for (String instanceName : instanceConfigMap.keySet()) {
      Map<String, String> domainAsMap = instanceConfigMap.get(instanceName).getDomainAsMap();
      if (domainAsMap.isEmpty()) {
        logger.info(String.format("Domain for instance %s is not set", instanceName));
        toRemoveConfig.add(instanceName);
      } else {
        for (String key : _topologyKeys) {
          String value = domainAsMap.get(key);
          if (value == null || value.length() == 0) {
            logger.info(String.format("Domain %s for instance %s is not set", domainAsMap.get(key),
                instanceName));
            toRemoveConfig.add(instanceName);
            break;
          }
        }
      }
    }
    toRemoveConfig.forEach(entry -> instanceConfigMap.remove(entry));
  }

  /**
   * Constructs a trie based on the provided instance config map. It loops through all instance
   * configs and constructs the trie in a top down manner.
   */
  private void constructTrie(Map<String, InstanceConfig> instanceConfigMap) {
    removeInvalidInstanceConfig(instanceConfigMap);
    Map<String, Map<String, String>> instanceDomainsMap = new HashMap<>();
    instanceConfigMap.entrySet().forEach(
        entry -> instanceDomainsMap.put(entry.getKey(), entry.getValue().getDomainAsMap()));

    for (Map.Entry<String, Map<String, String>> entry : instanceDomainsMap.entrySet()) {
      TrieNode curNode = _rootNode;
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
  }

  private static class TrieNode {
    // A mapping between trie key and children nodes.
    private Map<String, TrieNode> _children;

    // the complete path/prefix leading to the current node.
    private final String _path;

    private final String _domainType;

    TrieNode(Map<String, TrieNode> children, String path, String domainType) {
      _children = children;
      _path = path;
      _domainType = domainType;
    }

    public Map<String, TrieNode> getChildren() {
      return _children;
    }

    public String getPath() {
      return _path;
    }

    public String getDomainType() {
      return _domainType;
    }

    public void addChild(String key, TrieNode node) {
      _children.put(key, node);
    }
  }
}