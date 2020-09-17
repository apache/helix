package org.apache.helix.api.topology;

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
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterTrie;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.TrieNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.helix.model.ClusterTrie.CONNECTOR;
import static org.apache.helix.model.ClusterTrie.DELIMITER;


public class ClusterTopology {
  private static Logger logger = LoggerFactory.getLogger(ClusterTopology.class);

  private final ClusterTrie _trieClusterTopology;

  public ClusterTopology(final List<String> liveNodes,
      final Map<String, InstanceConfig> instanceConfigMap, ClusterConfig clusterConfig) {
    _trieClusterTopology = new ClusterTrie(liveNodes, instanceConfigMap, clusterConfig);
  }

  /**
   * Return the whole topology of a cluster as a map. The key of the map is the first level of
   * domain, and the value is a list of string that represents the path to each end node in that
   * domain. E.g., assume the topology is defined as /group/zone/rack/host, the result may be {
   * ["/group:0": {"/zone:0/rack:0/host:0", "/zone:1/rack:1/host:1"}], ["/group:1": {"/zone:1
   * /rack:1/host:1", "/zone:1/rack:1/host:2"}]}
   */
  public Map<String, List<String>> getTopologyMap() {
    return getTopologyUnderDomain(new HashMap<>());
  }

  /**
   * Return all the instances under fault zone type. The key of the returned map is each fault
   * zone name, and the value is a list of string that represents the path to each end node in
   * that fault zone.
   * @return , e.g. if the fault zone is "zone", it may return {["/group:0/zone:0": {"rack:0/host
   * :0", "rack:1/host:1"}, ["/group:0/zone:1": {"/rack:0:host:2", "/rack:1/host:3"}]}
   */
  public Map<String, List<String>> getFaultZoneMap() {
    String faultZone = _trieClusterTopology.getFaultZoneType();
    if (faultZone == null) {
      throw new IllegalArgumentException("The fault zone in cluster config is not defined");
    }
    return getTopologyUnderDomainType(faultZone);
  }

  /**
   * Return the instances whose domain field is not valid
   */
  public List<String> getInvalidInstances() {
    return _trieClusterTopology.getInvalidInstances();
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
  private Map<String, List<String>> getTopologyUnderDomain(Map<String, String> domain) {
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
   * Return the full topology of a certain domain type.
   * @param domainType a specific type of domain, e.g. zone
   * @return the topology of the given domain type, e.g. {["/group:0/zone:0": {"rack:0/host:0",
   * "rack:1/host:1"}, ["/group:0/zone:1": {"/rack:0:host:2", "/rack:1/host:3"}]}
   */
  private Map<String, List<String>> getTopologyUnderDomainType(String domainType) {
    String[] topologyKeys = _trieClusterTopology.getTopologyKeys();
    if (domainType.equals(topologyKeys[0])) {
      return getTopologyMap();
    }
    Map<String, List<String>> results = new HashMap<>();
    String parentDomainType = null;
    for (int i = 1; i < topologyKeys.length; i++) {
      if (topologyKeys[i].equals(domainType)) {
        parentDomainType = topologyKeys[i - 1];
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
   * Return the topology under a certain path as a map. The key of the returned map is the next
   * level domain, and the value is a list of string that represents the path to each end node in
   * that domain.
   * @param path a path to a certain Trie node, e.g. /group:1/zone:2
   * @return the topology under the given domain, e.g. {["/group:1/zone:2/rack:0": {"/host:0",
   * "/host:1"}, ["/group:1/zone:2/rack:1": {"/host:2", "/host:3"}]}
   */
  private Map<String, List<String>> getTopologyUnderPath(String path) {
    Map<String, String> domain = convertPathToDomain(path);
    return getTopologyUnderDomain(domain);
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
    String[] topologyKeys = _trieClusterTopology.getTopologyKeys();
    for (int i = 0; i < domain.size(); i++) {
      if (!domain.containsKey(topologyKeys[i])) {
        throw new IllegalArgumentException(String
            .format("The input domain is not valid, the key %s is required", topologyKeys[i]));
      } else {
        orderedDomain.put(topologyKeys[i], domain.get(topologyKeys[i]));
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
    TrieNode curNode = _trieClusterTopology.getRootNode();
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
    TrieNode curNode = _trieClusterTopology.getRootNode();
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

  private Map<String, String> convertPathToDomain(String path) {
    Map<String, String> results = new HashMap<>();
    for (String part : path.substring(1).split(DELIMITER)) {
      results.put(part.substring(0, part.indexOf(CONNECTOR)),
          part.substring(part.indexOf(CONNECTOR) + 1));
    }
    return results;
  }
}
