package org.apache.helix.controller.rebalancer.topology;

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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixException;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterTopologyConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.util.InstanceValidationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Topology represents the structure of a cluster (the hierarchy of the nodes, its fault boundary, etc).
 * This class is intended for topology-aware partition placement.
 */
public class Topology {
  private static Logger logger = LoggerFactory.getLogger(Topology.class);
  public enum Types {
    ROOT,
    ZONE,
    INSTANCE
  }
  private static final int DEFAULT_NODE_WEIGHT = 1000;

  private final MessageDigest _md;
  private final Node _root; // root of the tree structure of all nodes;
  private final List<String> _allInstances;
  private final List<String> _liveInstances;
  private final Map<String, InstanceConfig> _instanceConfigMap;
  private final ClusterTopologyConfig _clusterTopologyConfig;

  public Topology(final List<String> allNodes, final List<String> liveNodes,
      final Map<String, InstanceConfig> instanceConfigMap, ClusterConfig clusterConfig) {
    try {
      _md = MessageDigest.getInstance("SHA-1");
    } catch (NoSuchAlgorithmException ex) {
      throw new IllegalArgumentException(ex);
    }

    _allInstances = allNodes;
    _liveInstances = liveNodes;
    _instanceConfigMap = instanceConfigMap;
    if (_instanceConfigMap == null || !_instanceConfigMap.keySet().containsAll(allNodes)) {
      throw new HelixException(String.format("Config for instances %s is not found!",
          _allInstances.removeAll(_instanceConfigMap.keySet())));
    }
    _clusterTopologyConfig = ClusterTopologyConfig.createFromClusterConfig(clusterConfig);
    _root = createClusterTree(clusterConfig);
  }

  public String getEndNodeType() {
    return _clusterTopologyConfig.getEndNodeType();
  }

  public String getFaultZoneType() {
    return _clusterTopologyConfig.getFaultZoneType();
  }

  public Node getRootNode() {
    return _root;
  }

  public List<Node> getFaultZones() {
    if (_root != null) {
      return _root.findChildren(getFaultZoneType());
    }
    return Collections.emptyList();
  }
  /**
   * Returns all leaf nodes that belong in the tree. Returns itself if this node is a leaf.
   *
   * @return
   */
  public static List<Node> getAllLeafNodes(Node root) {
    List<Node> nodes = new ArrayList<>();
    if (root.isLeaf()) {
      nodes.add(root);
    } else {
      for (Node child : root.getChildren()) {
        nodes.addAll(getAllLeafNodes(child));
      }
    }
    return nodes;
  }

  /**
   * Clone a node tree structure, with node weight updated using specified new weight,
   * and all nodes in @failedNodes as failed.
   *
   * @param root          origin root of the tree
   * @param newNodeWeight map of node name to its new weight. If absent, keep its original weight.
   * @param failedNodes   set of nodes that need to be failed.
   * @return new root node.
   */
  public static Node clone(Node root, Map<Node, Integer> newNodeWeight, Set<Node> failedNodes) {
    Node newRoot = cloneTree(root, newNodeWeight, failedNodes);
    computeWeight(newRoot);
    return newRoot;
  }

  private static Node cloneTree(Node root, Map<Node, Integer> newNodeWeight,
      Set<Node> failedNodes) {
    Node newRoot = root.clone();
    if (newNodeWeight.containsKey(root)) {
      newRoot.setWeight(newNodeWeight.get(root));
    }
    if (failedNodes.contains(root)) {
      newRoot.setFailed(true);
      newRoot.setWeight(0);
    }

    List<Node> children = root.getChildren();
    if (children != null) {
      for (Node child : children) {
        Node newChild = cloneTree(child, newNodeWeight, failedNodes);
        newChild.setParent(root);
        newRoot.addChild(newChild);
      }
    }

    return newRoot;
  }

  private Node createClusterTree(ClusterConfig clusterConfig) {
    // root
    Node root = new Node();
    root.setName("root");
    root.setId(computeId("root"));
    root.setType(Types.ROOT.name());

    // TODO: Currently we add disabled instance to the topology tree. Since they are not considered
    // TODO: in rebalance, maybe we should skip adding them to the tree for consistence.
    for (String instanceName : _allInstances) {
      InstanceConfig insConfig = _instanceConfigMap.get(instanceName);
      try {
        LinkedHashMap<String, String> instanceTopologyMap =
            computeInstanceTopologyMapHelper(_clusterTopologyConfig, instanceName, insConfig, null);
        int weight = insConfig.getWeight();
        if (weight < 0 || weight == InstanceConfig.WEIGHT_NOT_SET) {
          weight = DEFAULT_NODE_WEIGHT;
        }
        addEndNode(root, instanceName, instanceTopologyMap, weight, _liveInstances);
      } catch (IllegalArgumentException e) {
        if (InstanceValidationUtil.isInstanceEnabled(insConfig, clusterConfig)) {
          throw e;
        } else {
          logger.warn("Topology setting {} for instance {} is unset or invalid, ignore the instance!",
              insConfig.getDomainAsString(), instanceName);
        }
      }
    }
    return root;
  }

  /**
   * Construct the instance topology map for an instance.
   * The mapping is the cluster topology path name to its corresponding value.
   * @param clusterTopologyConfig
   * @param instanceName
   * @param instanceConfig
   * @param faultZoneForEarlyQuit  Nullable, if set to non-null value, the faultZone path will stop at the matched
   *                               faultZone value instead of constructing the whole path for the instance.
   */
  private static LinkedHashMap<String, String> computeInstanceTopologyMapHelper(
      ClusterTopologyConfig clusterTopologyConfig, String instanceName, InstanceConfig instanceConfig,
      String faultZoneForEarlyQuit)
      throws IllegalArgumentException {
    LinkedHashMap<String, String> instanceTopologyMap = new LinkedHashMap<>();
    if (clusterTopologyConfig.isTopologyAwareEnabled()) {
      if (clusterTopologyConfig.getTopologyKeyDefaultValue().isEmpty()) {
        // Return a ordered map using default cluster topology definition, i,e. /root/zone/instance
        String zone = instanceConfig.getZoneId();
        if (zone == null) {
          throw new IllegalArgumentException(String
              .format("ZONE_ID for instance %s is not set, fail the topology-aware placement!",
                  instanceName));
        }
        instanceTopologyMap.put(Types.ZONE.name(), zone);
        if (faultZoneForEarlyQuit != null) {
          return instanceTopologyMap;
        }
        instanceTopologyMap.put(Types.INSTANCE.name(), instanceName);
      } else {
        /*
         * Return a ordered map representing the instance path. The topology order is defined in
         * ClusterConfig.topology.
         */
        Map<String, String> domainAsMap = instanceConfig.getDomainAsMap();
        if (domainAsMap.isEmpty()) {
          throw new IllegalArgumentException(String
              .format("Domain for instance %s is not set, fail the topology-aware placement!",
                  instanceName));
        }
        int numOfMatchedKeys = 0;
        for (String key : clusterTopologyConfig.getTopologyKeyDefaultValue().keySet()) {
          // if a key does not exist in the instance domain config, using the default domain value.
          String value = domainAsMap.get(key);
          if (value == null || value.length() == 0) {
            value = clusterTopologyConfig.getTopologyKeyDefaultValue().get(key);
          } else {
            numOfMatchedKeys++;
          }
          instanceTopologyMap.put(key, value);
          if (key.equals(faultZoneForEarlyQuit)) {
            return instanceTopologyMap;
          }
        }
        if (numOfMatchedKeys < clusterTopologyConfig.getTopologyKeyDefaultValue().size()) {
          logger.warn(
              "Key-value pairs in InstanceConfig.Domain {} do not align with keys in ClusterConfig.Topology "
                  + "{}, using default domain value instead", instanceConfig.getDomainAsString(),
              clusterTopologyConfig.getTopologyKeyDefaultValue().keySet());
        }
      }
    } else {
      // TopologyAware rebalance is not enabled.
      instanceTopologyMap.put(Types.INSTANCE.name(), instanceName);
    }
    return instanceTopologyMap;
  }

  /**
   * This function returns a LinkedHashMap<String, String> object representing
   * the topology path for an instance.
   * LinkedHashMap is used here since the order of the path needs to be preserved
   * when creating the topology tree.
   *
   * @param clusterConfig         clusterConfig of the given cluster.
   * @param instanceName          name of the instance.
   * @param instanceConfig        instanceConfig to be checked.
   * @param earlyQuitForFaultZone Set to true if we only need the path till faultZone.
   *
   * @return an LinkedHashMap object representing the topology path for the input instance.
   * @throws IllegalArgumentException if input is not valid.
   */
  public static LinkedHashMap<String, String> computeInstanceTopologyMap(
      ClusterConfig clusterConfig, String instanceName, InstanceConfig instanceConfig,
      boolean earlyQuitForFaultZone) {
    ClusterTopologyConfig clusterTopologyConfig = ClusterTopologyConfig.createFromClusterConfig(clusterConfig);
    String faultZoneForEarlyQuit =
        earlyQuitForFaultZone ? clusterTopologyConfig.getFaultZoneType() : null;
    return computeInstanceTopologyMapHelper(clusterTopologyConfig, instanceName, instanceConfig, faultZoneForEarlyQuit);
  }

  /**
   * Add an end node to the tree, create all the paths to the leaf node if not present.
   */
  private void addEndNode(Node root, String instanceName, LinkedHashMap<String, String> pathNameMap,
      int instanceWeight, List<String> liveInstances) {
    Node current = root;
    List<Node> pathNodes = new ArrayList<>();
    for (Map.Entry<String, String> entry : pathNameMap.entrySet()) {
      String pathValue = entry.getValue();
      String path = entry.getKey();

      pathNodes.add(current);
      if (!current.hasChild(pathValue)) {
        buildNewNode(pathValue, path, current, instanceName, instanceWeight,
            liveInstances.contains(instanceName), pathNodes);
      } else if (path.equals(_clusterTopologyConfig.getEndNodeType())) {
        throw new HelixException(
            "Failed to add topology node because duplicate leaf nodes are not allowed. Duplicate node name: "
                + pathValue);
      }
      current = current.getChild(pathValue);
    }
  }

  private Node buildNewNode(String name, String type, Node parent, String instanceName,
      int instanceWeight, boolean isLiveInstance, List<Node> pathNodes) {
    Node n = new Node();
    n.setName(name);
    n.setId(computeId(name));
    n.setType(type);
    n.setParent(parent);
    // if it is leaf node, create an InstanceNode instead
    if (type.equals(_clusterTopologyConfig.getEndNodeType())) {
      n = new InstanceNode(n, instanceName);
      if (isLiveInstance) {
        // node is alive
        n.setWeight(instanceWeight);
        // add instance weight to all of its parent nodes.
        for (Node node : pathNodes) {
          node.addWeight(instanceWeight);
        }
      } else {
        n.setFailed(true);
        n.setWeight(0);
      }
    }
    parent.addChild(n);
    return n;
  }

  private long computeId(String name) {
    byte[] h = _md.digest(name.getBytes());
    return bstrTo32bit(h);
  }

  private static void computeWeight(Node node) {
    int weight = 0;
    for (Node child : node.getChildren()) {
      if (!child.isFailed()) {
        weight += child.getWeight();
      }
    }
    node.setWeight(weight);
  }

  private long bstrTo32bit(byte[] bstr) {
    if (bstr.length < 4) {
      throw new IllegalArgumentException("hashed is less than 4 bytes!");
    }
    // need to "simulate" unsigned int
    return (long) (((ord(bstr[0]) << 24) | (ord(bstr[1]) << 16) | (ord(bstr[2]) << 8) | (ord(
        bstr[3])))) & 0xffffffffL;
  }

  private int ord(byte b) {
    return b & 0xff;
  }
}
