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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixException;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Topology represents the structure of a cluster (the hierarchy of the nodes, its fault boundary, etc).
 * This class is intended for topology-aware partition placement.
 */
public class Topology {
  private static Logger logger = LoggerFactory.getLogger(Topology.class);

  public enum Types {
    ROOT, ZONE, INSTANCE
  }

  private static final int DEFAULT_NODE_WEIGHT = 1000;

  private final MessageDigest _md;
  private final Node _root; // root of the tree structure of all nodes;
  private final List<String> _allInstances;
  private final List<String> _liveInstances;
  private final Map<String, InstanceConfig> _instanceConfigMap;
  private final ClusterConfig _clusterConfig;
  private static final String DEFAULT_DOMAIN_PREFIX = "Helix_default_";

  private String _faultZoneType;
  private String _endNodeType;
  private LinkedHashSet<String> _clusterTopologyKeys;
  private Map<String, String> _defaultDomainPathValues = new HashMap<>();

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
    _clusterConfig = clusterConfig;
    _clusterTopologyKeys = new LinkedHashSet<>();

    if (clusterConfig.isTopologyAwareEnabled()) {
      if (_clusterConfig.getTopology() != null) {
        // Customized cluster topology definition is configured.
        _endNodeType = populateClusterTopologySetting(clusterConfig, _clusterTopologyKeys,
            _defaultDomainPathValues);
        _faultZoneType = clusterConfig.getFaultZoneType();
        if (_faultZoneType == null) {
          _faultZoneType = _endNodeType;
        } else if (!_clusterTopologyKeys.contains(_faultZoneType)) {
          throw new HelixException(String
              .format("Invalid fault zone type %s, not present in topology definition %s.",
                  _faultZoneType, _clusterConfig.getTopology()));
        }
      } else {
        // Use default cluster topology definition, i,e. /root/zone/instance
        _endNodeType = Types.INSTANCE.name();
        _faultZoneType = Types.ZONE.name();
      }
    } else {
      _endNodeType = Types.INSTANCE.name();
      _faultZoneType = Types.INSTANCE.name();
    }
    _root = createClusterTree();
  }

  public String getEndNodeType() {
    return _endNodeType;
  }

  public String getFaultZoneType() {
    return _faultZoneType;
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
      for (int i = 0; i < children.size(); i++) {
        Node newChild = cloneTree(children.get(i), newNodeWeight, failedNodes);
        newChild.setParent(root);
        newRoot.addChild(newChild);
      }
    }

    return newRoot;
  }

  private Node createClusterTree() {
    // root
    Node root = new Node();
    root.setName("root");
    root.setId(computeId("root"));
    root.setType(Types.ROOT.name());

    // TODO: Currently we add disabled instance to the topology tree. Since they are not considered
    // TODO: in relabalnce, maybe we should skip adding them to the tree for consistence.
    for (String instanceName : _allInstances) {
      InstanceConfig insConfig = _instanceConfigMap.get(instanceName);
      try {
        LinkedHashMap<String, String> instanceTopologyMap =
            computeInstanceTopologyMapHelper(_clusterConfig.isTopologyAwareEnabled(), instanceName,
                insConfig, _clusterTopologyKeys, _defaultDomainPathValues,
                null /*faultZoneForEarlyQuit*/);
        int weight = insConfig.getWeight();
        if (weight < 0 || weight == InstanceConfig.WEIGHT_NOT_SET) {
          weight = DEFAULT_NODE_WEIGHT;
        }
        addEndNode(root, instanceName, instanceTopologyMap, weight, _liveInstances);
      } catch (IllegalArgumentException e) {
        if (isInstanceEnabled(_clusterConfig, instanceName, insConfig)) {
          throw e;
        } else {
          logger
              .warn("Topology setting {} for instance {} is unset or invalid, ignore the instance!",
                  insConfig.getDomainAsString(), instanceName);
        }
      }
    }
    return root;
  }

  private boolean isInstanceEnabled(ClusterConfig clusterConfig, String instanceName,
      InstanceConfig instanceConfig) {
    return (instanceConfig.getInstanceEnabled() && (clusterConfig.getDisabledInstances() == null
        || !clusterConfig.getDisabledInstances().containsKey(instanceName)));
  }

  /**
   * Populate clusterTopologyKeys and defaultDomainPathValues from clusterConfig
   *
   * @param clusterTopologyKeys       out parameter. LinkedHashSet to be populated for cluster
   *                                  topology keys. The set will remain empty if topology aware is
   *                                  not enabled or this cluster uses zone instead of domains.
   * @param defaultDomainPathValues   out parameter. Map to be populated for all default path keys.
   *                                  The map will remain empty if topology aware is not enabled or
   *                                  this cluster uses zone instead of domains.
   * @return lastValidType in clusterConfig.topology
   */
  public static String populateClusterTopologySetting(ClusterConfig clusterConfig,
      LinkedHashSet<String> clusterTopologyKeys /*OUT*/,
      Map<String, String> defaultDomainPathValues /*OUT*/) {

    if (clusterConfig.isTopologyAwareEnabled()) {
      String topologyDef = clusterConfig.getTopology();
      if (topologyDef != null) {
        String[] topologyKeys = topologyDef.trim().split("/");
        int lastValidTypeIdx = 0;
        for (int i = 0; i < topologyKeys.length; i++) {
          if (topologyKeys[i].length() != 0) {
            clusterTopologyKeys.add(topologyKeys[i]);
            defaultDomainPathValues.put(topologyKeys[i], DEFAULT_DOMAIN_PREFIX + topologyKeys[i]);
            lastValidTypeIdx = i;
          }
        }
        if (clusterTopologyKeys.size() == 0) {
          throw new IllegalArgumentException("Invalid cluster topology definition " + topologyDef);
        }
        return topologyKeys[lastValidTypeIdx];
      }
    }
    return Types.INSTANCE.name();
  }

  /**
   * This function returns a LinkedHashMap<String, String> object representing
   * the topology path for an instance.
   * LinkedHashMap is used here since the order of the path needs to be preserved
   * when creating the topology tree.
   *
   * @param defaultDomainPathValues a const map for default path key.
   * @param faultZoneForEarlyQuit   this flag os set to true only if caller wants the path
   *                                to faultZone instead the whole path for the instance.
   * @return an LinkedHashMap object representing the topology path for the input instance.
   */
  private static LinkedHashMap<String, String> computeInstanceTopologyMapHelper(
      boolean isTopologyAwareEnabled, String instanceName, InstanceConfig instanceConfig,
      LinkedHashSet<String> clusterTopologyKeys, Map<String, String> defaultDomainPathValues,
      String faultZoneForEarlyQuit)
      throws IllegalArgumentException {
    LinkedHashMap<String, String> instanceTopologyMap = new LinkedHashMap<>();
    if (isTopologyAwareEnabled) {
      if (clusterTopologyKeys.size() == 0) {
        // Return a ordered map using default cluster topology definition, i,e. /root/zone/instance
        String zone = instanceConfig.getZoneId();
        if (zone == null) {
          throw new IllegalArgumentException(String
              .format("ZONE_ID for instance %s is not set, fail the topology-aware placement!",
                  instanceName));
        }
        instanceTopologyMap.put(Types.ZONE.name(), zone);
        if (faultZoneForEarlyQuit!=null) {
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
        for (String key : clusterTopologyKeys) {
          // if a key does not exist in the instance domain config, using the default domain value.
          String value = domainAsMap.get(key);
          if (value == null || value.length() == 0) {
            value = defaultDomainPathValues.get(key);
          } else {
            numOfMatchedKeys++;
          }
          instanceTopologyMap.put(key, value);
          if (key.equals(faultZoneForEarlyQuit)) {
            return instanceTopologyMap;
          }
        }
        if (numOfMatchedKeys != domainAsMap.size()) {
          logger.warn(
              "Key-value pairs in InstanceConfig.Domain {} do not align with keys in ClusterConfig.Topology "
                  + "{}, using default domain value instead", instanceConfig.getDomainAsString(),
              clusterTopologyKeys.toString());
        }
      }
    } else {
      // TopologyAware rebalance is not enabled.
      instanceTopologyMap.put(Types.INSTANCE.name(), instanceName);
    }
    return instanceTopologyMap;
  }

  public static LinkedHashMap<String, String> computeInstanceTopologyMap(
      ClusterConfig clusterConfig, String instanceName, InstanceConfig instanceConfig,
      boolean earlyQuitTillFaultZone)
      throws IllegalArgumentException {
    LinkedHashSet<String> clusterTopologyKeys = new LinkedHashSet<>();
    Map<String, String> defaultDomainPathValues = new HashMap<>();

    String endNodeType =
        populateClusterTopologySetting(clusterConfig, clusterTopologyKeys, defaultDomainPathValues);
    String faultZoneForEarlyQuit =
        earlyQuitTillFaultZone ? (clusterConfig.getFaultZoneType() == null ? endNodeType
            : clusterConfig.getFaultZoneType()) : null;
    return computeInstanceTopologyMapHelper(clusterConfig.isTopologyAwareEnabled(), instanceName,
        instanceConfig, clusterTopologyKeys, defaultDomainPathValues, faultZoneForEarlyQuit);
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
      } else if (path.equals(_endNodeType)) {
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
    if (type.equals(_endNodeType)) {
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
