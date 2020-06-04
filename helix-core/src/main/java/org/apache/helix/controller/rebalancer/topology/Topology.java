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
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
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
  private final ClusterConfig _clusterConfig;
  private static final String DEFAULT_PATH_PREFIX = "Helix_default_";

  private String _faultZoneType;
  private String _endNodeType;

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

    if (clusterConfig.isTopologyAwareEnabled()) {
      String topologyDef = _clusterConfig.getTopology();
      if (topologyDef != null) {
        // Customized cluster topology definition is configured.
        String[] topologyStr = topologyDef.trim().split("/");
        int lastValidTypeIdx = topologyStr.length - 1;
        while(lastValidTypeIdx >= 0) {
          if (topologyStr[lastValidTypeIdx].length() != 0) {
            break;
          }
          --lastValidTypeIdx;
        }
        if (lastValidTypeIdx < 0) {
          throw new HelixException("Invalid cluster topology definition " + topologyDef);
        }
        _endNodeType = topologyStr[lastValidTypeIdx];
        _faultZoneType = clusterConfig.getFaultZoneType();
        if (_faultZoneType == null) {
          _faultZoneType = _endNodeType;
        }
        if (Arrays.stream(topologyStr).noneMatch(type -> type.equals(_faultZoneType))) {
          throw new HelixException(String
              .format("Invalid fault zone type %s, not present in topology definition %s.",
                  _faultZoneType, topologyDef));
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

  private static Node cloneTree(Node root, Map<Node, Integer> newNodeWeight, Set<Node> failedNodes) {
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

    for (String instance : _allInstances) {
      InstanceConfig insConfig = _instanceConfigMap.get(instance);
      Map<String, String> instanceTopologyMap =
          computeInstanceTopologyMap(_clusterConfig, instance, insConfig);
      if (instanceTopologyMap != null) {
        int weight = insConfig.getWeight();
        if (weight < 0 || weight == InstanceConfig.WEIGHT_NOT_SET) {
          weight = DEFAULT_NODE_WEIGHT;
        }
        addEndNode(root, instance, instanceTopologyMap, weight, _liveInstances);
      }
    }
    return root;
  }

  /**
   * This function returns a LinkedHashMap<String, String> object representing
   * the topology path for an instance.
   * LinkedHashMap is used here since the order of the path needs to be preserved
   * when creating the topology tree.
   *
   * @param clusterConfig    clusterConfig of the cluster.
   * @param instance         Name of the given instance.
   * @param instanceConfig   instanceConfig of the instance.
   * @return an LinkedHashMap object representing the topology path for the input instance.
   */
  private LinkedHashMap<String, String> computeInstanceTopologyMap(ClusterConfig clusterConfig,
      String instance, InstanceConfig instanceConfig) {
    LinkedHashMap<String, String> instanceTopologyMap = new LinkedHashMap<>();
    if (clusterConfig.isTopologyAwareEnabled()) {
      String topologyDef = clusterConfig.getTopology();
      if (topologyDef == null) {
        // Return a ordered map using default cluster topology definition, i,e. /root/zone/instance
        String zone = instanceConfig.getZoneId();
        if (zone == null) {
          // we have the hierarchy style of domain id for instance.
          if (instanceConfig.getInstanceEnabled() && (clusterConfig.getDisabledInstances() == null
              || !clusterConfig.getDisabledInstances().containsKey(instance))) {
            // if enabled instance missing ZONE_ID information, fail the rebalance.
            throw new HelixException(String
                .format("ZONE_ID for instance %s is not set, fail the topology-aware placement!",
                    instance));
          } else {
            // if the disabled instance missing ZONE setting, ignore it should be fine.
            logger.warn("ZONE_ID for instance {} is not set, ignore the instance!", instance);
            return null;
          }
        }
        instanceTopologyMap.put(Types.ZONE.name(), zone);
        instanceTopologyMap.put(Types.INSTANCE.name(), instance);
      } else {
        /*
         * Return a ordered map representing the instance path. The topology order is defined in
         * ClusterConfig.topology.
         */
        String domain = instanceConfig.getDomainAsString();
        if (domain == null || domain.isEmpty()) {
          if (instanceConfig.getInstanceEnabled() && (clusterConfig.getDisabledInstances() == null
              || !clusterConfig.getDisabledInstances().containsKey(instance))) {
            // if enabled instance missing domain information, fail the rebalance.
            throw new HelixException(String
                .format("Domain for instance %s is not set, fail the topology-aware placement!",
                    instance));
          } else {
            // if the disabled instance missing domain setting, ignore it should be fine.
            logger.warn("Domain for instance {} is not set, ignore the instance!", instance);
            return null;
          }
        }
        Map<String, String> domainAsMap;
        try {
          domainAsMap = instanceConfig.getDomainAsMap();
        } catch (IllegalArgumentException e) {
          throw new HelixException(String.format(
              "Domain %s for instance %s is not valid, fail the topology-aware placement!",
              domain, instance));
        }
        String[] topologyKeys = topologyDef.trim().split("/");
        int numOfmatchedKeys = 0;
        for (String key : topologyKeys) {
          if (!key.isEmpty()) {
            // if a key does not exist in the instance domain config, apply the default domain value.
            String value = domainAsMap.get(key);
            if (value == null || value.length() == 0) {
              value = DEFAULT_PATH_PREFIX + key;
            } else {
              numOfmatchedKeys++;
            }
            instanceTopologyMap.put(key, value);
          }
        }
        if (numOfmatchedKeys != domainAsMap.size()) {
          logger.warn(
              "Key-value pairs in InstanceConfig.Domain {} do not align with keys in ClusterConfig.Topology {}",
              domain, topologyKeys.toString());
        }
      }
    } else {
      instanceTopologyMap.put(Types.INSTANCE.name(), instance);
    }
    return instanceTopologyMap;
  }

  /**
   * Add an end node to the tree, create all the paths to the leaf node if not present.
   * Input param 'pathNameMap' must have a certain order where the order of the keys should be
   * the same as the path order in ClusterConfig.topology.
   */
  private void addEndNode(Node root, String instanceName, Map<String, String> pathNameMap,
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
